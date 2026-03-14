using System.Globalization;
using Microsoft.Extensions.Logging;
using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Models;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Services;
using AdvancedMarketData.Core.Interfaces;
using Microsoft.Extensions.Configuration;

namespace AdvancedMarketData.Streaming;

public class MarketDataStreamer : IMarketDataStreamer, IApiCommandExecutor
{
    private enum StreamRunMode
    {
        LiveOnly,
        HistoricalOnly,
        HistoricalThenLive
    }

    private readonly ILogger<MarketDataStreamer> _logger;
    private readonly IWebSocketStreamService _webSocketStreamService;
    private readonly ICsvService _csvService;
    private readonly ICoinbaseRestService _coinbaseRestService;
    private readonly IConfiguration _configuration;
    private readonly Ed25519JwtHelper _jwtHelper;
    private CancellationTokenSource _cancellationTokenSource;
    private readonly List<Candle> _candles;
    private readonly object _candlesLock = new object();
    private readonly SemaphoreSlim _apiCommandLock = new(1, 1);
    private StreamRunMode _runMode = StreamRunMode.LiveOnly;
    private int _maxCandlesInMemory = 1000;
    private int _selectedGranularitySeconds = CandleGranularity.FiveMinutes;
    private string _selectedGranularityLabel = "300s";
    private bool _hasExportedDuringRun;
    private volatile bool _isStreaming;
    private Task? _activeStreamingTask;
    private string[] _activeProducts = Array.Empty<string>();
    private string[] _activeChannels = Array.Empty<string>();

    public MarketDataStreamer(
        ILogger<MarketDataStreamer> logger,
        IWebSocketStreamService webSocketStreamService,
        ICsvService csvService,
        ICoinbaseRestService coinbaseRestService,
        IConfiguration configuration,
        Ed25519JwtHelper jwtHelper)
    {
        _logger = logger;
        _webSocketStreamService = webSocketStreamService;
        _csvService = csvService;
        _coinbaseRestService = coinbaseRestService;
        _configuration = configuration;
        _jwtHelper = jwtHelper;
        _cancellationTokenSource = new CancellationTokenSource();
        _candles = new List<Candle>();
        
        // Subscribe to candle events
        _webSocketStreamService.OnCandleReceived += OnCandleReceived;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            ResetCancellationTokenSourceIfNeeded();
            _hasExportedDuringRun = false;
            _logger.LogInformation("Starting market data stream...");
            
            // Get configuration for asset pairs and channels
            var productIds = GetProductIds();
            var channels = GetChannels();
            _activeProducts = productIds;
            _activeChannels = channels;
            var historyDate = GetHistoryDateFromArgs();
            _runMode = GetRunMode(historyDate);
            _maxCandlesInMemory = GetMaxCandlesInMemory();
            ValidateGranularityUsage();

            var granularitySeconds = CandleGranularity.FiveMinutes;
            if (_runMode == StreamRunMode.HistoricalOnly)
            {
                var selection = GetHistoricalGranularitySelection();
                granularitySeconds = selection.seconds;
                _selectedGranularityLabel = selection.label;
            }
            else
            {
                _selectedGranularityLabel = "300s";
            }
            _selectedGranularitySeconds = granularitySeconds;
            
            _logger.LogInformation("Streaming {ProductCount} products with {ChannelCount} channels", 
                productIds.Length, channels.Length);
            
            // Log what we're about to stream
            _logger.LogInformation("Products: [{Products}]", string.Join(", ", productIds));
            _logger.LogInformation("Channels: [{Channels}]", string.Join(", ", channels));
            _logger.LogInformation("Run mode: {RunMode}", _runMode);

            if (_runMode == StreamRunMode.LiveOnly)
            {
                _logger.LogInformation("Live WebSocket candle granularity: 300s (5 minutes, fixed by Coinbase)");
            }
            else
            {
                _logger.LogInformation("Historical candle granularity: {GranularitySeconds}s ({GranularityDescription})",
                    granularitySeconds, CandleGranularity.Describe(granularitySeconds));
            }

            if ((_runMode == StreamRunMode.HistoricalOnly || _runMode == StreamRunMode.HistoricalThenLive) && !historyDate.HasValue)
            {
                _logger.LogWarning("Run mode requires --history-date=YYYY-MM-DD. Falling back to live-only mode.");
                _runMode = StreamRunMode.LiveOnly;
            }

            if ((_runMode == StreamRunMode.HistoricalOnly || _runMode == StreamRunMode.HistoricalThenLive) && historyDate.HasValue)
            {
                var startUtc = historyDate.Value.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
                var endUtcExclusive = _runMode == StreamRunMode.HistoricalOnly
                    ? startUtc.AddDays(1)
                    : DateTime.UtcNow;

                await LoadHistoricalCandlesAsync(
                    productIds,
                    startUtc,
                    endUtcExclusive,
                    granularitySeconds,
                    _cancellationTokenSource.Token);
            }

            if (_runMode == StreamRunMode.HistoricalOnly)
            {
                AutoExportIfEnabled("historical-only completion");
                _logger.LogInformation("Historical-only mode completed.");
                _isStreaming = false;
                return;
            }

            _logger.LogInformation("Live stream mode active. Snapshot candles are disabled.");

            // Start streaming for each product with the specified channels
            _logger.LogInformation("Starting WebSocket streams for {ChannelCount} channels", channels.Length);
            
            // Use internal cancellation token that can be stopped by StopAsync()
            var streamingTasks = productIds.Select(productId => 
                StartProductStreamAsync(productId, channels, _cancellationTokenSource.Token));
            
            // Wait for all streaming tasks, but handle cancellation properly
            _isStreaming = true;
            _activeStreamingTask = Task.WhenAll(streamingTasks);
            await _activeStreamingTask;
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Market data streaming was cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error starting market data stream");
            throw;
        }
        finally
        {
            _isStreaming = false;
        }
    }

    private async Task StartProductStreamAsync(string productId, string[] channels, CancellationToken ct)
    {
        try
        {
            var includeSnapshotCandles = _runMode == StreamRunMode.HistoricalOnly;
            _logger.LogInformation("Starting stream for product: {ProductId} with channels: [{Channels}]", 
                productId, string.Join(", ", channels));
            await _webSocketStreamService.StartStreamingAsync(
                channels,
                new[] { productId },
                ct,
                includeSnapshotCandles);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Streaming cancelled for product {ProductId}", productId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error streaming product {ProductId}", productId);
        }
    }

    private void OnCandleReceived(Candle candle)
    {
        if (ShouldIgnoreLiveCandle(candle))
        {
            return;
        }

        lock (_candlesLock)
        {
            var existingIndex = _candles.FindIndex(c =>
                c.ProductId == candle.ProductId &&
                c.Time == candle.Time);

            if (existingIndex >= 0)
            {
                // Coinbase sends updates for the active candle bucket; keep latest state.
                _candles[existingIndex] = candle;
            }
            else
            {
                _candles.Add(candle);
            }

            _logger.LogDebug("Received candle: {Time} O:{Open} H:{High} L:{Low} C:{Close} V:{Volume}", 
                candle.Time, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume);
            
            if (_maxCandlesInMemory > 0 && _candles.Count > _maxCandlesInMemory)
            {
                _candles.RemoveAt(0);
            }
        }
    }

    private bool ShouldIgnoreLiveCandle(Candle candle)
    {
        // Live/historical separation is enforced in WebSocket processing by skipping
        // snapshot candles for live modes. No additional time-based filtering is needed.
        return false;
    }

    private string[] GetProductIds()
    {
        // 1. Check for manual override first (command line)
        var args = Environment.GetCommandLineArgs();
        var productArg = args.FirstOrDefault(arg => arg.StartsWith("--products="));
        
        if (productArg != null)
        {
            var productIds = ParseProductIdString(productArg.Substring("--products=".Length));
            _logger.LogInformation("Using command line override: [{ProductIds}]", string.Join(", ", productIds));
            return productIds;
        }

        // 2. Fall back to configuration
        var configProductIds = _configuration.GetSection("AppConfiguration:ProductIds").Get<string[]>() ?? new[] { "BTC-USD" };
        _logger.LogInformation("Using configuration asset pairs: [{ProductIds}]", string.Join(", ", configProductIds));
        return configProductIds;
    }

    private (int seconds, string label) GetHistoricalGranularitySelection()
    {
        // 1. Command line override
        var args = Environment.GetCommandLineArgs();
        var granularityArg = args.FirstOrDefault(arg => arg.StartsWith("--granularity=", StringComparison.OrdinalIgnoreCase));
        if (granularityArg is not null)
        {
            var value = granularityArg.Substring("--granularity=".Length);
            if (CandleGranularity.TryParse(value, out var parsedFromArg, out var labelFromArg))
            {
                _logger.LogInformation("Using command line granularity: {GranularitySeconds}s", parsedFromArg);
                return (parsedFromArg, labelFromArg);
            }

            _logger.LogWarning(
                "Invalid granularity override '{GranularityArg}'. Supported: {SupportedValues}. Falling back to config/default.",
                value,
                CandleGranularity.SupportedValuesText);
        }

        // 2. Configuration fallback
        var configuredRaw = _configuration["AppConfiguration:GranularitySeconds"];
        if (configuredRaw is not null && CandleGranularity.TryParse(configuredRaw, out var parsedFromConfig, out var labelFromConfig))
        {
            _logger.LogInformation("Using configured granularity: {GranularitySeconds}s", parsedFromConfig);
            return (parsedFromConfig, labelFromConfig);
        }

        // 3. Default
        return (CandleGranularity.FiveMinutes, "300s");
    }

    private void ValidateGranularityUsage()
    {
        if (_runMode == StreamRunMode.HistoricalOnly)
        {
            return;
        }

        var args = Environment.GetCommandLineArgs();
        var hasGranularityArg = args.Any(arg => arg.StartsWith("--granularity=", StringComparison.OrdinalIgnoreCase));
        if (hasGranularityArg)
        {
            throw new InvalidOperationException("--granularity is only supported when --mode=historical-only.");
        }
    }

    private StreamRunMode GetRunMode(DateOnly? historyDate)
    {
        // 1. Command line override
        var args = Environment.GetCommandLineArgs();
        var modeArg = args.FirstOrDefault(arg => arg.StartsWith("--mode=", StringComparison.OrdinalIgnoreCase));
        if (modeArg is not null)
        {
            var modeValue = modeArg.Substring("--mode=".Length).Trim().ToLowerInvariant();
            if (TryParseRunMode(modeValue, out var parsedMode))
            {
                return parsedMode;
            }

            _logger.LogWarning("Invalid mode '{Mode}'. Supported: live-only, historical-only, historical-then-live", modeValue);
        }

        // 2. Configuration fallback
        var configuredMode = _configuration.GetValue<string>("AppConfiguration:RunMode");
        if (!string.IsNullOrWhiteSpace(configuredMode) && TryParseRunMode(configuredMode.Trim().ToLowerInvariant(), out var parsedConfigMode))
        {
            return parsedConfigMode;
        }

        // 3. Auto mode selection
        return historyDate.HasValue ? StreamRunMode.HistoricalThenLive : StreamRunMode.LiveOnly;
    }

    private static bool TryParseRunMode(string value, out StreamRunMode runMode)
    {
        if (value == "live-only")
        {
            runMode = StreamRunMode.LiveOnly;
            return true;
        }

        if (value == "historical-only")
        {
            runMode = StreamRunMode.HistoricalOnly;
            return true;
        }

        if (value == "historical-then-live")
        {
            runMode = StreamRunMode.HistoricalThenLive;
            return true;
        }

        runMode = StreamRunMode.LiveOnly;
        return false;
    }

    private int GetMaxCandlesInMemory()
    {
        var enabled = _configuration.GetValue<bool>("AppConfiguration:MaxCandlesInMemory:Enabled", true);
        if (!enabled)
        {
            return 0;
        }

        var amount = _configuration.GetValue<int?>("AppConfiguration:MaxCandlesInMemory:Amount") ?? 1000;
        return amount > 0 ? amount : 1000;
    }

    private DateOnly? GetHistoryDateFromArgs()
    {
        var args = Environment.GetCommandLineArgs();
        var historyDateArg = args.FirstOrDefault(arg => arg.StartsWith("--history-date=", StringComparison.OrdinalIgnoreCase));
        if (historyDateArg is null)
        {
            return null;
        }

        var value = historyDateArg.Substring("--history-date=".Length);
        if (DateOnly.TryParse(value, out var parsedDate))
        {
            return parsedDate;
        }

        _logger.LogWarning("Invalid history date value '{HistoryDate}'. Use format YYYY-MM-DD.", value);
        return null;
    }

    private async Task LoadHistoricalCandlesAsync(
        IEnumerable<string> productIds,
        DateTime startUtc,
        DateTime endUtcExclusive,
        int granularitySeconds,
        CancellationToken ct)
    {
        if (endUtcExclusive <= startUtc)
        {
            _logger.LogWarning(
                "Historical range is empty. Start: {StartUtc:O}, End: {EndUtc:O}. Skipping historical preload.",
                startUtc,
                endUtcExclusive);
            return;
        }

        _logger.LogInformation(
            "Historical preload enabled. Loading {Granularity}s candles from {StartUtc:O} to {EndUtc:O}.",
            granularitySeconds,
            startUtc,
            endUtcExclusive);

        foreach (var productId in productIds)
        {
            try
            {
                var historical = await _coinbaseRestService.GetHistoricalCandlesAsync(
                    productId,
                    startUtc,
                    endUtcExclusive,
                    granularitySeconds,
                    ct);

                lock (_candlesLock)
                {
                    _candles.AddRange(historical);
                }

                _logger.LogInformation("Loaded {Count} historical candles for {ProductId} in requested range",
                    historical.Count, productId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load historical candles for {ProductId} in requested range", productId);
            }
        }
    }

    private string[] ParseProductIdString(string input)
    {
        return input.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(p => p.Trim().ToUpperInvariant())
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .ToArray();
    }

    private string[] GetChannels()
    {
        // 1. Check for manual override first (command line)
        var args = Environment.GetCommandLineArgs();
        var channelsArg = args.FirstOrDefault(arg => arg.StartsWith("--channels="));
        
        if (channelsArg != null)
        {
            var channels = ParseChannelString(channelsArg.Substring("--channels=".Length));
            _logger.LogInformation("Using command line channels: [{Channels}]", string.Join(", ", channels));
            return channels;
        }

        // 2. Try to load from channels.txt file
        var channelsFromFile = LoadChannelsFromFile();
        if (channelsFromFile.Length > 0)
        {
            _logger.LogInformation("Using channels from file: [{Channels}]", string.Join(", ", channelsFromFile));
            return channelsFromFile;
        }

        // 3. Fall back to default channels
        var defaultChannels = _configuration.GetSection("AppConfiguration:DefaultChannels").Get<string[]>() ?? new[] { "candles" };
        _logger.LogInformation("Using default channels: [{Channels}]", string.Join(", ", defaultChannels));
        return defaultChannels;
    }

    private string[] ParseChannelString(string input)
    {
        return input.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Trim().ToLowerInvariant())
                    .Where(c => !string.IsNullOrWhiteSpace(c))
                    .ToArray();
    }

    private string[] LoadChannelsFromFile()
    {
        try
        {
            var channelsFile = _configuration.GetValue<string>("AppConfiguration:ChannelsFile", "channels.txt");
            if (!File.Exists(channelsFile))
            {
                _logger.LogDebug("Channels file not found: {ChannelsFile}", channelsFile);
                return Array.Empty<string>();
            }

            var lines = File.ReadAllLines(channelsFile);
            var channels = lines
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.Trim().StartsWith("#"))
                .Select(line => line.Trim().ToLowerInvariant())
                .ToArray();

            return channels;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading channels from file");
            return Array.Empty<string>();
        }
    }

    private void ProcessCandleMessage(string messageJson)
    {
        try
        {
            // This method processes candle messages from the multi-channel service
            // and forwards them to the existing OnCandleReceived handler
            // For now, we'll just log that we received the message
            _logger.LogDebug("Received candle message from multi-channel service");
            
            // TODO: Parse the message and convert to Candle objects for CSV export
            // This will be implemented when we integrate the full candle processing
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing candle message from multi-channel service");
        }
    }

    public Task StopAsync()
    {
        _logger.LogInformation("Stopping market data stream...");
        
        // Check if auto export is enabled
        var autoExportOnExit = _configuration.GetValue<bool>("AppConfiguration:AutoExportOnExit", true);
        
        if (autoExportOnExit && _candles.Count > 0 && !_hasExportedDuringRun)
        {
            _logger.LogInformation("Auto-exporting {CandleCount} candles on exit...", _candles.Count);
            ExportMultiStreamCsv();
        }
        else if (autoExportOnExit && _hasExportedDuringRun)
        {
            _logger.LogInformation("Auto-export already completed earlier in this run");
        }
        else if (!autoExportOnExit)
        {
            _logger.LogInformation("Auto-export on exit is disabled");
        }
        else
        {
            _logger.LogInformation("No candles to export on exit");
        }
        
        _cancellationTokenSource.Cancel();
        _isStreaming = false;
        return Task.CompletedTask;
    }

    private void AutoExportIfEnabled(string reason)
    {
        var autoExportOnExit = _configuration.GetValue<bool>("AppConfiguration:AutoExportOnExit", true);
        if (!autoExportOnExit)
        {
            _logger.LogInformation("Auto-export disabled; skipping export at {Reason}", reason);
            return;
        }

        if (_candles.Count == 0)
        {
            _logger.LogInformation("No candles available to export at {Reason}", reason);
            return;
        }

        _logger.LogInformation("Auto-exporting {CandleCount} candles at {Reason}", _candles.Count, reason);
        ExportMultiStreamCsv();
    }

    public Task ExportToCsvAsync(string filePath)
    {
        lock (_candlesLock)
        {
            _csvService.SaveCandles(_candles, filePath);
            _logger.LogInformation("Exported {CandleCount} candles to {FilePath}", _candles.Count, filePath);
        }
        return Task.CompletedTask;
    }

    public Task ExportCurrentDataAsync()
    {
        if (_candles.Count > 0)
        {
            _logger.LogInformation("Manually exporting {CandleCount} candles from {StreamCount} streams...", 
                _candles.Count, _candles.GroupBy(c => c.ProductId).Count());
            ExportMultiStreamCsv();
            _logger.LogInformation("Manual CSV export completed.");
        }
        else
        {
            _logger.LogWarning("No candles collected yet - nothing to export.");
        }
        return Task.CompletedTask;
    }

    private void ExportMultiStreamCsv()
    {
        lock (_candlesLock)
        {
            if (_candles.Count == 0)
            {
                _logger.LogWarning("No candles to export");
                return;
            }

            // Group candles by ProductId
            var candlesByProduct = _candles.GroupBy(c => c.ProductId);
            
            foreach (var group in candlesByProduct)
            {
                var productId = group.Key;
                var productCandles = group.OrderBy(c => c.Time).ToList();
                var dateStr = DateTime.Now.ToString("MM-dd-yyyy");
                var granularityLabel = GetExportGranularityLabel();
                var fileName = GenerateUniqueFileName(productId, dateStr, granularityLabel);
                
                try
                {
                    _logger.LogInformation("Exporting {Count} candles for {ProductId} to {FileName}", 
                        productCandles.Count, productId, fileName);
                    
                    _csvService.SaveCandles(productCandles, fileName);
                    _logger.LogInformation("Successfully exported {ProductId} data to {FileName}", productId, fileName);
                    _hasExportedDuringRun = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to export {ProductId} data to {FileName}", productId, fileName);
                }
            }
        }
    }

    private string GetExportGranularityLabel()
    {
        return _runMode == StreamRunMode.HistoricalOnly
            ? _selectedGranularityLabel
            : "300s";
    }

    private string GenerateUniqueFileName(string productId, string dateStr, string granularityLabel)
    {
        var baseFileName = $"candles_{granularityLabel}_{productId.Replace("-", "_")}_{dateStr}.csv";
        
        // If the file doesn't exist, use the base name
        if (!File.Exists(baseFileName))
            return baseFileName;
        
        // If it exists, find the next available number
        int counter = 1;
        string numberedFileName;
        do
        {
            numberedFileName = $"candles_{granularityLabel}_{productId.Replace("-", "_")}_{dateStr}({counter}).csv";
            counter++;
        }
        while (File.Exists(numberedFileName));
        
        return numberedFileName;
    }



    public void Dispose()
    {
        _webSocketStreamService.OnCandleReceived -= OnCandleReceived;
        _cancellationTokenSource?.Cancel();
        _cancellationTokenSource?.Dispose();
        _apiCommandLock.Dispose();
    }

    public ApiStatusSnapshot GetApiStatusSnapshot()
    {
        var mode = _isStreaming ? _runMode.ToString() : "Idle";
        return new ApiStatusSnapshot
        {
            IsStreaming = _isStreaming,
            ActiveMode = mode,
            ActiveProducts = _activeProducts,
            CollectedCandles = _candles.Count,
            SelectedGranularitySeconds = _selectedGranularitySeconds,
            SelectedGranularityLabel = _selectedGranularityLabel
        };
    }

    public async Task<ApiCommandExecutionResponse> ExecuteApiCommandAsync(ApiCommandRequest request, CancellationToken ct = default)
    {
        await _apiCommandLock.WaitAsync(ct);
        try
        {
            var command = request.Command.Trim().ToLowerInvariant();
            request.Args ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            return command switch
            {
                "get_status" => BuildSuccessResponse("Status snapshot returned.", new Dictionary<string, object?>
                {
                    ["status"] = GetApiStatusSnapshot()
                }),
                "stop_stream" => await ExecuteStopCommandAsync(),
                "start_live_stream" => await ExecuteStartLiveCommandAsync(request.Args, ct),
                "run_historical_only" => await ExecuteHistoricalOnlyCommandAsync(request.Args, ct),
                "run_historical_then_live" => await ExecuteHistoricalThenLiveCommandAsync(request.Args, ct),
                _ => new ApiCommandExecutionResponse
                {
                    Success = false,
                    Status = "failed",
                    Message = $"Unsupported command '{request.Command}'."
                }
            };
        }
        finally
        {
            _apiCommandLock.Release();
        }
    }

    private async Task<ApiCommandExecutionResponse> ExecuteStopCommandAsync()
    {
        await StopAsync();
        return BuildSuccessResponse("Stream stopped.", new Dictionary<string, object?>
        {
            ["status"] = GetApiStatusSnapshot()
        });
    }

    private async Task<ApiCommandExecutionResponse> ExecuteStartLiveCommandAsync(
        IReadOnlyDictionary<string, string> args,
        CancellationToken ct)
    {
        if (_isStreaming)
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "A stream is already active. Stop it before starting another."
            };
        }

        var products = ParseProductsArgument(args);
        if (products.Length == 0)
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "Argument 'products' must include at least one product id."
            };
        }

        _ = Task.Run(async () =>
        {
            await ExecuteConfiguredRunAsync(
                StreamRunMode.LiveOnly,
                products,
                null,
                CandleGranularity.FiveMinutes,
                "300s",
                ct);
        }, ct);

        return BuildSuccessResponse("Live stream started.", new Dictionary<string, object?>
        {
            ["products"] = products,
            ["mode"] = "live-only"
        });
    }

    private async Task<ApiCommandExecutionResponse> ExecuteHistoricalOnlyCommandAsync(
        IReadOnlyDictionary<string, string> args,
        CancellationToken ct)
    {
        if (_isStreaming)
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "Cannot run historical-only while a stream is active."
            };
        }

        var products = ParseProductsArgument(args);
        if (products.Length == 0)
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "Argument 'products' must include at least one product id."
            };
        }

        if (!TryParseHistoryDate(args, out var historyDate, out var historyError))
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = historyError
            };
        }

        if (!TryParseGranularity(args, out var granularitySeconds, out var label, out var granularityError))
        {
            return new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = granularityError
            };
        }

        await ExecuteConfiguredRunAsync(
            StreamRunMode.HistoricalOnly,
            products,
            historyDate,
            granularitySeconds,
            label,
            ct);

        return BuildSuccessResponse("Historical-only run completed.", new Dictionary<string, object?>
        {
            ["products"] = products,
            ["historyDate"] = historyDate.ToString("yyyy-MM-dd"),
            ["granularity"] = label,
            ["status"] = GetApiStatusSnapshot()
        });
    }

    private Task<ApiCommandExecutionResponse> ExecuteHistoricalThenLiveCommandAsync(
        IReadOnlyDictionary<string, string> args,
        CancellationToken ct)
    {
        if (_isStreaming)
        {
            return Task.FromResult(new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "A stream is already active. Stop it before starting another."
            });
        }

        var products = ParseProductsArgument(args);
        if (products.Length == 0)
        {
            return Task.FromResult(new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = "Argument 'products' must include at least one product id."
            });
        }

        if (!TryParseHistoryDate(args, out var historyDate, out var historyError))
        {
            return Task.FromResult(new ApiCommandExecutionResponse
            {
                Success = false,
                Status = "failed",
                Message = historyError
            });
        }

        _ = Task.Run(async () =>
        {
            await ExecuteConfiguredRunAsync(
                StreamRunMode.HistoricalThenLive,
                products,
                historyDate,
                CandleGranularity.FiveMinutes,
                "300s",
                ct);
        }, ct);

        return Task.FromResult(BuildSuccessResponse("Historical-then-live run started.", new Dictionary<string, object?>
        {
            ["products"] = products,
            ["historyDate"] = historyDate.ToString("yyyy-MM-dd"),
            ["mode"] = "historical-then-live"
        }));
    }

    private async Task ExecuteConfiguredRunAsync(
        StreamRunMode mode,
        string[] productIds,
        DateOnly? historyDate,
        int granularitySeconds,
        string granularityLabel,
        CancellationToken ct)
    {
        ResetCancellationTokenSourceIfNeeded();
        _hasExportedDuringRun = false;
        _runMode = mode;
        _selectedGranularitySeconds = granularitySeconds;
        _selectedGranularityLabel = granularityLabel;
        _maxCandlesInMemory = GetMaxCandlesInMemory();
        _activeProducts = productIds;
        _activeChannels = GetChannels();

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cancellationTokenSource.Token);
        try
        {
            if ((mode == StreamRunMode.HistoricalOnly || mode == StreamRunMode.HistoricalThenLive) && historyDate.HasValue)
            {
                var startUtc = historyDate.Value.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
                var endUtcExclusive = mode == StreamRunMode.HistoricalOnly
                    ? startUtc.AddDays(1)
                    : DateTime.UtcNow;

                await LoadHistoricalCandlesAsync(
                    productIds,
                    startUtc,
                    endUtcExclusive,
                    granularitySeconds,
                    linkedCts.Token);
            }

            if (mode == StreamRunMode.HistoricalOnly)
            {
                AutoExportIfEnabled("historical-only completion (api)");
                _isStreaming = false;
                return;
            }

            var streamingTasks = productIds.Select(productId =>
                StartProductStreamAsync(productId, _activeChannels, linkedCts.Token));

            _isStreaming = true;
            _activeStreamingTask = Task.WhenAll(streamingTasks);
            await _activeStreamingTask;
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Configured API run canceled.");
        }
        finally
        {
            _isStreaming = false;
            linkedCts.Dispose();
        }
    }

    private static string[] ParseProductsArgument(IReadOnlyDictionary<string, string> args)
    {
        if (!args.TryGetValue("products", out var productsRaw) || string.IsNullOrWhiteSpace(productsRaw))
        {
            return Array.Empty<string>();
        }

        return productsRaw.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim().ToUpperInvariant())
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .ToArray();
    }

    private static bool TryParseHistoryDate(
        IReadOnlyDictionary<string, string> args,
        out DateOnly historyDate,
        out string error)
    {
        historyDate = default;
        error = string.Empty;

        if (!args.TryGetValue("history_date", out var value) || string.IsNullOrWhiteSpace(value))
        {
            error = "Argument 'history_date' is required in YYYY-MM-DD format.";
            return false;
        }

        if (!DateOnly.TryParseExact(value.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out historyDate))
        {
            error = $"Invalid history_date '{value}'. Expected format YYYY-MM-DD.";
            return false;
        }

        return true;
    }

    private static bool TryParseGranularity(
        IReadOnlyDictionary<string, string> args,
        out int granularitySeconds,
        out string label,
        out string error)
    {
        granularitySeconds = CandleGranularity.FiveMinutes;
        label = "300s";
        error = string.Empty;

        if (!args.TryGetValue("granularity", out var value) || string.IsNullOrWhiteSpace(value))
        {
            error = "Argument 'granularity' is required for historical-only runs.";
            return false;
        }

        if (!CandleGranularity.TryParse(value.Trim(), out granularitySeconds, out label))
        {
            error = $"Invalid granularity '{value}'. Supported values: {CandleGranularity.SupportedValuesText}.";
            return false;
        }

        return true;
    }

    private ApiCommandExecutionResponse BuildSuccessResponse(string message, Dictionary<string, object?> data)
    {
        return new ApiCommandExecutionResponse
        {
            Success = true,
            Status = "success",
            Message = message,
            Data = data
        };
    }

    private void ResetCancellationTokenSourceIfNeeded()
    {
        if (_cancellationTokenSource.IsCancellationRequested)
        {
            _cancellationTokenSource.Dispose();
            _cancellationTokenSource = new CancellationTokenSource();
        }
    }
}