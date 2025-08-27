using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Models;
using AdvancedMarketData.Core.Services;
using AdvancedMarketData.Core.Interfaces;
using AdvancedMarketData.Core.Helpers;

namespace AdvancedMarketData.Streaming;

public class MarketDataStreamer : IMarketDataStreamer
{
    private readonly ILogger<MarketDataStreamer> _logger;
    private readonly IWebSocketStreamService _webSocketStreamService;
    private readonly ICsvService _csvService;
    private readonly IConfiguration _configuration;
    private readonly Ed25519JwtHelper _jwtHelper;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly List<Candle> _candles;
    private readonly object _candlesLock = new object();

    public MarketDataStreamer(
        ILogger<MarketDataStreamer> logger,
        IWebSocketStreamService webSocketStreamService,
        ICsvService csvService,
        IConfiguration configuration,
        Ed25519JwtHelper jwtHelper)
    {
        _logger = logger;
        _webSocketStreamService = webSocketStreamService;
        _csvService = csvService;
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
            _logger.LogInformation("Starting market data stream...");
            
            // Get configuration for asset pairs and channels
            var productIds = GetProductIds();
            var channels = GetChannels();
            
            _logger.LogInformation("Streaming {ProductCount} products with {ChannelCount} channels (5-minute candles)", 
                productIds.Length, channels.Length);
            
            // Log what we're about to stream
            _logger.LogInformation("Products: [{Products}]", string.Join(", ", productIds));
            _logger.LogInformation("Channels: [{Channels}]", string.Join(", ", channels));

            // Start streaming for each product with the specified channels
            _logger.LogInformation("Starting WebSocket streams for {ChannelCount} channels", channels.Length);
            
            // Use internal cancellation token that can be stopped by StopAsync()
            var streamingTasks = productIds.Select(productId => 
                StartProductStreamAsync(productId, channels, _cancellationTokenSource.Token));
            
            // Wait for all streaming tasks, but handle cancellation properly
            await Task.WhenAll(streamingTasks);
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
    }

    private async Task StartProductStreamAsync(string productId, string[] channels, CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Starting stream for product: {ProductId} with channels: [{Channels}]", 
                productId, string.Join(", ", channels));
            await _webSocketStreamService.StartStreamingAsync(channels, new[] { productId }, ct);
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
        lock (_candlesLock)
        {
            _candles.Add(candle);
            _logger.LogDebug("Received candle: {Time} O:{Open} H:{High} L:{Low} C:{Close} V:{Volume}", 
                candle.Time, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume);
            
            // Keep only last 1000 candles to prevent memory issues
            if (_candles.Count > 1000)
            {
                _candles.RemoveAt(0);
            }
        }
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
        
        if (autoExportOnExit && _candles.Count > 0)
        {
            _logger.LogInformation("Auto-exporting {CandleCount} candles on exit...", _candles.Count);
            ExportMultiStreamCsv();
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
        
        // Unsubscribe from events
        _webSocketStreamService.OnCandleReceived -= OnCandleReceived;
        return Task.CompletedTask;
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
                var fileName = GenerateUniqueFileName(productId, dateStr);
                
                try
                {
                    _logger.LogInformation("Exporting {Count} candles for {ProductId} to {FileName}", 
                        productCandles.Count, productId, fileName);
                    
                    _csvService.SaveCandles(productCandles, fileName);
                    _logger.LogInformation("Successfully exported {ProductId} data to {FileName}", productId, fileName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to export {ProductId} data to {FileName}", productId, fileName);
                }
            }
        }
    }

    private string GenerateUniqueFileName(string productId, string dateStr)
    {
        var baseFileName = $"candles_5min_{productId.Replace("-", "_")}_{dateStr}.csv";
        
        // If the file doesn't exist, use the base name
        if (!File.Exists(baseFileName))
            return baseFileName;
        
        // If it exists, find the next available number
        int counter = 1;
        string numberedFileName;
        do
        {
            numberedFileName = $"candles_5min_{productId.Replace("-", "_")}_{dateStr}({counter}).csv";
            counter++;
        }
        while (File.Exists(numberedFileName));
        
        return numberedFileName;
    }



    public void Dispose()
    {
        _cancellationTokenSource?.Cancel();
        _cancellationTokenSource?.Dispose();
    }
}