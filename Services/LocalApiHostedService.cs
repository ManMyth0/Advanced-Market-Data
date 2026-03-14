using System.Net;
using System.Text;
using System.Text.Json;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Models;
using Microsoft.Extensions.Configuration;

namespace AdvancedMarketData.Core.Services;

public sealed class LocalApiHostedService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = true
    };

    private readonly ILogger<LocalApiHostedService> _logger;
    private readonly IConfiguration _configuration;
    private readonly ICommandWhitelistValidator _whitelistValidator;
    private readonly IApiCommandExecutor _commandExecutor;
    private readonly string _auditLogDirectory;
    private readonly int _auditRetentionDays;
    private readonly string _contentRootPath;
    private readonly object _auditFileLock = new();
    private HttpListener? _listener;

    public LocalApiHostedService(
        ILogger<LocalApiHostedService> logger,
        IConfiguration configuration,
        ICommandWhitelistValidator whitelistValidator,
        IApiCommandExecutor commandExecutor,
        IHostEnvironment hostEnvironment)
    {
        _logger = logger;
        _configuration = configuration;
        _whitelistValidator = whitelistValidator;
        _commandExecutor = commandExecutor;
        _contentRootPath = hostEnvironment.ContentRootPath;

        var relativeAuditPath = _configuration["AppConfiguration:LocalApi:AuditLog:Directory"] ?? "logs";
        _auditLogDirectory = Path.GetFullPath(Path.Combine(_contentRootPath, relativeAuditPath));
        _auditRetentionDays = Math.Clamp(_configuration.GetValue("AppConfiguration:LocalApi:AuditLog:RetentionDays", 31), 1, 366);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_configuration.GetValue("AppConfiguration:LocalApi:Enabled", false))
        {
            _logger.LogInformation("Local API is disabled. Set AppConfiguration:LocalApi:Enabled=true to enable it.");
            return;
        }

        var host = _configuration["AppConfiguration:LocalApi:Host"] ?? "127.0.0.1";
        var port = _configuration.GetValue("AppConfiguration:LocalApi:Port", 5057);
        var prefix = $"http://{host}:{port}/";
        Directory.CreateDirectory(_auditLogDirectory);

        _listener = new HttpListener();
        _listener.Prefixes.Add(prefix);
        _listener.Start();
        _logger.LogInformation("Local API listening on {Prefix}", prefix);
        _logger.LogInformation("API command audit logs enabled at {AuditPath} with retention {RetentionDays} days", _auditLogDirectory, _auditRetentionDays);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var context = await _listener.GetContextAsync().WaitAsync(stoppingToken);
                _ = ProcessRequestAsync(context, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Local API hosted service cancellation requested.");
        }
        finally
        {
            try
            {
                _listener.Stop();
                _listener.Close();
            }
            catch
            {
                // No-op on shutdown cleanup.
            }
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _listener?.Stop();
        }
        catch
        {
            // Ignore listener stop failures during teardown.
        }

        return base.StopAsync(cancellationToken);
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, CancellationToken ct)
    {
        var method = context.Request.HttpMethod;
        var path = context.Request.Url?.AbsolutePath?.TrimEnd('/') ?? "/";
        if (string.IsNullOrEmpty(path))
        {
            path = "/";
        }

        try
        {
            if (path.Equals("/docs/logs", StringComparison.OrdinalIgnoreCase) ||
                path.StartsWith("/logs", StringComparison.OrdinalIgnoreCase))
            {
                await WriteJsonAsync(context.Response, 403, new
                {
                    error = "Access to logs is not permitted."
                });
                return;
            }

            if (method.Equals("GET", StringComparison.OrdinalIgnoreCase) &&
                path.Equals("/health", StringComparison.OrdinalIgnoreCase))
            {
                await WriteJsonAsync(context.Response, 200, new
                {
                    status = "ok",
                    timestampUtc = DateTime.UtcNow
                });
                return;
            }

            if (method.Equals("GET", StringComparison.OrdinalIgnoreCase) &&
                path.Equals("/docs/whitelist", StringComparison.OrdinalIgnoreCase))
            {
                await WriteJsonAsync(context.Response, 200, new
                {
                    commands = _whitelistValidator.GetDefinitions()
                });
                return;
            }

            if (method.Equals("GET", StringComparison.OrdinalIgnoreCase) &&
                path.Equals("/docs/readme", StringComparison.OrdinalIgnoreCase))
            {
                var readmePath = Path.Combine(_contentRootPath, "README.md");
                if (!File.Exists(readmePath))
                {
                    await WriteJsonAsync(context.Response, 404, new
                    {
                        error = "README not found",
                        path = readmePath
                    });
                    return;
                }

                var content = await File.ReadAllTextAsync(readmePath, ct);
                await WriteJsonAsync(context.Response, 200, new
                {
                    path = readmePath,
                    content
                });
                return;
            }

            if (method.Equals("POST", StringComparison.OrdinalIgnoreCase) &&
                path.Equals("/commands/execute", StringComparison.OrdinalIgnoreCase))
            {
                await HandleExecuteCommandAsync(context, ct);
                return;
            }

            await WriteJsonAsync(context.Response, 404, new
            {
                error = "Not found",
                path
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled API error for {Method} {Path}", method, path);
            await WriteJsonAsync(context.Response, 500, new
            {
                error = "Internal server error"
            });
        }
    }

    private async Task HandleExecuteCommandAsync(HttpListenerContext context, CancellationToken ct)
    {
        var requestId = Guid.NewGuid().ToString("D");
        var startedUtc = DateTime.UtcNow;
        var watch = Stopwatch.StartNew();

        ApiCommandRequest? request;
        await using (var stream = context.Request.InputStream)
        {
            request = await JsonSerializer.DeserializeAsync<ApiCommandRequest>(stream, JsonOptions, ct);
        }

        if (request is null)
        {
            watch.Stop();
            LogAudit(requestId, "unknown", startedUtc, "failed", "validation_error", "Request body is required.", watch.ElapsedMilliseconds, null);
            await WriteJsonAsync(context.Response, 400, new ApiCommandExecutionResponse
            {
                RequestId = requestId,
                Success = false,
                Status = "failed",
                Message = "Request body is required."
            });
            return;
        }

        request.Args ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!_whitelistValidator.TryValidate(request, out var error))
        {
            watch.Stop();
            LogAudit(requestId, request.Command, startedUtc, "failed", "validation_error", error, watch.ElapsedMilliseconds, request.Args);
            await WriteJsonAsync(context.Response, 400, new ApiCommandExecutionResponse
            {
                RequestId = requestId,
                Success = false,
                Status = "failed",
                Message = error
            });
            return;
        }

        try
        {
            var response = await _commandExecutor.ExecuteApiCommandAsync(request, ct);
            response.RequestId = requestId;
            watch.Stop();

            LogAudit(
                requestId,
                request.Command,
                startedUtc,
                response.Success ? "success" : "failed",
                response.Success ? null : "execution_error",
                response.Message,
                watch.ElapsedMilliseconds,
                request.Args);

            await WriteJsonAsync(context.Response, response.Success ? 200 : 409, response);
        }
        catch (OperationCanceledException)
        {
            watch.Stop();
            LogAudit(requestId, request.Command, startedUtc, "failed", "cancelled", "Command was cancelled.", watch.ElapsedMilliseconds, request.Args);
            await WriteJsonAsync(context.Response, 499, new ApiCommandExecutionResponse
            {
                RequestId = requestId,
                Success = false,
                Status = "failed",
                Message = "Command was cancelled."
            });
        }
        catch (Exception ex)
        {
            watch.Stop();
            LogAudit(requestId, request.Command, startedUtc, "failed", "internal_error", ex.Message, watch.ElapsedMilliseconds, request.Args);
            _logger.LogError(ex, "Command execution failure for request {RequestId}", requestId);
            await WriteJsonAsync(context.Response, 500, new ApiCommandExecutionResponse
            {
                RequestId = requestId,
                Success = false,
                Status = "failed",
                Message = "Internal command execution error."
            });
        }
    }

    private void LogAudit(
        string requestId,
        string command,
        DateTime startedUtc,
        string status,
        string? failureType,
        string message,
        long durationMs,
        Dictionary<string, string>? args)
    {
        var argsSummary = args is null
            ? "{}"
            : JsonSerializer.Serialize(args, JsonOptions);

        _logger.LogInformation(
            "api.command.execute TimestampUtc={TimestampUtc:o} RequestId={RequestId} Command={Command} Status={Status} FailureType={FailureType} DurationMs={DurationMs} Args={Args} Message={Message}",
            startedUtc,
            requestId,
            command,
            status,
            failureType ?? "none",
            durationMs,
            argsSummary,
            message);

        var auditEntry = new
        {
            eventName = "api.command.execute",
            timestampUtc = startedUtc,
            requestId,
            command,
            status,
            failureType = failureType ?? "none",
            durationMs,
            argumentsSummary = args ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            message
        };

        AppendAuditLogEntry(auditEntry);
    }

    private void AppendAuditLogEntry(object auditEntry)
    {
        try
        {
            var fileName = $"api-command-audit-{DateTime.UtcNow:yyyy-MM-dd}.jsonl";
            var filePath = Path.Combine(_auditLogDirectory, fileName);
            var line = JsonSerializer.Serialize(auditEntry);

            lock (_auditFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine, Encoding.UTF8);
                CleanupExpiredAuditFiles();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist API audit log entry to file.");
        }
    }

    private void CleanupExpiredAuditFiles()
    {
        var cutoff = DateTime.UtcNow.AddDays(-_auditRetentionDays);
        var files = Directory.GetFiles(_auditLogDirectory, "api-command-audit-*.jsonl");

        foreach (var filePath in files)
        {
            var lastWrite = File.GetLastWriteTimeUtc(filePath);
            if (lastWrite < cutoff)
            {
                try
                {
                    File.Delete(filePath);
                }
                catch
                {
                    // Non-fatal; keep going if one old file can't be deleted.
                }
            }
        }
    }

    private static async Task WriteJsonAsync(HttpListenerResponse response, int statusCode, object payload)
    {
        response.StatusCode = statusCode;
        response.ContentType = "application/json";
        response.ContentEncoding = Encoding.UTF8;

        var json = JsonSerializer.Serialize(payload, JsonOptions);
        var bytes = Encoding.UTF8.GetBytes(json);
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes);
        response.OutputStream.Close();
    }
}
