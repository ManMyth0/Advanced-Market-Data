using System.Threading;
using AdvancedMarketData.Core;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using AdvancedMarketData.Streaming;
using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Services;
using AdvancedMarketData.Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // Logging
        services.AddLogging(config =>
        {
            config.AddConsole();
            config.SetMinimumLevel(LogLevel.Information);
        });

        // Core services
        services.AddHttpClient();
        
        // Register Ed25519JwtHelper factory for WebSocketStreamService
        services.AddSingleton<Func<Ed25519JwtHelper?>>(serviceProvider =>
        {
            return new Func<Ed25519JwtHelper?>(() =>
            {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                var apiKeyId = configuration["CoinbaseApi:ApiKeyId"];
                var apiSecret = configuration["CoinbaseApi:ApiSecret"];
                
                if (string.IsNullOrEmpty(apiKeyId) || string.IsNullOrEmpty(apiSecret))
                {
                    return null; // No JWT helper if credentials are missing
                }
                
                return new Ed25519JwtHelper(apiKeyId, apiSecret);
            });
        });
        
        // Register Ed25519JwtHelper with conditional creation for MarketDataStreamer
        services.AddSingleton<Ed25519JwtHelper>(serviceProvider =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();
            var apiKeyId = configuration["CoinbaseApi:ApiKeyId"];
            var apiSecret = configuration["CoinbaseApi:ApiSecret"];
            
            if (string.IsNullOrEmpty(apiKeyId) || string.IsNullOrEmpty(apiSecret))
            {
                // Return a dummy instance for now - we'll handle this better later
                return new Ed25519JwtHelper("dummy", "dummy");
            }
            
            return new Ed25519JwtHelper(apiKeyId, apiSecret);
        });
        
        services.AddSingleton<ICsvService, CsvService>();
        services.AddSingleton<IAnalyticsService, AnalyticsService>();
        services.AddSingleton<IMarketDataStreamer, MarketDataStreamer>();
        services.AddSingleton<IApiCommandExecutor>(sp => (IApiCommandExecutor)sp.GetRequiredService<IMarketDataStreamer>());
        services.AddSingleton<ICommandWhitelistValidator, CommandWhitelistValidator>();
        services.AddSingleton<IWebSocketStreamService, WebSocketStreamService>();
        services.AddSingleton<ICoinbaseRestService, CoinbaseRestService>();
        services.AddHostedService<LocalApiHostedService>();

    })
    .Build();

// Check if this is a routing test
// (removed test routing code)

// Set up graceful shutdown handling
Console.CancelKeyPress += async (sender, e) =>
{
    e.Cancel = true; // Prevent immediate termination
    Console.WriteLine("\n⚠️  Graceful shutdown initiated...");
    
    var streamer = host.Services.GetRequiredService<IMarketDataStreamer>();
    Console.WriteLine("📊 Stopping service and exporting data...");
    await streamer.StopAsync();
    await host.StopAsync();
    Console.WriteLine("✅ Shutdown complete.");
    Environment.Exit(0);
};

// Start the main service and keep it running
var streamer = host.Services.GetRequiredService<IMarketDataStreamer>();
var configuration = host.Services.GetRequiredService<IConfiguration>();
var localApiEnabled = configuration.GetValue("AppConfiguration:LocalApi:Enabled", false);
var localApiApiOnlyMode = configuration.GetValue("AppConfiguration:LocalApi:ApiOnlyMode", false);

try
{
    await host.StartAsync();

    if (!localApiApiOnlyMode)
    {
        await streamer.StartAsync();

        if (IsHistoricalOnlyRun(args, configuration))
        {
            Console.WriteLine("✅ Historical-only run completed. Exiting automatically.");
            return;
        }
    }
    else if (localApiEnabled)
    {
        Console.WriteLine("✅ Local API command mode active. Waiting for API commands...");
    }
    
    // Keep the application running until manually stopped
    await Task.Delay(Timeout.Infinite);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Application was cancelled.");
}
finally
{
    await host.StopAsync();
}

static bool IsHistoricalOnlyRun(string[] args, IConfiguration configuration)
{
    var modeArg = args.FirstOrDefault(a => a.StartsWith("--mode=", StringComparison.OrdinalIgnoreCase));
    if (modeArg is not null)
    {
        var modeValue = modeArg.Substring("--mode=".Length).Trim().ToLowerInvariant();
        return modeValue == "historical-only";
    }

    var configuredMode = configuration["AppConfiguration:RunMode"]?.Trim().ToLowerInvariant();
    return configuredMode == "historical-only";
}