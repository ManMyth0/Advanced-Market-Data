using Microsoft.Extensions.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using AdvancedMarketData.Streaming;
using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Services;
using AdvancedMarketData.Core.Interfaces;
using AdvancedMarketData.Core;
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
        services.AddSingleton<IWebSocketStreamService, WebSocketStreamService>();
        services.AddSingleton<ICoinbaseRestService, CoinbaseRestService>();

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
    Console.WriteLine("✅ Shutdown complete.");
    Environment.Exit(0);
};

// Start the main service and keep it running
var streamer = host.Services.GetRequiredService<IMarketDataStreamer>();

try
{
    await streamer.StartAsync();
    
    // Keep the application running until manually stopped
    await Task.Delay(Timeout.Infinite);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Application was cancelled.");
}