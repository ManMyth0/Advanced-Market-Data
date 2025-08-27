namespace AdvancedMarketData.Interfaces;

public interface IMarketDataStreamer : IDisposable
{
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync();
    Task ExportToCsvAsync(string filePath);
    Task ExportCurrentDataAsync();
}