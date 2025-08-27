using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Core.Interfaces
{
    public interface IWebSocketStreamService
    {
        Task StartStreamingAsync(string[] channels, string[] productIds, CancellationToken ct);
        event Action<Candle> OnCandleReceived;
    }
}