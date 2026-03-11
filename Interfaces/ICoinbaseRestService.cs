using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Core.Interfaces
{
    public interface ICoinbaseRestService
    {
        Task<string> GetPrivateEndpointAsync(string method, string path, object? body = null);
        Task<IReadOnlyList<Candle>> GetHistoricalCandlesAsync(
            string productId,
            DateTime startUtc,
            DateTime endUtc,
            int granularitySeconds,
            CancellationToken ct = default);
    }
}