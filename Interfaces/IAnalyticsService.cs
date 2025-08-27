using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Core.Interfaces
{
    public interface IAnalyticsService
    {
        decimal CalculateSMA(IEnumerable<Candle> candles, int period);
        // Extend with EMA, RSI, etc.
    }
}