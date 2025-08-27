using AdvancedMarketData.Core.Models;
using AdvancedMarketData.Core.Interfaces;

namespace AdvancedMarketData.Core.Services
{
    public class AnalyticsService : IAnalyticsService
    {
        public decimal CalculateSMA(IEnumerable<Candle> candles, int period)
        {
            var closePrices = candles.Select(c => c.Close).TakeLast(period);
            return closePrices.Average();
        }
    }
}