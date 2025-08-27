using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Core.Interfaces
{
    public interface ICsvService
    {
        void SaveCandles(IEnumerable<Candle> candles, string filePath);
        IEnumerable<Candle> LoadCandles(string filePath);
    }
}