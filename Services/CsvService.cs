using CsvHelper;
using System.Globalization;
using AdvancedMarketData.Core.Models;
using AdvancedMarketData.Core.Interfaces;

namespace AdvancedMarketData.Core.Services
{
    public class CsvService : ICsvService
    {
            public void SaveCandles(IEnumerable<Candle> candles, string filePath)
    {
        using var writer = new StreamWriter(filePath);
        
        var candleList = candles.ToList();
        var exportTime = DateTime.UtcNow;
        var dateRange = candleList.Count > 0 
            ? $"{candleList.First().Time:yyyy-MM-dd HH:mm:ss} to {candleList.Last().Time:yyyy-MM-dd HH:mm:ss}"
            : "No data";
        
        // Write only essential metadata with proper spacing
        writer.WriteLine($"{exportTime:yyyy-MM-dd HH:mm:ss UTC}, {candleList.Count} Candles (5-minute), {dateRange}");
        writer.WriteLine(); // Empty line for separation
        
        // Write candle data with proper spacing and descriptive labels
        foreach (var candle in candleList)
        {
            writer.WriteLine($"Asset: {candle.ProductId}, " +
                           $"Time: {candle.Time:yyyy-MM-dd HH:mm:ss}, " +
                           $"Open: {candle.Open:F8}, " +
                           $"High: {candle.High:F8}, " +
                           $"Low: {candle.Low:F8}, " +
                           $"Close: {candle.Close:F8}, " +
                           $"Volume: {candle.Volume:F8}");
        }
    }

        public IEnumerable<Candle> LoadCandles(string filePath)
        {
            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            return csv.GetRecords<Candle>().ToList();
        }
    }
}