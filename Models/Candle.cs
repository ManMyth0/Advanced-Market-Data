namespace AdvancedMarketData.Core.Models
{
    public record Candle(string ProductId, DateTime Time, decimal Open, decimal High, decimal Low, decimal Close, decimal Volume);
}