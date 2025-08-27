namespace AdvancedMarketData.Core.Models;

public record StreamingStatistics(int TotalCandles, decimal TotalVolume, DateTime FirstCandleTime, DateTime LastCandleTime);

