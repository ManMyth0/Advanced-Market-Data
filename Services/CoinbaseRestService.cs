using System.Text;
using System.Text.Json;
using AdvancedMarketData.Core.Models;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Interfaces;

namespace AdvancedMarketData.Core.Services
{
    public class CoinbaseRestService : ICoinbaseRestService
    {
        private const int MaxHistoricalCandlesPerRequest = 300;
        private readonly HttpClient _httpClient;
        private readonly Ed25519JwtHelper? _jwtHelper;

        public CoinbaseRestService(HttpClient httpClient, Func<Ed25519JwtHelper?> jwtHelperFactory)
        {
            _httpClient = httpClient;
            _jwtHelper = jwtHelperFactory();
        }

        public async Task<string> GetPrivateEndpointAsync(string method, string path, object? body = null)
        {
            if (_jwtHelper == null)
            {
                throw new InvalidOperationException("JWT helper is not available. API credentials may not be configured.");
            }

            // 1. Build URI in "METHOD /path" form for JWT
            string jwt = _jwtHelper.GenerateJwt($"{method} {path}");

            // 2. Prepare request
            var request = new HttpRequestMessage(new HttpMethod(method), $"https://api.coinbase.com{path}");
            request.Headers.Add("Authorization", $"Bearer {jwt}");

            if (body != null)
            {
                request.Content = new StringContent(
                    JsonSerializer.Serialize(body),
                    Encoding.UTF8,
                    "application/json"
                );
            }

            // 3. Execute and read response
            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<IReadOnlyList<Candle>> GetHistoricalCandlesAsync(
            string productId,
            DateTime startUtc,
            DateTime endUtc,
            int granularitySeconds,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(productId))
                throw new ArgumentException("Product ID is required.", nameof(productId));

            if (!CandleGranularity.IsSupported(granularitySeconds))
                throw new ArgumentException($"Unsupported granularity: {granularitySeconds}", nameof(granularitySeconds));

            if (endUtc <= startUtc)
                throw new ArgumentException("endUtc must be greater than startUtc.");

            var candles = new List<Candle>();
            var currentStart = DateTime.SpecifyKind(startUtc, DateTimeKind.Utc);
            var finalEndExclusive = DateTime.SpecifyKind(endUtc, DateTimeKind.Utc);
            var maxRangeSeconds = granularitySeconds * MaxHistoricalCandlesPerRequest;

            while (currentStart < finalEndExclusive)
            {
                ct.ThrowIfCancellationRequested();

                var chunkEnd = currentStart.AddSeconds(maxRangeSeconds);
                if (chunkEnd > finalEndExclusive)
                {
                    chunkEnd = finalEndExclusive;
                }

                var responseBody = await GetExchangeCandlesChunkAsync(
                    productId,
                    currentStart,
                    chunkEnd,
                    granularitySeconds,
                    ct);

                var chunkCandles = ParseExchangeCandlesResponse(responseBody, productId);

                // Defensive filter: Coinbase may return candles outside requested start/end.
                candles.AddRange(chunkCandles.Where(c => c.Time >= currentStart && c.Time < chunkEnd));

                // Advance without gaps; boundaries are handled as [start, end) ranges.
                currentStart = chunkEnd;
            }

            return candles
                .OrderBy(c => c.Time)
                .ToList();
        }

        private async Task<string> GetExchangeCandlesChunkAsync(
            string productId,
            DateTime startUtc,
            DateTime endUtc,
            int granularitySeconds,
            CancellationToken ct)
        {
            var start = new DateTimeOffset(startUtc).ToUnixTimeSeconds();
            var end = new DateTimeOffset(endUtc).ToUnixTimeSeconds();
            var requestUri =
                $"https://api.exchange.coinbase.com/products/{productId}/candles?start={start}&end={end}&granularity={granularitySeconds}";

            using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
            request.Headers.Add("User-Agent", "Advanced-Market-Data");
            using var response = await _httpClient.SendAsync(request, ct);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(ct);
        }

        private static IReadOnlyList<Candle> ParseExchangeCandlesResponse(string json, string productId)
        {
            // Exchange API returns: [ time, low, high, open, close, volume ]
            var rawRows = JsonSerializer.Deserialize<List<List<decimal>>>(json) ?? new List<List<decimal>>();
            var candles = new List<Candle>(rawRows.Count);

            foreach (var row in rawRows)
            {
                if (row.Count < 6)
                    continue;

                var unixSeconds = Convert.ToInt64(row[0]);
                var time = DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
                var low = row[1];
                var high = row[2];
                var open = row[3];
                var close = row[4];
                var volume = row[5];

                candles.Add(new Candle(productId, time, open, high, low, close, volume));
            }

            return candles;
        }
    }
}