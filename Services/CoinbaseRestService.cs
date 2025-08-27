using System.Text;
using System.Text.Json;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Interfaces;

namespace AdvancedMarketData.Core.Services
{
    public class CoinbaseRestService : ICoinbaseRestService
{
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
}
}