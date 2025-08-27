namespace AdvancedMarketData.Core.Interfaces
{
    public interface ICoinbaseRestService
    {
        Task<string> GetPrivateEndpointAsync(string method, string path, object? body = null);
    }
}