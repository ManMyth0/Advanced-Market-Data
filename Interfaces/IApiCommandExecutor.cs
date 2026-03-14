using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Interfaces;

public interface IApiCommandExecutor
{
    Task<ApiCommandExecutionResponse> ExecuteApiCommandAsync(ApiCommandRequest request, CancellationToken ct = default);
    ApiStatusSnapshot GetApiStatusSnapshot();
}
