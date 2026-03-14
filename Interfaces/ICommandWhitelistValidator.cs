using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Interfaces;

public interface ICommandWhitelistValidator
{
    bool TryValidate(ApiCommandRequest request, out string error);
    IReadOnlyList<WhitelistedCommandDefinition> GetDefinitions();
}
