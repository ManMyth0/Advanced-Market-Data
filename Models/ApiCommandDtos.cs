using System.Text.Json.Serialization;

namespace AdvancedMarketData.Core.Models;

public sealed class ApiCommandRequest
{
    [JsonPropertyName("command")]
    public string Command { get; set; } = string.Empty;

    [JsonPropertyName("args")]
    public Dictionary<string, string> Args { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonPropertyName("client")]
    public string Client { get; set; } = "llm-local";
}

public sealed class ApiCommandExecutionResponse
{
    [JsonPropertyName("requestId")]
    public string RequestId { get; set; } = Guid.NewGuid().ToString("D");

    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "failed";

    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;

    [JsonPropertyName("data")]
    public Dictionary<string, object?> Data { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class ApiStatusSnapshot
{
    [JsonPropertyName("isStreaming")]
    public bool IsStreaming { get; set; }

    [JsonPropertyName("activeMode")]
    public string ActiveMode { get; set; } = "idle";

    [JsonPropertyName("activeProducts")]
    public string[] ActiveProducts { get; set; } = Array.Empty<string>();

    [JsonPropertyName("collectedCandles")]
    public int CollectedCandles { get; set; }

    [JsonPropertyName("selectedGranularitySeconds")]
    public int SelectedGranularitySeconds { get; set; }

    [JsonPropertyName("selectedGranularityLabel")]
    public string SelectedGranularityLabel { get; set; } = "300s";
}

public sealed class WhitelistedCommandDefinition
{
    [JsonPropertyName("command")]
    public string Command { get; set; } = string.Empty;

    [JsonPropertyName("description")]
    public string Description { get; set; } = string.Empty;

    [JsonPropertyName("requiredArgs")]
    public string[] RequiredArgs { get; set; } = Array.Empty<string>();

    [JsonPropertyName("optionalArgs")]
    public string[] OptionalArgs { get; set; } = Array.Empty<string>();

    [JsonPropertyName("expectedOutcome")]
    public string ExpectedOutcome { get; set; } = string.Empty;
}
