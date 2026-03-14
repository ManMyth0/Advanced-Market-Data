using AdvancedMarketData.Interfaces;
using AdvancedMarketData.Core.Models;

namespace AdvancedMarketData.Core.Services;

public sealed class CommandWhitelistValidator : ICommandWhitelistValidator
{
    private static readonly IReadOnlyList<WhitelistedCommandDefinition> Definitions =
    [
        new WhitelistedCommandDefinition
        {
            Command = "start_live_stream",
            Description = "Starts live-only candle streaming for products.",
            RequiredArgs = ["products"],
            OptionalArgs = [],
            ExpectedOutcome = "Streaming starts and status shows active products."
        },
        new WhitelistedCommandDefinition
        {
            Command = "run_historical_only",
            Description = "Fetches historical candles for a UTC day and auto-exports CSV.",
            RequiredArgs = ["products", "history_date", "granularity"],
            OptionalArgs = [],
            ExpectedOutcome = "Historical candles are fetched and CSV export metadata is returned."
        },
        new WhitelistedCommandDefinition
        {
            Command = "run_historical_then_live",
            Description = "Backfills historical candles from date to now, then starts live stream.",
            RequiredArgs = ["products", "history_date"],
            OptionalArgs = [],
            ExpectedOutcome = "Backfill completes and live stream stays active."
        },
        new WhitelistedCommandDefinition
        {
            Command = "stop_stream",
            Description = "Stops active stream and runs export-on-exit behavior.",
            RequiredArgs = [],
            OptionalArgs = [],
            ExpectedOutcome = "Streaming stops and stop summary is returned."
        },
        new WhitelistedCommandDefinition
        {
            Command = "get_status",
            Description = "Returns current runtime status snapshot.",
            RequiredArgs = [],
            OptionalArgs = [],
            ExpectedOutcome = "Current mode, products, and candle counters are returned."
        }
    ];

    public bool TryValidate(ApiCommandRequest request, out string error)
    {
        error = string.Empty;
        if (request is null)
        {
            error = "Request body is required.";
            return false;
        }

        var command = request.Command?.Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(command))
        {
            error = "Field 'command' is required.";
            return false;
        }

        if (command.Contains("log", StringComparison.OrdinalIgnoreCase))
        {
            error = "Log access commands are not permitted.";
            return false;
        }

        var definition = Definitions.FirstOrDefault(d =>
            d.Command.Equals(command, StringComparison.OrdinalIgnoreCase));

        if (definition is null)
        {
            error = $"Unknown command '{request.Command}'.";
            return false;
        }

        var args = request.Args ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var allowed = new HashSet<string>(
            definition.RequiredArgs.Concat(definition.OptionalArgs),
            StringComparer.OrdinalIgnoreCase);

        foreach (var required in definition.RequiredArgs)
        {
            if (!args.TryGetValue(required, out var value) || string.IsNullOrWhiteSpace(value))
            {
                error = $"Missing required argument '{required}' for command '{definition.Command}'.";
                return false;
            }
        }

        foreach (var key in args.Keys)
        {
            if (!allowed.Contains(key))
            {
                error = $"Unknown argument '{key}' for command '{definition.Command}'.";
                return false;
            }
        }

        return true;
    }

    public IReadOnlyList<WhitelistedCommandDefinition> GetDefinitions()
    {
        return Definitions;
    }
}
