namespace AdvancedMarketData.Core.Models
{
    public static class CandleGranularity
    {
        public const int OneMinute = 60;
        public const int FiveMinutes = 300;
        public const int FifteenMinutes = 900;
        public const int OneHour = 3600;
        public const int SixHours = 21600;
        public const int OneDay = 86400;

        public static readonly int[] SupportedSeconds =
        {
            OneMinute,
            FiveMinutes,
            FifteenMinutes,
            OneHour,
            SixHours,
            OneDay
        };

        public static bool IsSupported(int seconds)
        {
            return SupportedSeconds.Contains(seconds);
        }

        public static bool TryParse(string input, out int seconds)
        {
            return TryParse(input, out seconds, out _);
        }

        public static bool TryParse(string input, out int seconds, out string exportLabel)
        {
            seconds = FiveMinutes;
            exportLabel = $"{FiveMinutes}s";
            if (string.IsNullOrWhiteSpace(input))
            {
                return false;
            }

            var trimmed = input.Trim();
            if (int.TryParse(trimmed, out var parsedSeconds) && IsSupported(parsedSeconds))
            {
                seconds = parsedSeconds;
                exportLabel = $"{parsedSeconds}s";
                return true;
            }

            var normalized = Normalize(trimmed);
            var parsed = normalized switch
            {
                "oneminute" => (true, OneMinute, "OneMinute"),
                "fiveminutes" => (true, FiveMinutes, "FiveMinutes"),
                "fifteenminutes" => (true, FifteenMinutes, "FifteenMinutes"),
                "onehour" => (true, OneHour, "OneHour"),
                "sixhours" => (true, SixHours, "SixHours"),
                "oneday" => (true, OneDay, "OneDay"),
                _ => (false, seconds, exportLabel)
            };

            if (!parsed.Item1)
            {
                return false;
            }

            seconds = parsed.Item2;
            exportLabel = parsed.Item3;
            return true;
        }

        public static string SupportedValuesText =>
            "60|300|900|3600|21600|86400 or OneMinute|FiveMinutes|FifteenMinutes|OneHour|SixHours|OneDay";

        public static string Describe(int seconds)
        {
            return seconds switch
            {
                OneMinute => "1 minute",
                FiveMinutes => "5 minutes",
                FifteenMinutes => "15 minutes",
                OneHour => "1 hour",
                SixHours => "6 hours",
                OneDay => "1 day",
                _ => $"{seconds} seconds"
            };
        }

        private static string Normalize(string value)
        {
            return value
                .Replace("_", string.Empty)
                .Replace("-", string.Empty)
                .Replace(" ", string.Empty)
                .ToLowerInvariant();
        }
    }
}
