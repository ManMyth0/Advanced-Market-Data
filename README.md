# Advanced Market Data - Coinbase Advanced API Integration
**Version**: 1.0.0

A standalone .NET 9.0 application that streams real-time cryptocurrency market data from Coinbase Advanced API. Features live WebSocket connections, multi-asset streaming, intelligent price formatting, CSV export, JWT authentication, and clean real-time output display.

## Current Behavior (Authoritative)

This section is the source of truth for the current runtime behavior.

- **Live mode (`live-only`)**
  - Uses Coinbase Advanced Trade WebSocket `candles` channel.
  - Snapshot candles are skipped, so only live update flow is ingested.
  - Coinbase live candle buckets are fixed at **5 minutes**.
  - `--granularity` is not allowed in `live-only` mode (fails fast).
- **Historical mode (`historical-only`)**
  - Uses Coinbase Exchange REST candles endpoint with date range chunking.
  - Supported granularity values:
    - Seconds: `60, 300, 900, 3600, 21600, 86400`
    - Names: `OneMinute, FiveMinutes, FifteenMinutes, OneHour, SixHours, OneDay`
  - Uses max `300` candles/request and automatically paginates a full UTC day.
  - Auto-exports CSV at completion and exits automatically.
- **Historical + live mode (`historical-then-live`)**
  - Advanced mode for one-command preload + stream workflows.
  - Loads historical candles first from input date `00:00:00` UTC up to current UTC time.
  - Uses fixed 5-minute granularity for this backfill.
  - Then starts live stream (5-minute WebSocket candles).
  - Uses a continuous 5-minute interval across backfill and live stream.

### Important Data Caveat

For historical candles, Coinbase documents that intervals with no ticks may be missing, so full-day minute coverage is product/day dependent. Reference:
- [Get product candles (Exchange API)](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)

## Features

- **Live Market Data**: Real-time cryptocurrency price and volume updates
- **Multi-Asset Streaming**: Stream up to 4 pairs (unauthenticated) or 50 pairs (authenticated) simultaneously
- **Intelligent Price Formatting**: Automatic precision adjustment (BTC: $109,535, SHIB: $0.00001188)
- **Independent Stream Management**: Each asset has its own heartbeat tracking and connection health
- **WebSocket Streaming**: Direct connection to Coinbase Advanced API WebSocket endpoints  
- **Flexible Input**: Command-line arguments, interactive prompts, or predefined asset lists
- **Configurable Candle Granularity**: 1m, 5m, 15m, 1h, 6h, 1d controls
- **Historical + Live Modes**: Query a day of historical OHLCV, stream live-only, or chain both
- **Authentication Support**: Optional JWT authentication for higher rate limits and private data
- **Connection Health**: Automatic heartbeat monitoring and reconnection handling
- **Smart CSV Export**: Auto-timestamped files with export control options
- **Flexible Memory Management**: Configurable limits or unlimited collection
- **Clean Output**: Human-readable real-time display without JSON noise
- **Rate Limiting**: Automatic compliance with Coinbase API limits
- **Cross-Platform**: Windows, macOS, and Linux support

## Requirements

- .NET 9.0 Runtime
- Windows, macOS, or Linux
- Internet connection for Coinbase API access

## Quick Start

1. Clone and run:
```bash
git clone <repository-url>
cd Advanced-Market-Data
dotnet run -- --products=BTC-USD
```

2. You'll see real-time output like:
```
info: Using command line override: [BTC-USD]
info: Connected to WebSocket at wss://advanced-trade-ws.coinbase.com for BTC-USD
info: Heartbeat #516
info: Heartbeat #517
info: LIVE: BTC-USD $109893 Vol:66.2 BTC [20:40:00]
info: Heartbeat #518
```

### Multiple Asset Streaming:
```bash
dotnet run -- --products=BTC-USD,SHIB-USD,ETH-USD
```
```
info: Heartbeat #27614  # BTC stream
info: Heartbeat #13674  # SHIB stream
info: Heartbeat #17077  # ETH stream
info: LIVE: BTC-USD $109535 Vol:21.7 BTC [21:10:00]
info: LIVE: SHIB-USD $0.00001188 Vol:19082653413.0 BTC [21:10:00]
info: LIVE: ETH-USD $4352.59 Vol:1005.1 BTC [21:10:00]
```

## Configuration

### Asset Pair Selection

The application uses **only two sources** for asset pairs:

1. **Manual Override** (highest priority): Command line arguments
2. **Asset Pairs File** (fallback): `asset-pairs.txt` file

**Note**: Historical granularity supports both seconds and names:
- Seconds: `60, 300, 900, 3600, 21600, 86400`
- Names: `OneMinute, FiveMinutes, FifteenMinutes, OneHour, SixHours, OneDay`

### What You Need to Configure

**1. Asset Pairs** - Edit `asset-pairs.txt`:
```
# Uncomment the pairs you want to stream by removing the '#', or insert your own pairs following the established format
BTC-USD
# ETH-USD  
# SOL-USD
```

**2. API Credentials** (optional, for accessing your own coinbase trades. Use Coinbase Advanced Trading Api Key & Secret) - Use user secrets:
```bash
dotnet user-secrets set "CoinbaseApi:ApiKeyId" "your-api-key-id"
dotnet user-secrets set "CoinbaseApi:ApiSecret" "your-api-secret"
```

**3. CSV Export Options** (optional) - Command line flags:
```bash
--no-csv         # Disable auto-export:  dotnet run -- --products=BTC-USD--no-csv
--csv=false      # Disable auto-export
```

**4. Memory Management** (optional) - Edit `appsettings.json`

**Memory Options:**
- **Limited (Default)**: `"Enabled": true, "Amount": 5000` - Keeps last 5000 candles
- **Unlimited**: `"Enabled": false` - Keeps all candles (use for complete CSV export)

**When to Use Each:**
- **Limited**: Long-running sessions, memory-constrained environments
- **Unlimited**: Short sessions, complete historical data needed for CSV

## Advanced Configuration

### Complete AppConfiguration Options

Edit `appsettings.json` for advanced settings:

```json
{
  "AppConfiguration": {
    "AssetPairsFile": "asset-pairs.txt",           // Path to asset pairs file
    "MaxConcurrentSubscriptions": 4,               // Rate limit enforcement  
    "AllowManualOverride": true,                   // Allow command line override
    "PromptForApiSetup": true,                     // Show interactive API setup
    "AutoExportOnExit": true,                      // Auto-create CSV on exit
    "PromptForExportIfDisabled": true,             // Ask user if auto-export disabled
    "EnableRealTimeExport": false,                 // Export continuously during streaming
    "CsvExportPath": "candles.csv",                // Legacy single-file path
    "MaxCandlesInMemory": {                        // Memory management
      "Enabled": true,                             // Enable memory limits
      "Amount": 5000                               // Max candles to keep
    }
  }
}

// OR To remove the candle's memory constraint, ommit the "Amount" property and changed the "Enabled" property to false:
    "MaxCandlesInMemory": {
      "Enabled": false
    }

```

### Advanced Options Explained

**CSV Export Control:**
- `AutoExportOnExit`: Automatically export on Ctrl+C (default: true)
- `PromptForExportIfDisabled`: Ask user when auto-export disabled (default: true)  
- `EnableRealTimeExport`: Export continuously during streaming (default: false)

**Rate Limiting:**
- `MaxConcurrentSubscriptions`: Enforces Coinbase API limits (default: 4)
- Automatically adjusts based on authentication (4 unauthenticated, 50 authenticated)

**User Experience:**
- `PromptForApiSetup`: Show interactive credential setup (default: true)
- `AllowManualOverride`: Allow command line to override file (default: true)

## Data Flow & Timeframes

### **Coinbase Candles Channel**
- **Fixed Timeframe**: 5-minute buckets for live WebSocket candle stream
- **Initial Data**: Snapshot with ~100 historical 5-minute candles (8+ hours)
- **Live Updates**: Real-time `update` events **every second** when trading occurs
- **Event Types**: 
  - `snapshot`: Historical candles sent on connection
  - `update`: New/modified candles as trading happens

### **Custom Timeframes**
- **Historical REST candles** support configurable granularity (e.g. `60`, `300`, `900`, `3600`, `21600`, `86400`).
- **Live WebSocket candles** remain fixed to 5-minute intervals from Coinbase.

### Asset Pairs File (`asset-pairs.txt`)
```
# Coinbase Advanced API - Supported Asset Pairs
# RATE LIMITS: 8 messages/sec (unauthenticated) = MAX 4 pairs, 100 messages/sec (authenticated) = MAX 50 pairs
# Each asset pair uses 2 messages: heartbeats + candles subscription
# Format: ASSET-QUOTE (e.g., BTC-USD)
# One pair per line, comments start with #

# Major Cryptocurrencies
#BTC-USD
#ETH-USD
#SOL-USD

# Popular Altcoins
# ADA-USD
# DOT-USD
# MATIC-USD

# Add your own pairs here...
```

| Mode | Rate Limit | Public Data | Private Data | Setup Required |
|------|------------|-------------|--------------|----------------|
| **Unauthenticated** | 8 msg/sec | ✅ Full access to candles, heartbeats | ❌ No access to your trades | None |
| **Authenticated** | 100 msg/sec | ✅ Same public data access | ✅ Access to your trades, balances | API credentials |

**Important Clarifications:**
- **Asset Pair Limits**: No difference between authenticated/unauthenticated for public candle data
- **Rate Limits**: Higher for authenticated (100 vs 8 msg/sec) but public data is still limited by Coinbase
- **Authentication Purpose**: Only for accessing your private trade data, not for more asset pairs
- **Public Data**: Candles and heartbeats work the same regardless of authentication!

**Quick Start:**
1. **Stream public data** with any number of pairs (no setup needed)
2. **Add API credentials** if you want to monitor your own trades
3. **Private channels** require authentication for your personal trading data

### Usage Examples

#### Using Asset Pairs File (Default)
```bash
dotnet run
# Reads from asset-pairs.txt (only uncommented pairs)
```

#### Manual Override - Command Line
```bash
dotnet run -- --products=BTC-USD,ETH-USD,SOL-USD
# Overrides file, uses command line pairs
# Each asset gets independent stream with proper heartbeat tracking
```

#### Historical Day Query + Live Separation
```bash
# Historical only (single UTC day)
dotnet run -- --products=BTC-USD --mode=historical-only --history-date=2026-03-01 --granularity=300

# Equivalent named granularity
dotnet run -- --products=BTC-USD --mode=historical-only --history-date=2026-03-01 --granularity=FiveMinutes

# Live only from app start onward (5-minute WS candles)
dotnet run -- --products=BTC-USD --mode=live-only

# Advanced: load history first, then continue with live candles
dotnet run -- --products=BTC-USD --mode=historical-then-live --history-date=2026-03-01
```

Notes:
- In `live-only` mode, Coinbase WebSocket candles are fixed to 5-minute buckets.
- `--granularity` is accepted only with `--mode=historical-only`.
- Using `--granularity` with other modes fails fast with a validation error.
- Historical export filenames preserve your input style:
  - `--granularity=300` -> `candles_300s_...csv`
  - `--granularity=FiveMinutes` -> `candles_FiveMinutes_...csv`

Strongest use case for `historical-then-live`:
- "Give me today's candles so far, then keep streaming new candles without running a second command."
- Example:
  ```bash
  dotnet run -- --products=BTC-USD --mode=historical-then-live --history-date=2026-03-08
  ```

#### CSV Export Control
```bash
# Auto-export (default)
dotnet run -- --products=BTC-USD

# Disable auto-export  
dotnet run -- --products=BTC-USD --no-csv
```

### Command Reference (With Expected Output)

#### 1) Live-only stream (default behavior)
```bash
dotnet run -- --products=BTC-USD --mode=live-only
```
Expected:
- Starts WebSocket stream with heartbeats + live candle updates.
- No snapshot candle ingestion in live mode.
- Exports CSV on `Ctrl+C` (default enabled), filename like `candles_300s_BTC_USD_<date>.csv`.

#### 2) Historical-only by date (seconds granularity)
```bash
dotnet run -- --products=BTC-USD --mode=historical-only --history-date=2026-03-01 --granularity=300
```
Expected:
- Loads historical candles for that UTC day (`00:00:00` to `23:59:59` window).
- Uses chunked REST retrieval at max 300 candles/request.
- Auto-exports and exits automatically, filename like `candles_300s_BTC_USD_<date>.csv`.

#### 3) Historical-only by date (named granularity)
```bash
dotnet run -- --products=BTC-USD --mode=historical-only --history-date=2026-03-01 --granularity=FiveMinutes
```
Expected:
- Same behavior as `--granularity=300`.
- Filename preserves named style: `candles_FiveMinutes_BTC_USD_<date>.csv`.

#### 4) Historical-then-live (advanced)
```bash
dotnet run -- --products=BTC-USD --mode=historical-then-live --history-date=2026-03-08
```
Expected:
- Backfills from `2026-03-08 00:00:00` UTC to current UTC time using 5-minute candles.
- Then continues live WebSocket streaming.
- On `Ctrl+C`, exports one continuous 5-minute series to CSV.

#### 5) Invalid: granularity with live-only
```bash
dotnet run -- --products=BTC-USD --mode=live-only --granularity=60
```
Expected:
- Fails fast with validation error:
  - `--granularity is only supported when --mode=historical-only.`

#### 6) Optional: disable auto-export
```bash
dotnet run -- --products=BTC-USD --mode=live-only --no-csv
```
Expected:
- Streams normally.
- No automatic CSV export on shutdown.

## CSV Output

- **Naming**
  - Live/default export: `candles_300s_<PRODUCT>_<MM-dd-yyyy>.csv`
  - Historical numeric granularity: `candles_<seconds>s_<PRODUCT>_<MM-dd-yyyy>.csv`
  - Historical named granularity: `candles_<Name>_<PRODUCT>_<MM-dd-yyyy>.csv` (example: `FiveMinutes`)
- **Header format**
  - `<export UTC timestamp>, <count> Candles, <first candle time> to <last candle time>`
- **Rows**
  - `Asset: <Product>, Time: <yyyy-MM-dd HH:mm:ss>, Open: ..., High: ..., Low: ..., Close: ..., Volume: ...`

## Services Used

- `MarketDataStreamer`: mode selection, orchestration, export lifecycle.
- `WebSocketStreamService`: live WebSocket connection, heartbeat handling, candle updates.
- `CoinbaseRestService`: historical REST retrieval with chunking.
- `CsvService`: CSV write/read utilities.

## Notes

- This project currently focuses on OHLCV ingestion and export, not analytics pipelines.
- For historical data caveats and endpoint behavior, see:
  - [Get product candles (Exchange API)](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)

## Local API for LLM Integration

This app can expose a local JSON API so any local LLM client can issue approved commands without shell access.

- **Enable API in `appsettings.json`**
  - `AppConfiguration:LocalApi:Enabled = true`
  - `AppConfiguration:LocalApi:ApiOnlyMode = true`
  - `AppConfiguration:LocalApi:Host = 127.0.0.1`
  - `AppConfiguration:LocalApi:Port = 5058`
  - `AppConfiguration:LocalApi:AuditLog:Directory = logs`
  - `AppConfiguration:LocalApi:AuditLog:RetentionDays = 31`
- **Run the app**
  - `dotnet run`
- **Core endpoints**
  - `GET /health`
  - `GET /docs/whitelist`
  - `GET /docs/readme`
  - `POST /commands/execute`
- **Whitelisted commands**
  - `start_live_stream`
  - `run_historical_only`
  - `run_historical_then_live`
  - `stop_stream`
  - `get_status`
- **Whitelist document**
  - `docs/llm-command-whitelist.json`

### API Audit Logging

Every `POST /commands/execute` attempt writes an audit log entry with:

- UTC timestamp
- command name
- request ID
- sanitized args summary
- success/failure
- failure type and message (when failed)
- file output at `logs/api-command-audit-YYYY-MM-DD.jsonl`
- automatic retention cleanup after 31 days

Security guardrails:

- Log files are **not** exposed through API endpoints.
- Any log-related command request is rejected by whitelist validation.
- `/docs/readme` always serves `README.md` only (no arbitrary file path reads).

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes. Use at your own risk. The author(s) are not responsible for any financial losses or damages resulting from the use of this software.

---