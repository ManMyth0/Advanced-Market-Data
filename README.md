# Advanced Market Data - Coinbase Advanced API Integration
**Version**: 1.0.0

A standalone .NET 9.0 application that streams real-time cryptocurrency market data from Coinbase Advanced API. Features live WebSocket connections, multi-asset streaming, intelligent price formatting, CSV export, JWT authentication, and clean real-time output display.

## Features

- **Live Market Data**: Real-time cryptocurrency price and volume updates
- **Multi-Asset Streaming**: Stream up to 4 pairs (unauthenticated) or 50 pairs (authenticated) simultaneously
- **Intelligent Price Formatting**: Automatic precision adjustment (BTC: $109,535, SHIB: $0.00001188)
- **Independent Stream Management**: Each asset has its own heartbeat tracking and connection health
- **WebSocket Streaming**: Direct connection to Coinbase Advanced API WebSocket endpoints  
- **Flexible Input**: Command-line arguments, interactive prompts, or predefined asset lists
- **5-Minute Candles**: OHLCV data with historical snapshots and live updates
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
info: AUTHENTICATED MODE: 100 msg/sec = Up to 50 asset pairs supported
info: Using command line override: [BTC-USD]
info: Connected to WebSocket at wss://advanced-trade-ws.coinbase.com for BTC-USD
info: Received candle snapshot for BTC-USD with 100 candles
info: Snapshot 1/100: BTC-USD $111261 Vol:56.3
info: Snapshot 10/100: BTC-USD $111526 Vol:14.0
...
info: Snapshot 100/100: BTC-USD $109876 Vol:65.4
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
info: Snapshot 1/100: BTC-USD $109535 Vol:21.7
info: Snapshot 1/100: SHIB-USD $0.00001188 Vol:19082653413.0
info: Snapshot 1/100: ETH-USD $4352.59 Vol:1005.1
info: Heartbeat #27614  # BTC stream
info: Heartbeat #13674  # SHIB stream  
info: Heartbeat #17077  # ETH stream
info: LIVE: BTC-USD $109535 Vol:21.7 BTC [21:10:00]
info: LIVE: SHIB-USD $0.00001188 Vol:19082653413.0 BTC [21:10:00]
info: LIVE: ETH-USD $4352.59 Vol:1005.1 BTC [21:10:00]
```

Notice the **intelligent price formatting** and **independent heartbeat tracking** per asset!

## Configuration

### Asset Pair Selection

The application uses **only two sources** for asset pairs:

1. **Manual Override** (highest priority): Command line arguments
2. **Asset Pairs File** (fallback): `asset-pairs.txt` file

**Note**: Coinbase candles are **fixed at 5-minute intervals** - no timeframe selection available.

### What You Need to Configure

**1. Asset Pairs** - Edit `asset-pairs.txt`:
```
# Uncomment the pairs you want to stream by removing the '#', or insert your own pairs following the established format
BTC-USD
# ETH-USD  
# SOL-USD
```

**2. API Credentials** (optional, for accessing your own coinbase trades) - Use user secrets:
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
- **Fixed Timeframe**: 5-minute buckets (no user selection available)
- **Initial Data**: Snapshot with ~100 historical 5-minute candles (8+ hours)
- **Live Updates**: Real-time `update` events **every second** when trading occurs
- **Event Types**: 
  - `snapshot`: Historical candles sent on connection
  - `update`: New/modified candles as trading happens

### **Custom Timeframes**
To create other intervals (1-minute, 1-hour, etc.), you can:
1. Collect the 5-minute candles from our stream
2. Aggregate them programmatically:
   - **1-hour candle** = Combine 12 consecutive 5-minute candles
   - **1-day candle** = Combine 288 consecutive 5-minute candles

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

#### CSV Export Control
```bash
# Auto-export (default)
dotnet run -- --products=BTC-USD

# Disable auto-export  
dotnet run -- --products=BTC-USD --no-csv
```

## API Authentication Setup

### **Why Authenticate?**
- **12.5x Rate Limit Increase**: 4 → 50 asset pairs
- **Private Channels**: Monitor your own trades and balances
- **Production Ready**: Suitable for real applications

### **Seamless Setup (1-Click):**

**Option 1: Interactive Setup (Recommended)**
```bash
dotnet run
# App detects no credentials and prompts:
# "Set up API credentials now? (y/n): y"
# Follow the guided setup - credentials stored securely!
```

**Option 2: Manual Setup**
1. **Get Coinbase API Credentials:**
   - Go to [Coinbase Advanced Trade](https://www.coinbase.com/advanced-trade/api)
   - Create new API key with **"trade"** permissions
   - Save your `API Key ID` and `API Secret`

2. **Configure Credentials (Secure):**
   ```bash
   dotnet user-secrets set "CoinbaseApi:ApiKeyId" "your-api-key-id"
   dotnet user-secrets set "CoinbaseApi:ApiSecret" "your-api-secret"
   ```
   
   **Alternative: Direct secrets.json editing:**
   ```json
   {
     "CoinbaseApi": {
       "MarketDataEndpoint": "wss://advanced-trade-ws.coinbase.com",
       "UserOrderDataEndpoint": "wss://advanced-trade-ws-user.coinbase.com",
       "ApiKeyId": "api-key-goes-here",
       "ApiSecret": "secret-goes-here"
     }
   }
   ```
   
   **Note:** Endpoints are already configured in `appsettings.json`, so you only need `ApiKeyId` and `ApiSecret` in secrets. The complete structure above shows all possible CoinbaseApi settings for reference.

3. **Restart Application:**
   ```bash
   dotnet run
   # You'll see: "AUTHENTICATED MODE: 100 msg/sec = Up to 50 asset pairs supported"
   ```

**Security:** Credentials are stored securely in user secrets, not in code.

**Disable Prompts:** Set `"PromptForApiSetup": false` in config to skip interactive setup.

### **User Secrets Location**

The `secrets.json` file is automatically created by .NET in:
- **Windows:** `%APPDATA%\Microsoft\UserSecrets\[user-secrets-id]\secrets.json`
- **macOS/Linux:** `~/.microsoft/usersecrets/[user-secrets-id]/secrets.json`

You can also edit it directly using: `dotnet user-secrets list` to view current secrets.

## Data Format

The application streams OHLCV (Open, High, Low, Close, Volume) candle data in real-time. Each candle includes:

- **Time**: Timestamp of the candle
- **Open**: Opening price
- **High**: Highest price during the period
- **Low**: Lowest price during the period
- **Close**: Closing price
- **Volume**: Trading volume during the period

## CSV Export

### **Automatic Export Features**
- **Smart Filenames**: `candles_5min_BTC_USD_08-25-2025.csv` (includes candle type and date)
- **Separate Files**: Each asset gets its own CSV file for better organization
- **Descriptive Headers**: Includes export metadata and candle type information
- **Proper Spacing**: Clean, readable format with spaces after commas
- **Volume Accuracy**: Base asset volume (BTC volume for BTC-USD, ETH volume for ETH-USD)

### **Export Control Options**

#### **Default Behavior - Auto Export**
```bash
dotnet run -- --products=BTC-USD,ETH-USD
# [Ctrl+C] → Automatically creates CSV files:
# ✅ candles_5min_BTC_USD_08-25-2025.csv
# ✅ candles_5min_ETH_USD_08-25-2025.csv
```

#### **Disable Auto Export**
```bash
# Option 1: Command line flag
dotnet run -- --products=BTC-USD --no-csv
dotnet run -- --products=BTC-USD --csv=false

# Option 2: Configuration
# Set "AutoExportOnExit": false in appsettings.json
```

#### **Interactive Export Prompt**
When auto-export is disabled, you'll be prompted at shutdown:
```
📊 Would you like to export the collected data to CSV files? (y/n):
```
- Press **Y** = Export CSV files
- Press **N** = Discard data
- Press **Enter** = Default to Yes

### **CSV File Format**

**Filename**: `candles_5min_BTC_USD_08-25-2025.csv`

**Content Example**:
```csv
2025-08-25 21:55:25 UTC, 100 Candles (5-minute), 2025-08-25 05:20:00 to 2025-08-25 21:55:00

Asset: BTC-USD, Time: 2025-08-25 05:20:00, Open: 112587.60000000, High: 112619.00000000, Low: 112486.08000000, Close: 112487.66000000, Volume: 6.95725607
Asset: BTC-USD, Time: 2025-08-25 05:25:00, Open: 112487.64000000, High: 112549.57000000, Low: 112461.93000000, Close: 112505.61000000, Volume: 8.80207340
...
```

### **Volume Interpretation**
- **BTC-USD**: Volume = BTC units traded (e.g., Volume: 6.95725607 BTC ≈ How many Bitcoin Were Traded in that candle )
- **ETH-USD**: Volume = ETH units traded 
- **SHIB-USD**: Volume = SHIB units traded
- **Standard OHLCV**: Follows industry-standard candlestick data format

### **Export Control Options**

**Command Line Flags**:
```bash
# Default: Auto-export enabled
dotnet run -- --products=BTC-USD

**appsettings.json**:
```json
{
  "AppConfiguration": {
    "AutoExportOnExit": true,          // Auto-create CSV on exit
    "PromptForExportIfDisabled": true, // Ask user if auto-export disabled
    "CsvExportPath": "candles.csv",    // Legacy single-file path
    "MaxCandlesInMemory": {            // OR "Enabled": false, ommit the amount
  "Enabled": true,
  "Amount": 5000
}
  }
}

# Disable auto-export to .csv
dotnet run -- --products=BTC-USD --no-csv
dotnet run -- --products=BTC-USD --csv=false
```

**Interactive Prompt**: When auto-export is disabled, you'll be prompted at shutdown to save data.

### **Multiple Asset Handling**
- **Individual Files**: Each asset gets its own timestamped CSV file
- **No Combined File**: Cleaner organization with separate files per asset
- **Consistent Naming**: `candles_5min_{ASSET}_{DATE}.csv` format

### **Manual Export During Runtime**
The application supports manual export via the `ExportCurrentDataAsync()` method for programmatic integration.

##Monitoring

The application provides real-time monitoring including:
- Total candles received
- Total volume processed
- Candles per second rate
- Volume per second rate
- Data range information
- Connection status and health

## Architecture

- **Interfaces**: Clean separation of concerns with interface-based design
- **Services**: Modular services for different functionalities
- **Models**: Strongly-typed data models
- **Configuration**: Flexible configuration management
- **Logging**: Structured logging throughout the pipeline
- **Error Handling**: Comprehensive error handling and recovery

## Development

### Project Structure
```
Advanced-Market-Data/
├── Interfaces/          # Service interfaces
├── Models/             # Data models and configuration
├── Services/           # Implementation of services
├── Utility/            # Utility classes and helpers
├── Program.cs          # Main application entry point
├── appsettings.json    # Configuration file
└── TODO.md            # Development progress tracking
```

### Key Services
- **MarketDataStreamer**: Main orchestrator for streaming operations
- **CandleStreamService**: WebSocket connection and candle parsing
- **CsvService**: CSV import/export functionality
- **AnalyticsService**: Basic technical analysis (SMA, etc.)
- **MonitoringService**: Real-time performance monitoring

## Error Handling

The application includes robust error handling:
- Automatic WebSocket reconnection (up to 5 attempts)
- Graceful degradation when individual product streams fail
- Comprehensive logging of all errors and warnings
- Memory management to prevent memory leaks

## Logging

Logs are structured and include:
- Connection status and WebSocket events
- Data reception and processing
- Error conditions and recovery attempts
- Performance metrics and statistics
- Export operations and file operations

## Use Cases

- **Real-time Trading**: Monitor cryptocurrency prices in real-time
- **Data Analysis**: Collect historical data for analysis
- **Algorithmic Trading**: Feed data to trading algorithms
- **Research**: Academic or market research purposes
- **Integration**: Use as a data source for other applications

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes. Use at your own risk. The author(s) are not responsible for any financial losses or damages resulting from the use of this software.

---