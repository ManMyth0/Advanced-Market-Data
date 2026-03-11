using System.Linq;
using System.Text.Json;
using System.Globalization;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using AdvancedMarketData.Core.Models;
using System.Text.Json.Serialization;
using AdvancedMarketData.Core.Helpers;
using AdvancedMarketData.Core.Interfaces;

namespace AdvancedMarketData.Core.Services
{
    // Coinbase Advanced API WebSocket message models
    public class CoinbaseWebSocketMessage
    {
        [JsonPropertyName("channel")]
        public string Channel { get; set; } = string.Empty;
        
        [JsonPropertyName("client_id")]
        public string ClientId { get; set; } = string.Empty;
        
        [JsonPropertyName("timestamp")]
        public string Timestamp { get; set; } = string.Empty;
        
        [JsonPropertyName("sequence_num")]
        public long SequenceNum { get; set; }
        
        [JsonPropertyName("events")]
        public CoinbaseEvent[]? Events { get; set; }
    }

    public class CoinbaseEvent
    {
        [JsonPropertyName("type")]
        public string Type { get; set; } = string.Empty;
        
        [JsonPropertyName("candles")]
        public CoinbaseCandle[]? Candles { get; set; }
        
        [JsonPropertyName("current_time")]
        public string? CurrentTime { get; set; }
        
        [JsonPropertyName("heartbeat_counter")]
        public long? HeartbeatCounter { get; set; }
    }

    public class CoinbaseCandle
    {
        [JsonPropertyName("start")]
        public string Start { get; set; } = string.Empty;
        
        [JsonPropertyName("low")]
        public string Low { get; set; } = string.Empty;
        
        [JsonPropertyName("high")]
        public string High { get; set; } = string.Empty;
        
        [JsonPropertyName("open")]
        public string Open { get; set; } = string.Empty;
        
        [JsonPropertyName("close")]
        public string Close { get; set; } = string.Empty;
        
        [JsonPropertyName("volume")]
        public string Volume { get; set; } = string.Empty;
        
        [JsonPropertyName("product_id")]
        public string ProductId { get; set; } = string.Empty;
    }

    public class WebSocketStreamService : IWebSocketStreamService
    {
        public event Action<Candle>? OnCandleReceived;
        private readonly ILogger<WebSocketStreamService> _logger;
        private readonly IConfiguration _configuration;
        private readonly int _maxReconnectAttempts = 5;
        private readonly TimeSpan _reconnectDelay = TimeSpan.FromSeconds(5);
        // Thread-safe dictionaries for multi-connection tracking
        private readonly Dictionary<string, long> _lastHeartbeatCounters = new();
        private readonly Dictionary<string, DateTime> _lastHeartbeatTimes = new();
        private readonly Dictionary<string, int> _snapshotCandleCounts = new();
        private readonly object _heartbeatLock = new object();
        private readonly object _snapshotCountLock = new object();
        
        // Rate limiting for WebSocket messages (8 messages/second for unauthenticated)
        private readonly Queue<DateTime> _messageTimes = new();
        private readonly object _rateLimitLock = new object();
        private const int MaxMessagesPerSecond = 8;
        private const int AuthenticatedMaxMessagesPerSecond = 100; // Higher limit for authenticated connections

        private readonly Ed25519JwtHelper? _jwtHelper;

        public WebSocketStreamService(ILogger<WebSocketStreamService> logger, IConfiguration configuration, Func<Ed25519JwtHelper?> jwtHelperFactory)
        {
            _logger = logger;
            _configuration = configuration;
            _jwtHelper = jwtHelperFactory();
        }

        private static bool RequiresAuthentication(string channel)
        {
            return channel switch
            {
                "user" => true,
                "futures_balance_summary" => true,
                _ => false
            };
        }

        private string GetEndpointForChannels(string[] channels, bool hasCredentials)
        {
            // If any channel requires authentication and we have credentials, use auth endpoint
            bool needsAuthEndpoint = channels.Any(RequiresAuthentication) && hasCredentials;
            var authEndpoint = _configuration["CoinbaseApi:UserOrderDataEndpoint"] ?? "wss://advanced-trade-ws-user.coinbase.com";
            var publicEndpoint = _configuration["CoinbaseApi:MarketDataEndpoint"] ?? "wss://advanced-trade-ws.coinbase.com";
            return needsAuthEndpoint ? authEndpoint : publicEndpoint;
        }

        private object CreateSubscriptionPayload(string channel, string[]? productIds = null)
        {
            var payload = new Dictionary<string, object>
            {
                ["type"] = "subscribe",
                ["channel"] = channel
            };

            // Add product IDs if provided
            if (productIds != null && productIds.Length > 0)
            {
                payload["product_ids"] = productIds;
            }

            // Only add JWT if this specific channel requires authentication
            if (RequiresAuthentication(channel))
            {
                if (_jwtHelper == null)
                {
                    _logger.LogWarning("Channel {Channel} requires authentication but no JWT helper is available", channel);
                    throw new InvalidOperationException($"Channel '{channel}' requires authentication but JWT helper is not configured");
                }

                var apiKeyId = _configuration["CoinbaseApi:ApiKeyId"];
                var apiSecret = _configuration["CoinbaseApi:ApiSecret"];
                if (string.IsNullOrEmpty(apiKeyId) || string.IsNullOrEmpty(apiSecret))
                {
                    _logger.LogWarning("Channel {Channel} requires authentication but API credentials are not configured", channel);
                    throw new InvalidOperationException($"Channel '{channel}' requires authentication but API credentials are not configured");
                }

                try
                {
                    // Generate JWT for WebSocket subscription using GET request format
                    var jwt = _jwtHelper.GenerateJwt("GET /users/self/verify");
                    payload["jwt"] = jwt;
                    _logger.LogDebug("Added JWT authentication for channel {Channel}", channel);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to generate JWT for channel {Channel}", channel);
                    throw;
                }
            }

            return payload;
        }

        private async Task WaitForRateLimit(bool isAuthenticated, CancellationToken ct)
        {
            TimeSpan waitTime = TimeSpan.Zero;
            
            lock (_rateLimitLock)
            {
                var now = DateTime.UtcNow;
                var maxMessages = isAuthenticated ? AuthenticatedMaxMessagesPerSecond : MaxMessagesPerSecond;
                
                // Remove messages older than 1 second
                while (_messageTimes.Count > 0 && (now - _messageTimes.Peek()).TotalSeconds >= 1.0)
                {
                    _messageTimes.Dequeue();
                }
                
                // If we're at the limit, we need to wait
                if (_messageTimes.Count >= maxMessages)
                {
                    var oldestMessage = _messageTimes.Peek();
                    waitTime = TimeSpan.FromSeconds(1.0) - (now - oldestMessage);
                    
                    if (waitTime > TimeSpan.Zero)
                    {
                        _logger.LogDebug("Rate limit reached, waiting {WaitTime}ms before sending message", waitTime.TotalMilliseconds);
                    }
                }
                
                // Record this message
                _messageTimes.Enqueue(DateTime.UtcNow);
            }
            
            // Wait outside the lock
            if (waitTime > TimeSpan.Zero)
            {
                await Task.Delay(waitTime, ct);
            }
        }

        private async Task SendWebSocketMessage(WebSocket ws, object payload, string messageType, bool isAuthenticated, CancellationToken ct)
        {
            // Apply rate limiting
            await WaitForRateLimit(isAuthenticated, ct);
            
            var json = JsonSerializer.Serialize(payload);
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);
            await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);

            _logger.LogDebug("Sent {MessageType} message: {Json}", messageType, json);
        }

        public async Task StartStreamingAsync(string[] channels, string[] productIds, CancellationToken ct, bool includeSnapshotCandles = true)
        {
            int reconnectAttempts = 0;
            
            while (!ct.IsCancellationRequested && reconnectAttempts < _maxReconnectAttempts)
            {
                try
                {
                    var channelList = string.Join(", ", channels);
                    var productList = string.Join(", ", productIds);
                    
                    _logger.LogInformation("Connecting to WebSocket for channels [{Channels}] with products [{Products}] (attempt {Attempt}/{MaxAttempts})", 
                        channelList, productList, reconnectAttempts + 1, _maxReconnectAttempts);
                    
                    await ConnectAndStreamAsync(channels, productIds, ct, includeSnapshotCandles);
                    
                    // If we get here, the connection was successful and completed normally
                    _logger.LogInformation("WebSocket stream completed normally for channels [{Channels}]", channelList);
                    break;
                }
                catch (WebSocketException ex)
                {
                    reconnectAttempts++;
                    var channelList = string.Join(", ", channels);
                    _logger.LogWarning(ex, "WebSocket error for channels [{Channels}] (attempt {Attempt}/{MaxAttempts})", 
                        channelList, reconnectAttempts, _maxReconnectAttempts);
                    
                    if (reconnectAttempts >= _maxReconnectAttempts)
                    {
                        _logger.LogError("Max reconnection attempts reached for channels [{Channels}]", channelList);
                        throw;
                    }
                    
                    // Wait before reconnecting, but only if not cancelled
                    if (!ct.IsCancellationRequested)
                    {
                        _logger.LogInformation("Waiting {Delay} seconds before reconnecting...", _reconnectDelay.TotalSeconds);
                        await Task.Delay(_reconnectDelay, ct);
                    }
                }
                catch (OperationCanceledException)
                {
                    var cancelledProducts = string.Join(", ", productIds);
                    _logger.LogInformation("Streaming cancelled for products [{Products}]", cancelledProducts);
                    break;
                }
                catch (Exception ex)
                {
                    var errorProducts = string.Join(", ", productIds);
                    _logger.LogError(ex, "Unexpected error streaming products [{Products}]", errorProducts);
                    throw;
                }
            }
        }

        private async Task ConnectAndStreamAsync(string[] channels, string[] productIds, CancellationToken ct, bool includeSnapshotCandles)
        {
            using var ws = new ClientWebSocket();
            
            // Generate unique connection ID for this WebSocket connection
            var connectionId = $"{string.Join("-", productIds)}_{Guid.NewGuid().ToString("N")[..8]}";
            
            // Determine available credentials
            var apiKeyId = _configuration["CoinbaseApi:ApiKeyId"];
            var apiSecret = _configuration["CoinbaseApi:ApiSecret"];
            bool hasCredentials = _jwtHelper != null && 
                                !string.IsNullOrEmpty(apiKeyId) && 
                                !string.IsNullOrEmpty(apiSecret);
            
            // Always include heartbeats to keep connection alive
            var allChannels = channels.Contains("heartbeats") ? channels : channels.Concat(new[] { "heartbeats" }).ToArray();
            
            // Choose endpoint based on channel requirements and available credentials
            var endpoint = GetEndpointForChannels(allChannels, hasCredentials);
            var authEndpoint = _configuration["CoinbaseApi:UserOrderDataEndpoint"] ?? "wss://advanced-trade-ws-user.coinbase.com";
            bool useAuth = endpoint == authEndpoint;
            
            await ws.ConnectAsync(new Uri(endpoint), ct);
            var productList = string.Join(", ", productIds);
            _logger.LogInformation("Connected to WebSocket at {Endpoint} for products [{Products}] (Auth: {RequiresAuth}) [ID: {ConnectionId}]", 
                endpoint, productList, useAuth, connectionId);

            // Subscribe to all requested channels
            foreach (var channel in allChannels)
            {
                if (channel == "heartbeats")
                {
                    // Heartbeats don't need product IDs
                    var heartbeatsPayload = CreateSubscriptionPayload("heartbeats");
                    await SendWebSocketMessage(ws, heartbeatsPayload, "heartbeats subscription", useAuth, ct);
                    _logger.LogInformation("✓ Heartbeats subscription sent (connection keep-alive)");
                }
                else if (channel == "futures_balance_summary")
                {
                    // Futures balance summary doesn't need product IDs
                    var futuresPayload = CreateSubscriptionPayload("futures_balance_summary");
                    await SendWebSocketMessage(ws, futuresPayload, "futures_balance_summary subscription", useAuth, ct);
                    _logger.LogInformation("✓ Futures balance summary subscription sent");
                }
                else
                {
                    // Other channels need product IDs
                    var channelPayload = CreateSubscriptionPayload(channel, productIds);
                    await SendWebSocketMessage(ws, channelPayload, $"{channel} subscription", useAuth, ct);
                    _logger.LogInformation("✓ {Channel} subscription sent for [{Products}]", channel, productList);
                }
            }

            // Listen for messages with proper chunking support
            var buffer = new byte[4096]; // Smaller chunk size
            var messageBuffer = new List<byte>(); // Buffer to accumulate message chunks
            
            while (!ct.IsCancellationRequested && ws.State == WebSocketState.Open)
            {
                try
                {
                    var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                if (result.MessageType == WebSocketMessageType.Text)
                {
                        // Add received bytes to message buffer
                        messageBuffer.AddRange(buffer.Take(result.Count));
                        
                        // Check if this is the end of the message
                        if (result.EndOfMessage)
                        {
                            // Complete message received - convert to string and process
                            var json = System.Text.Encoding.UTF8.GetString(messageBuffer.ToArray());
                            
                            // Log message size for debugging
                            _logger.LogDebug("Received complete message: {Size} bytes", messageBuffer.Count);
                            
                            // Handle different message types
                            if (json.Contains("\"type\":\"subscriptions\""))
                            {
                                _logger.LogInformation("Subscription confirmed");
                            }
                            else if (json.Contains("\"type\":\"error\""))
                            {
                                _logger.LogError("WebSocket error: {Json}", json);
                            }
                            else
                            {
                                // Process the complete message based on channel type (no verbose logging)
                                ProcessWebSocketMessage(json, connectionId, includeSnapshotCandles);
                            }
                            
                            // Clear buffer for next message
                            messageBuffer.Clear();
                        }
                        else
                        {
                            // Message is not complete - continue receiving chunks
                            _logger.LogDebug("Received message chunk: {ChunkSize} bytes (partial)", result.Count);
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("WebSocket close message received");
                        break;
                    }
                }
                catch (WebSocketException ex)
                {
                    _logger.LogWarning(ex, "WebSocket error during streaming");
                    throw; // Re-throw to trigger reconnection
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Streaming operation cancelled");
                    break;
                }
            }
            
            var closedProducts = string.Join(", ", productIds);
            _logger.LogInformation("WebSocket connection closed for products [{Products}]", closedProducts);
        }

        private void ProcessWebSocketMessage(string json, string connectionId, bool includeSnapshotCandles)
        {
            try
            {
                var message = JsonSerializer.Deserialize<CoinbaseWebSocketMessage>(json);
                if (message?.Events == null || message.Events.Length == 0)
                {
                    _logger.LogDebug("Received message with no events: {Json}", json);
                    return;
                }

                // Handle based on channel type
                if (message.Channel == "heartbeats")
                {
                    ProcessHeartbeat(message, connectionId);
                }
                else if (message.Channel == "candles")
                {
                    // Extract productId from the candle events in the message
                    ProcessCandleMessage(message, ExtractProductIdFromMessage(message), includeSnapshotCandles);
                }
                else
                {
                    _logger.LogDebug("Received message for unknown channel: {Channel}", message.Channel);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing WebSocket message: {Json}", json);
            }
        }

        private string ExtractProductIdFromMessage(CoinbaseWebSocketMessage message)
        {
            // Try to extract productId from the first event's candle data
            try
            {
                if (message.Events?.FirstOrDefault() is CoinbaseEvent eventData &&
                    eventData.Candles?.Length > 0)
                {
                    return eventData.Candles[0].ProductId ?? "UNKNOWN";
                }
            }
            catch
            {
                // Fallback if extraction fails
            }
            return "UNKNOWN";
        }

        private void ProcessCandleMessage(CoinbaseWebSocketMessage message, string productId, bool includeSnapshotCandles)
        {
            try
            {
                if (message.Events == null || message.Events.Length == 0)
                {
                    _logger.LogDebug("Received candle message with no events");
                    return;
                }

                foreach (var eventData in message.Events)
                {
                    var eventType = eventData.Type?.ToLowerInvariant();
                    
                    // Handle different event types
                    switch (eventType)
                    {
                        case "snapshot":
                            if (includeSnapshotCandles)
                            {
                                _logger.LogInformation("Received candle snapshot for {ProductId} with {CandleCount} candles", 
                                    productId, eventData.Candles?.Length ?? 0);
                                ProcessCandleEvent(eventData, productId, isSnapshot: true);
                            }
                            else
                            {
                                _logger.LogDebug("Skipping snapshot candles for {ProductId} (live-only behavior)", productId);
                            }
                            break;
                            
                        case "update":
                            ProcessCandleEvent(eventData, productId, isSnapshot: false);
                            break;
                            
                        default:
                            _logger.LogWarning("Unknown candle event type: {EventType} for {ProductId}", eventType, productId);
                            // Still try to process it as a regular update
                            ProcessCandleEvent(eventData, productId, isSnapshot: false);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing candle message for {ProductId}", productId);
            }
        }

        private void ProcessCandleEvent(CoinbaseEvent eventData, string productId, bool isSnapshot)
        {
            try
            {
                if (eventData.Candles == null || eventData.Candles.Length == 0)
                {
                    _logger.LogDebug("Event has no candles. Event type: {EventType}, Snapshot: {IsSnapshot}", 
                        eventData.Type, isSnapshot);
                    return;
                }

                // Process each candle in the event
                foreach (var coinbaseCandle in eventData.Candles)
                {
                    var candle = ParseCandleData(coinbaseCandle, productId);
                    if (candle != null)
                    {
                        OnCandleReceived?.Invoke(candle);
                        
                        if (isSnapshot)
                        {
                            // Track snapshots per product (thread-safe)
                            lock (_snapshotCandleCounts)
                            {
                                if (!_snapshotCandleCounts.ContainsKey(productId))
                                    _snapshotCandleCounts[productId] = 0;
                                
                                var currentCandleNumber = ++_snapshotCandleCounts[productId];
                                
                                if (currentCandleNumber % 10 == 1 || currentCandleNumber % 10 == 0)
                                {
                                    _logger.LogInformation("Snapshot {Current}/100: {ProductId} ${Close} Vol:{Volume:F1}", 
                                        currentCandleNumber, productId, FormatPrice(candle.Close), candle.Volume);
                                }
                            }
                        }
                        else
                        {
                            // Always log real-time updates - these are the important ones!
                            _logger.LogInformation("LIVE: {ProductId} ${Close} Vol:{Volume:F1} BTC [{Time:HH:mm:ss}]", 
                                productId, FormatPrice(candle.Close), candle.Volume, candle.Time);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing candle event for {ProductId}, Snapshot: {IsSnapshot}", productId, isSnapshot);
            }
        }

        private void ProcessHeartbeat(CoinbaseWebSocketMessage message, string connectionId)
        {
            try
            {
                var heartbeatEvent = message.Events?[0];
                if (heartbeatEvent?.HeartbeatCounter != null)
                {
                    var counter = heartbeatEvent.HeartbeatCounter.Value;
                    
                    // Thread-safe heartbeat tracking per connection
                    lock (_heartbeatLock)
                    {
                        // Check for missed heartbeats per connection
                        if (_lastHeartbeatCounters.ContainsKey(connectionId) && counter != _lastHeartbeatCounters[connectionId] + 1)
                        {
                            var missedBeats = counter - _lastHeartbeatCounters[connectionId] - 1;
                            _logger.LogWarning("Missed {MissedBeats} heartbeat(s) for connection {ConnectionId}. Last: {Last}, Current: {Current}", 
                                missedBeats, connectionId, _lastHeartbeatCounters[connectionId], counter);
                        }

                        _lastHeartbeatCounters[connectionId] = counter;
                        _lastHeartbeatTimes[connectionId] = DateTime.UtcNow;
                    }
                    
                    _logger.LogInformation("Heartbeat #{Counter} [{ConnectionId}]", counter, connectionId);
                }
                else
                {
                    _logger.LogWarning("Heartbeat event missing heartbeat_counter");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing heartbeat message");
            }
        }

        private Candle? ParseCandleData(CoinbaseCandle coinbaseCandle, string productId)
        {
            try
            {
                
                // Parse the timestamp (Coinbase sends UNIX timestamp as string)
                if (!long.TryParse(coinbaseCandle.Start, out var unixTimestamp))
                {
                    _logger.LogWarning("Failed to parse timestamp: {Start}", coinbaseCandle.Start);
                    return null;
                }
                
                var timestamp = DateTimeOffset.FromUnixTimeSeconds(unixTimestamp).DateTime;

                // Parse decimal values with culture-invariant parsing
                if (!decimal.TryParse(coinbaseCandle.Open, NumberStyles.Float, CultureInfo.InvariantCulture, out var open) ||
                    !decimal.TryParse(coinbaseCandle.High, NumberStyles.Float, CultureInfo.InvariantCulture, out var high) ||
                    !decimal.TryParse(coinbaseCandle.Low, NumberStyles.Float, CultureInfo.InvariantCulture, out var low) ||
                    !decimal.TryParse(coinbaseCandle.Close, NumberStyles.Float, CultureInfo.InvariantCulture, out var close) ||
                    !decimal.TryParse(coinbaseCandle.Volume, NumberStyles.Float, CultureInfo.InvariantCulture, out var volume))
                {
                    _logger.LogWarning("Failed to parse decimal values for candle: O:{Open} H:{High} L:{Low} C:{Close} V:{Volume}", 
                        coinbaseCandle.Open, coinbaseCandle.High, coinbaseCandle.Low, coinbaseCandle.Close, coinbaseCandle.Volume);
                    return null;
                }

                _logger.LogDebug("Successfully parsed candle for {ProductId}: {Time} O:{Open} H:{High} L:{Low} C:{Close} V:{Volume}", 
                    coinbaseCandle.ProductId, timestamp, open, high, low, close, volume);

                return new Candle(productId, timestamp, open, high, low, close, volume);
            }
            catch (Exception ex)
            {
                // Log unexpected errors
                _logger.LogError(ex, "Unexpected error parsing candle data for {ProductId}: {Error}", productId, ex.Message);
            return null;
            }
        }

        private string FormatPrice(decimal price)
        {
            // For very small values (like SHIB-USD), show more decimal places
            if (price < 0.01m)
            {
                return price.ToString("F8").TrimEnd('0').TrimEnd('.');
            }
            // For normal values, show 2 decimal places
            else if (price < 1000m)
            {
                return price.ToString("F2");
            }
            // For large values (like BTC), show no decimal places if it's a whole number
            else
            {
                return price % 1 == 0 ? price.ToString("F0") : price.ToString("F2");
            }
        }
    }
}