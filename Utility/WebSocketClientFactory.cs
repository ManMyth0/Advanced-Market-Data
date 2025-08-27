using System.Net.WebSockets;

namespace AdvancedMarketData.Core.Utilities
{
    public static class WebSocketClientFactory
    {
        public static ClientWebSocket CreateDefault()
        {
            var ws = new ClientWebSocket();
            // TODO: Add default headers, auth if needed
            return ws;
        }
    }
}