using System.Text;
using System.Text.Json;
using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Crypto.Parameters;

namespace AdvancedMarketData.Core.Helpers
{
    // Generates a JWT token signed with Ed25519 for Coinbase Advanced API
    public class Ed25519JwtHelper
    {
        private readonly string _apiKeyId;
        private readonly string _apiSecret;
        private readonly JsonSerializerOptions _jsonOptions;

        public Ed25519JwtHelper(string apiKeyId, string apiSecret)
        {
            _apiKeyId = apiKeyId ?? throw new ArgumentNullException(nameof(apiKeyId));
            _apiSecret = apiSecret ?? throw new ArgumentNullException(nameof(apiSecret));

            // Configure JSON serialization to match Coinbase's expected format
            _jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy = null,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
        }

        // Returns the JWT token as a string
        public string GenerateJwt(string uri)
        {
            try
            {
                // Extract method and path
                string[] uriParts = uri.Split(' ');
                if (uriParts.Length != 2)
                {
                    throw new ArgumentException("URI must be in the format 'METHOD /path'");
                }

                string method = uriParts[0];
                string path = uriParts[1];

                // Remove query parameters from the path for JWT generation
                string basePath = path.Split('?')[0];

                string fullUri = $"{method} api.coinbase.com{basePath}";

                // Create JWT header with correct field order
                var header = new Dictionary<string, object>
                {
                    { "typ", "JWT" },
                    { "alg", "EdDSA" },
                    { "kid", _apiKeyId },
                    { "nonce", GenerateNonce() }
                };

                // Format header as JSON and encode
                string headerJson = System.Text.Json.JsonSerializer.Serialize(header, _jsonOptions);
                string encodedHeader = Base64UrlEncode(headerJson);

                // Get current time for JWT validity
                var now = DateTimeOffset.UtcNow;
                long nbf = now.ToUnixTimeSeconds();
                long exp = now.AddMinutes(2).ToUnixTimeSeconds(); // 2 minutes expiration as per Coinbase docs

                // Create JWT payload with correct field order
                var payload = new Dictionary<string, object>
                {
                    { "iss", "cdp" },     // 1st
                    { "nbf", nbf },       // 2nd
                    { "exp", exp },       // 3rd
                    { "sub", _apiKeyId }, // 4th
                    { "uri", fullUri }    // 5th
                };

                // Format payload as JSON and encode
                string payloadJson = System.Text.Json.JsonSerializer.Serialize(payload, _jsonOptions);
                string encodedPayload = Base64UrlEncode(payloadJson);

                // Create signature data (header.payload)
                string dataToSign = $"{encodedHeader}.{encodedPayload}";

                // Sign with Ed25519 
                byte[] signature = SignWithEd25519(Encoding.UTF8.GetBytes(dataToSign), _apiSecret);
                string encodedSignature = Base64UrlEncode(signature);

                // Assemble final JWT (header.payload.signature)
                string jwt = $"{encodedHeader}.{encodedPayload}.{encodedSignature}";

                return jwt;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error generating JWT: {ex.Message}", ex);
            }
        }


        // Generates a cryptographically secure random nonce for JWT

        private string GenerateNonce(int length = 32)
        {
            // Per Coinbase's example, they use a 16-byte random value converted to hex (32 chars)
            byte[] nonceBytes = new byte[length / 2];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(nonceBytes);
            }
            return BitConverter.ToString(nonceBytes).Replace("-", "").ToLower();
        }


        // Base64Url encodes data for JWT

        private string Base64UrlEncode(string input)
        {
            return Base64UrlEncode(Encoding.UTF8.GetBytes(input));
        }


        // Base64Url encodes bytes for JWT

        private string Base64UrlEncode(byte[] input)
        {
            string base64 = Convert.ToBase64String(input);
            return base64
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');
        }


        // Signs data using BouncyCastle's Ed25519 implementation

        private byte[] SignWithEd25519(byte[] data, string secret)
        {
            try
            {
                // Decode the base64 secret to get the Ed25519 private key bytes
                byte[] secretBytes = Convert.FromBase64String(secret);

                // Create a BouncyCastle Ed25519 signer
                var privateKey = new Ed25519PrivateKeyParameters(secretBytes, 0);
                var signer = new Ed25519Signer();
                signer.Init(true, privateKey);
                signer.BlockUpdate(data, 0, data.Length);

                return signer.GenerateSignature();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Unable to sign data using Ed25519", ex);
            }
        }
    }
}