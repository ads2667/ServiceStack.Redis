using System;
using System.Security.Cryptography;
using System.Text;

namespace ServiceStack.Aws.Messaging
{
    // TODO: Security
    // TODO: Performance Tests
    // TODO: Unit Tests

    public static class MessageSecurity
    {
        private static string ToHex(this byte[] hash)
        {
            // convert byte array to hex string
            var sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("x2"));
            }
            return sb.ToString();
        }

        public static bool IsMd5Valid(string messageBody, string expectedMd5)
        {
            if (messageBody == null)
            {
                throw new ArgumentNullException("messageBody");
            }

            // Define the hash algorithm to use
            byte[] hashBytes;
            using (var hash = new MD5Cng())
            {
                hashBytes = hash.ComputeHash(Encoding.UTF8.GetBytes(messageBody));
                hash.Clear();
            }

            var messageBodyMd5 = hashBytes.ToHex();
            if (messageBodyMd5 == expectedMd5)
            {
                return true;
            }

            return false;
        }
    }
}
