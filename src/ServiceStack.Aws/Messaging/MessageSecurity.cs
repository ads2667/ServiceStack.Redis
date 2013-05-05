using System;
using System.Security.Cryptography;

namespace ServiceStack.Aws.Messaging
{
    public static class MessageSecurity
    {
        public static void VerifyMessageMd5(string messageBody, string expectedMd5)
        {
            return;
            if (messageBody == null)
            {
                throw new ArgumentNullException("messageBody");
            }

            // Define the hash algorithm to use
            byte[] hashBytes;
            using (var hash = new MD5Cng())
            {
                // Compute hash value of our plain text with appended salt.
                hashBytes = hash.ComputeHash(Convert.FromBase64String(messageBody));
                hash.Clear();
            }
            
            var messageMd5 = Convert.ToBase64String(hashBytes);
            if (messageMd5 != expectedMd5)
            {
                throw new InvalidOperationException("MD5 is Wrong!");
            }

            /*
            // TODO: Compate Md5 values
            // If the computed hash matches the specified hash,
            // the plain text value must be correct.
            if (expectedMd5.Length != hashBytes.Length)
            {
                throw new InvalidOperationException("Invalid MD5");
            }

            for (int i = 0; i < expectedMd5.Length; i++)
            {
                if (expectedMd5[i] != hashBytes[i])
                {
                    throw new InvalidOperationException("Invalid MD5");
                }
            }
            */
            throw new NotImplementedException("MD5 Verification not implemented");
        }
    }
}
