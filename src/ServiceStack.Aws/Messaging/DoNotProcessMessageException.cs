using System;

namespace ServiceStack.Aws.Messaging
{
    /// <summary>
    /// An exception that denotes that a MQ Message should not be processed.
    /// </summary>
    internal class DoNotProcessMessageException : Exception
    {
    }
}
