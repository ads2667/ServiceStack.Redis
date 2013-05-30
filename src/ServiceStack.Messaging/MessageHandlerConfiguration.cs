using System;

namespace ServiceStack.Messaging
{
    public class MessageHandlerConfiguration
    {
        /// <summary>
        /// Gets or sets the number of times a message handler will attempt to process a message.
        /// </summary>
        public int? RetryCount { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic before a request is processed.
        /// Must be thread-safe.
        /// </summary>
        public Func<IMessage, IMessage> RequestFilter { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic on the response.
        /// Must be thread-safe.
        /// </summary>
        public Func<object, object> ResponseFilter { get; set; }
    }
}
