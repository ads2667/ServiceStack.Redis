using System;

namespace ServiceStack.Redis.Messaging
{
    public class MessageReceivedArgs
    {
        public Type MessageType { get; set; }

        public string QueueName { get; set; }

        public string MessageId { get; set; }
    }
}
