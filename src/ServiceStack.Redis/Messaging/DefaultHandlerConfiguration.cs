using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
    public class DefaultHandlerConfiguration
    {
        public DefaultHandlerConfiguration(IMessageHandlerFactory messageHandlerFactory, int noOfThreads)
        {
            if (messageHandlerFactory == null)
            {
                throw new ArgumentNullException("messageHandlerFactory");
            }

            this.MessageHandlerFactory = messageHandlerFactory;
            this.NoOfThreads = noOfThreads;
        }

        public IMessageHandlerFactory MessageHandlerFactory { get; set; }

        public int NoOfThreads { get; set; }
    }
}
