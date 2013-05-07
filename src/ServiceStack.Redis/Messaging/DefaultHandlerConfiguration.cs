using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
    public class HandlerRegistration<THandlerConfiguration>
    {
        public HandlerRegistration(IMessageHandlerFactory messageHandlerFactory, THandlerConfiguration configuration)
        {
            if (messageHandlerFactory == null)
            {
                throw new ArgumentNullException("messageHandlerFactory");
            }

            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }

            this.MessageHandlerFactory = messageHandlerFactory;
            this.Configuration = configuration;
        }

        public IMessageHandlerFactory MessageHandlerFactory { get; internal set; }

        public THandlerConfiguration Configuration { get; internal set; }
    }

    public class DefaultHandlerConfiguration
    {
        public DefaultHandlerConfiguration()
        {
            /*
            if (messageHandlerFactory == null)
            {
                throw new ArgumentNullException("messageHandlerFactory");
            }
            */

            // this.MessageHandlerFactory = messageHandlerFactory;
            // this.NoOfThreads = noOfThreads;
        }

        // internal IMessageHandlerFactory MessageHandlerFactory { get; set; }

        public int NoOfThreads { get; internal set; }
    }
}
