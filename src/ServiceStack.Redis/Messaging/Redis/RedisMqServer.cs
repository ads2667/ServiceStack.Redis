using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging.Redis
{
    /// <summary>
    /// Creates a Redis MQ Server that processes each message on its own background thread.
    /// i.e. if you register 3 handlers it will create 7 background threads:
    ///   - 1 listening to the Redis MQ Subscription, getting notified of each new message
    ///   - 3x1 Normal InQ for each message handler
    ///   - 3x1 PriorityQ for each message handler
    /// 
    /// When RedisMqServer Starts it creates a background thread subscribed to the Redis MQ Topic that
    /// listens for new incoming messages. It also starts 2 background threads for each message type:
    ///  - 1 for processing the services Priority Queue and 1 processing the services normal Inbox Queue.
    /// 
    /// Priority Queue's can be enabled on a message-per-message basis by specifying types in the 
    /// OnlyEnablePriortyQueuesForTypes property. The DisableAllPriorityQueues property disables all Queues.
    /// 
    /// The Start/Stop methods are idempotent i.e. It's safe to call them repeatedly on multiple threads 
    /// and the Redis MQ Server will only have Started or Stopped once.
    /// </summary>
    public class RedisMqServer : MqServer2<DefaultHandlerConfiguration, DefaultMessageHandlerRegister, RedisBackgroundWorkerFactory> //<MessageHandlerRegister>
    {       
        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public override IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager, null);
        }

        public RedisMqServer(IRedisClientsManager clientsManager,
            int retryCount = DefaultRetryCount)
            : base(new RedisMessageFactory(clientsManager), retryCount)
        {
            this.clientsManager = clientsManager;            
        }

        protected override RedisBackgroundWorkerFactory CreateBackgroundWorkerFactory()
        {
            return new RedisBackgroundWorkerFactory(this.clientsManager, this);
        }

        protected override DefaultMessageHandlerRegister CreateMessageHandlerRegister()
        {
            return new DefaultMessageHandlerRegister(this, this.RetryCount);
        }
    }
}