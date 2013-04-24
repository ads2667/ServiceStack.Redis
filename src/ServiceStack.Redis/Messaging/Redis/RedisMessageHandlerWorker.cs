using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging.Redis
{
    public class RedisMessageHandlerWorker : MessageHandlerBackgroundWorker<RedisMessageHandlerWorker>
    {        
        private readonly IRedisClientsManager clientsManager;

        public RedisMessageHandlerWorker(
            IRedisClientsManager clientsManager, IMessageHandler messageHandler, string queueName,
            Action<RedisMessageHandlerWorker, Exception> errorHandler)
            : base(messageHandler, queueName, errorHandler)
        {
            this.clientsManager = clientsManager;
        }

        public override RedisMessageHandlerWorker CloneBackgroundWorker()
        {
            return new RedisMessageHandlerWorker(clientsManager, messageHandler, QueueName, this.ErrorHandler);
        }

        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager);
        }

        protected override void InvokeErrorHandler(Exception ex)
        {
            this.ErrorHandler.Invoke(this, ex);
        }
    }
}