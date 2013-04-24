using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging.Redis
{
    public class RedisMessageHandlerWorker : MessageHandlerBackgroundWorker
    {        
        private readonly IRedisClientsManager clientsManager;

        public RedisMessageHandlerWorker(
            IRedisClientsManager clientsManager, IMessageHandler messageHandler, string queueName,
            Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
            : base(messageHandler, queueName, errorHandler)
        {
            this.clientsManager = clientsManager;
        }

        public override IMessageHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new RedisMessageHandlerWorker(clientsManager, messageHandler, QueueName, this.ErrorHandler);
        }

        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager);
        }
    }
}