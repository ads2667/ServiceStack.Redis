using System;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging.Redis;

namespace ServiceStack.Redis.Messaging.Redis
{
    public class RedisMessageHandlerWorker : MessageHandlerWorker
    {        
        private readonly IRedisClientsManager clientsManager;

        public RedisMessageHandlerWorker(
            IRedisClientsManager clientsManager, IMessageHandler messageHandler, string queueName,
            Action<MessageHandlerWorker, Exception> errorHandler)
            : base(messageHandler, queueName, errorHandler)
        {
            this.clientsManager = clientsManager;
        }

        public override MessageHandlerWorker Clone()
        {
            return new RedisMessageHandlerWorker(clientsManager, messageHandler, QueueName, errorHandler);
        }

        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager);
        }
    }
}