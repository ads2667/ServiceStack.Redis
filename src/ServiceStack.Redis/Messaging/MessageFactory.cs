////using ServiceStack.Messaging;

////namespace ServiceStack.Redis.Messaging
////{
////    public class MessageFactory : IMessageFactory
////    {
////        private readonly IRedisClientsManager clientsManager;

////        public MessageFactory(IRedisClientsManager clientsManager)
////        {
////            this.clientsManager = clientsManager;
////        }

////        public IMessageQueueClient CreateMessageQueueClient()
////        {
////            return new RedisMessageQueueClient(clientsManager);
////        }

////        public IMessageProducer CreateMessageProducer()
////        {
////            return new RedisMessageProducer(clientsManager);
////        }

////        public void Dispose()
////        {
////        }
////    }
////}