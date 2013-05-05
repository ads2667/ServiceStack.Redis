using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging.ServiceStack.Redis.Messaging;

namespace ServiceStack.Redis.Messaging
{    
    public interface IMessageHandlerBackgroundWorker : IBackgroundWorker
    {
        string QueueName { get; }

        IMessageHandlerStats GetStats();

        void NotifyNewMessage();
    }
}