namespace ServiceStack.Messaging
{    
    public interface IMessageHandlerBackgroundWorker : IBackgroundWorker
    {
        string QueueName { get; }

        IMessageHandlerStats GetStats();

        void NotifyNewMessage();
    }
}