using System;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging.ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{    
    public interface IQueueHandlerBackgroundWorker : IBackgroundWorker
    {
        string QueueName { get; }

        IQueueHandlerStats GetStats();

        // void NotifyNewMessage();
    }

    public abstract class QueueHandlerBackgroundWorker : BackgroundWorker<IQueueHandlerBackgroundWorker>, IQueueHandlerBackgroundWorker 
    {                
        private DateTime lastMsgProcessed;        
        private int totalMessagesReceived;        
        
        protected QueueHandlerBackgroundWorker(string queueName, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
            : base(errorHandler)
        {
            this.QueueName = queueName;            
        }

        public string QueueName { get; set; }
        
        // TODO: Need Queue Stats
        
        protected void IncrementMessageCount(int increment)
        {
            totalMessagesReceived += increment;
            lastMsgProcessed = DateTime.UtcNow;
        }

        public IQueueHandlerStats GetStats() 
        {
            return new QueueHandlerStats(this.QueueName, totalMessagesReceived);
        }
        
        public override string GetStatus()
        {
            return "[Worker: {0}, Status: {1}, ThreadStatus: {2}, LastMsgAt: {3}, TotalMessagesReceived: {4}]"
                .Fmt(QueueName, WorkerStatus.ToString(status), this.BgThreadState, lastMsgProcessed, totalMessagesReceived);
        }

        protected override string ThreadName
        {
            get { return "{0}: {1}".Fmt(GetType().Name, QueueName); }
        }

        protected override sealed void InvokeErrorHandler(Exception ex)
        {
            this.ErrorHandler.Invoke(this, ex);
        }
    }
}