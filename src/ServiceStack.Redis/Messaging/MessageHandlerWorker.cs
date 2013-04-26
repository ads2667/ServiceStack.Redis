using System;
using System.Threading;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging.ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{    
    public interface IMessageHandlerBackgroundWorker : IBackgroundWorker
    {
        string QueueName { get; }

        IMessageHandlerStats GetStats();

        void NotifyNewMessage();
    }

    public abstract class MessageHandlerBackgroundWorker : BackgroundWorker<IMessageHandlerBackgroundWorker>, IMessageHandlerBackgroundWorker 
    {
        protected MessageHandlerBackgroundWorker(Action<IMessageHandlerBackgroundWorker, Exception> errorHandler) 
            : base(errorHandler)
        {
        }

        protected readonly IMessageHandler messageHandler;
        public string QueueName { get; set; }
        private DateTime lastMsgProcessed;        
        private int totalMessagesProcessed;        
        private int msgNotificationsReceived;
        
        protected MessageHandlerBackgroundWorker(IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
            : base(errorHandler)
        {
            this.messageHandler = messageHandler;
            this.QueueName = queueName;            
        }

        public virtual void NotifyNewMessage()
        {
            Interlocked.Increment(ref msgNotificationsReceived);
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
            {
                if (Monitor.TryEnter(msgLock))
                {
                    Monitor.Pulse(msgLock);
                    Monitor.Exit(msgLock);
                }
                else
                {
                    receivedNewMsgs = true;
                }
            }
        }

        public IMessageHandlerStats GetStats()
        {
            return messageHandler.GetStats();
        }

        protected abstract IMessageQueueClient CreateMessageQueueClient();

        public override string GetStatus()
        {
            return "[Worker: {0}, Status: {1}, ThreadStatus: {2}, LastMsgAt: {3}, TotalMessagesProcessed: {4}]"
                .Fmt(QueueName, WorkerStatus.ToString(status), this.BgThreadState, lastMsgProcessed, totalMessagesProcessed);
        }

        protected override string ThreadName
        {
            get { return "{0}: {1}".Fmt(GetType().Name, QueueName); }
        }

        readonly object msgLock = new object();
        private bool receivedNewMsgs = false;

        protected override void OnStop()
        {
            lock (msgLock)
            {
                Monitor.Pulse(msgLock);
            }
        }

        /// <summary>
        /// Performs exceution on a background thread within a locked context.
        /// </summary>
        protected override void Execute()
        {
            lock (msgLock)
            {
                while (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
                {
                    receivedNewMsgs = false;
                    using (var mqClient = this.CreateMessageQueueClient())
                    {
                        var msgsProcessedThisTime = messageHandler.ProcessQueue(mqClient, QueueName,
                            () => Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started);

                        totalMessagesProcessed += msgsProcessedThisTime;

                        if (msgsProcessedThisTime > 0)
                            lastMsgProcessed = DateTime.UtcNow;
                    }

                    if (!receivedNewMsgs)
                        Monitor.Wait(msgLock);
                }
            }
        }

        protected override sealed void InvokeErrorHandler(Exception ex)
        {
            this.ErrorHandler.Invoke(this, ex);
        }
    }
}