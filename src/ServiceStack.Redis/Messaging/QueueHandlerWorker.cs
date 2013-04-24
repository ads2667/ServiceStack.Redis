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

        // TODO: Create MQ Stats?
        // IMessageHandlerStats GetStats();

        // void NotifyNewMessage();
    }

    public abstract class QueueHandlerBackgroundWorker : BackgroundWorker<IQueueHandlerBackgroundWorker>, IQueueHandlerBackgroundWorker 
    {
        /*
        protected QueueHandlerBackgroundWorker(Action<IQueueHandlerBackgroundWorker, Exception> errorHandler) 
            : base(errorHandler)
        {
        }
        */

        public string QueueName { get; set; }
        
        private DateTime lastMsgProcessed;
        public DateTime LastMsgProcessed
        {
            get { return lastMsgProcessed; }
        }

        private int totalMessagesProcessed;
        public int TotalMessagesProcessed
        {
            get { return totalMessagesProcessed; }
        }

        private int msgNotificationsReceived;
        public int MsgNotificationsReceived
        {
            get { return msgNotificationsReceived; }
        }

        protected QueueHandlerBackgroundWorker(string queueName, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
            : base(errorHandler)
        {
            this.QueueName = queueName;            
        }

        /*
        public IMessageHandlerStats GetStats()
        {
            return messageHandler.GetStats();
        }
        */

        protected abstract IMessageQueueClient CreateMessageQueueClient();

        public override string GetStatus()
        {
            return "[Worker: {0}, Status: {1}, ThreadStatus: {2}, LastMsgAt: {3}]"
                .Fmt(QueueName, WorkerStatus.ToString(status), this.BgThreadState, LastMsgProcessed);
        }

        protected override string ThreadName
        {
            get { return "{0}: {1}".Fmt(GetType().Name, QueueName); }
        }

        // TODO: Refactor for MQ
        // readonly object msgLock = new object();
        // private bool receivedNewMsgs = false;

        /*
        protected override void OnStop()
        {
            // TODO: Need to unblock the loop that is retrieving messages from the MQ
            throw new NotImplementedException("Code iT!");
        }

        protected override void Execute()
        {
            // TODO: Perform loop here to provide re-usable behavior for STOP/START.
            throw new NotImplementedException();
        }
        */

        /*
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
        */

        /*
        /// <summary>
        /// Performs exceution on a background thread.
        /// </summary>
        protected override void Execute()
        {
            // RUNLOOP            
        }
        */

        protected override sealed void InvokeErrorHandler(Exception ex)
        {
            this.ErrorHandler.Invoke(this, ex);
        }

        // ==============================================================================================

        /*
        private void RunLoop()
        {
            
        }
        
        
        protected abstract void StopListeningToMessages();
        
        public void Stop()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopping, WorkerStatus.Started) == WorkerStatus.Started)
            {
                Log.Debug("Stopping MQ Host...");

                //Unblock current bgthread by issuing StopCommand
                try
                {
                    this.StopListeningToMessages();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                    Log.Warn("Could not send STOP message to bg thread: " + ex.Message);
                }
            }
        }
        */
    }
}