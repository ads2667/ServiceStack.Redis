using System;
using System.Threading;
using ServiceStack.Text;

namespace ServiceStack.Messaging
{    
    public interface IQueueHandlerBackgroundWorker : IBackgroundWorker
    {
        string QueueName { get; }

        IQueueHandlerStats GetStats();
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
        
        protected void IncrementMessageCount(int increment)
        {
            totalMessagesReceived += increment;
            lastMsgProcessed = DateTime.UtcNow;
            Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0);
        }

        public IQueueHandlerStats GetStats() 
        {            
            return new QueueHandlerStats(this.QueueName, totalMessagesReceived, Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));
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

        protected override sealed void ExecuteErrorHandler(Exception ex)
        {
            // Increment the continuous error count.
            Interlocked.Increment(ref noOfContinuousErrors);
            base.ExecuteErrorHandler(ex);
        }

        protected override sealed void Execute()
        {
            SleepBackOffMultiplier(Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));

            this.RunLoop();
        }

        protected abstract void RunLoop();

        private int noOfContinuousErrors = 0;
        readonly Random rand = new Random(Environment.TickCount);
        private void SleepBackOffMultiplier(int continuousErrorsCount)
        {
            if (continuousErrorsCount == 0) return;
            const int MaxSleepMs = 60 * 1000;

            //exponential/random retry back-off.
            var nextTry = Math.Min(
                rand.Next((int)Math.Pow(continuousErrorsCount, 3), (int)Math.Pow(continuousErrorsCount + 1, 3) + 1),
                MaxSleepMs);

            Log.Debug("Sleeping for {0}ms after {1} continuous errors".Fmt(nextTry, continuousErrorsCount));

            Thread.Sleep(nextTry);
        }
    }
}