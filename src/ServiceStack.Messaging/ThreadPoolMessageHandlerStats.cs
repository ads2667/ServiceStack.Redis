using System;
using System.Threading;
using ServiceStack.Messaging;

namespace ServiceStack.Messaging
{
    public interface IThreadPoolMessageHandlerStats : IMessageHandlerStats
    {
        void IncrementMessageFailedCount(int increment);
        void IncrementMessageProcessedCount(int increment);
        void IncrementMessageRetryCount(int increment);
    }

    public class ThreadPoolMessageHandlerStats : IThreadPoolMessageHandlerStats
    {
        private int _totalMessagesProcessed;
        private int _totalMessagesFailed;
        private int _totalRetries;

        public ThreadPoolMessageHandlerStats(string messageHandlerName)
        {
            if (messageHandlerName == null)
            {
                throw new ArgumentNullException("messageHandlerName");
            }

            this.Name = messageHandlerName;
        }

        public void Add(IMessageHandlerStats stats)
        {
            if (stats == null)
            {
                throw new ArgumentNullException("stats");
            }

            this._totalMessagesFailed += stats.TotalMessagesFailed;
            this._totalMessagesProcessed += stats.TotalMessagesProcessed;
            this._totalRetries += stats.TotalRetries;
            this.TotalNormalMessagesReceived += stats.TotalNormalMessagesReceived;
            this.TotalPriorityMessagesReceived += stats.TotalPriorityMessagesReceived;
        }

        public int TotalMessagesProcessed
        {
            get { return _totalMessagesProcessed; }
        }

        public int TotalMessagesFailed
        {
            get { return _totalMessagesFailed; }         
        }

        public int TotalRetries
        {
            get { return _totalRetries; }
        }

        public string Name { get; private set; }
        public int TotalNormalMessagesReceived { get; private set; }
        public int TotalPriorityMessagesReceived { get; private set; }

        public void IncrementMessageFailedCount(int increment)
        {
            Interlocked.Add(ref _totalMessagesFailed, increment);
        }

        public void IncrementMessageProcessedCount(int increment)
        {
            Interlocked.Add(ref _totalMessagesProcessed, increment);
        }

        public void IncrementMessageRetryCount(int increment)
        {
            Interlocked.Add(ref _totalRetries, increment);
        }

        public override string ToString()
        {
            /*
            ---------------
            Stats for Message Type: Hello4, Queue: mq:Hello4:1.0.0.0.inq.
            ---------------
            TotalProcessed: 3
            TotalRetries: 0
            TotalFailed: 0
            */
            return string.Format("---------------\nStats for Message Type: {0}---------------\nTotalMessagesProcessed: {1}\nTotalMessagesFailed: {2}\nTotalRetries: {3}", this.Name, this.TotalMessagesProcessed, this.TotalMessagesFailed, this.TotalRetries);
        }
    }
}
