using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceStack.Redis.Messaging
{
    public interface IQueueHandlerStats
    {
        /// <summary>
        /// Gets the name of the queue.
        /// </summary>
        string QueueName { get; }

        /// <summary>
        /// Gets the total messages received by the queue.
        /// </summary>
        long TotalMessagesReceived { get; }

        /// <summary>
        /// Adds queue handler statistics.
        /// </summary>
        /// <param name="stats">The statistics to add.</param>
        /// <returns>Returns the summed value of the queue handler statistics.</returns>
        IQueueHandlerStats Add(IQueueHandlerStats stats);
    }

    public class QueueHandlerStats : IQueueHandlerStats
    {
        public QueueHandlerStats(string name, long totalMessagesReceived)
        {
            this.QueueName = name;
            this.TotalMessagesReceived = totalMessagesReceived;
        }

        public string QueueName { get; private set; }
        public long TotalMessagesReceived { get; private set; }
        public IQueueHandlerStats Add(IQueueHandlerStats stats)
        {
            return new QueueHandlerStats(this.QueueName, this.TotalMessagesReceived + stats.TotalMessagesReceived);
        }
    }
}
