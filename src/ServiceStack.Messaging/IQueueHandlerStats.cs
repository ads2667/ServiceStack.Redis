using System.Text;

namespace ServiceStack.Messaging
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
        public QueueHandlerStats(string name, long totalMessagesReceived, int noOfContinousErrors)
        {
            this.QueueName = name;
            this.TotalMessagesReceived = totalMessagesReceived;
            this.NoOfContinousErrors = noOfContinousErrors;
        }

        public string QueueName { get; private set; }
        public long TotalMessagesReceived { get; private set; }
        public int NoOfContinousErrors { get; set; }

        public IQueueHandlerStats Add(IQueueHandlerStats stats)
        {
            return new QueueHandlerStats(this.QueueName, this.TotalMessagesReceived + stats.TotalMessagesReceived, this.NoOfContinousErrors);
        }

        public override string ToString()
        {
            var sb = new StringBuilder("===============\n");
            sb.AppendFormat("Queue Handler: {0}\n", this.QueueName);
            sb.AppendFormat("Total Messages Received: {0}\n", this.TotalMessagesReceived);
            sb.AppendLine("Num of Continuous Errors: " + this.NoOfContinousErrors);
            return sb.ToString();
        }
    }
}
