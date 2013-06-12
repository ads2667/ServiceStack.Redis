using System;

namespace ServiceStack.Aws.Messaging
{
    public interface ISqsMessageBody
    {
        /*
        /// <summary>
        /// Gets or sets the message Id.
        /// </summary>
        string MessageId { get; set; }
        */

        /// <summary>
        /// Gets or sets the receipt handle of the message.
        /// </summary>
        string ReceiptHandle { get; set; }

        /// <summary>
        /// Gets or sets the URL of the queue where the message was received from.
        /// </summary>
        string QueueUrl { get; set; }

        /// <summary>
        /// Gets or sets the name of the queue where the message was received from.
        /// </summary>
        string QueueName { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout of the message.
        /// </summary>
        decimal VisibilityTimeout { get; set; }

        /// <summary>
        /// Gets or sets the count of the number of times that this message has been attempted to be processed.
        /// </summary>
        int PreviousRetryAttempts { get; set; }

        /// <summary>
        /// Gets the message expiry time in UTC. This is the time when the message
        /// expires in the SQS and can be recevied by another message queue client.
        /// </summary>
        /// <remarks>
        /// If a message has not already been processed before it's expiry time, it should
        /// not be processed. The next client that receives the message should process it.
        /// </remarks>
        DateTime MessageExpiryTimeUtc { get; }
    }
}
