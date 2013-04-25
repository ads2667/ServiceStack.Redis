using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public interface ISqsMessage : IMessage
    {
        /// <summary>
        /// Gets or sets the message Id.
        /// </summary>
        string MessageId { get; }

        /// <summary>
        /// Gets or sets the receipt handle of the message.
        /// </summary>
        string ReceiptHandle { get; }

        /// <summary>
        /// Gets or sets the URL of the queue where the message was received from.
        /// </summary>
        string QueueUrl { get; }

        /// <summary>
        /// Gets or sets the name of the queue where the message was received from.
        /// </summary>
        string QueueName { get; }
    }

    public interface ISqsMessage<T> : IMessage<T>, ISqsMessage
    {
    }

    /*
    // This code was intended to prevent the Message's having to implement from ISqsMessage
    // but this is not possible because the MessageHandler class would need to be modified
    public interface IAwsSqsMessage : IMessage
    {
        /// <summary>
        /// Gets or sets the message Id.
        /// </summary>
        string MessageId { get; set; }

        /// <summary>
        /// Gets or sets the receipt handle of the message.
        /// </summary>
        string ReceiptHandle { get; set; }

        /// <summary>
        /// Gets or sets the URL of the queue where the message was received from.
        /// </summary>
        string QueueUrl { get; set; }
    }

    public interface IAwsSqsMessage<T> : IMessage<T>, IAwsSqsMessage
    {      
    }
    */
}
