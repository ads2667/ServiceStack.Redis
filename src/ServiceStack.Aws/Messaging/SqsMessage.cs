using System;

namespace ServiceStack.Aws.Messaging
{
    public abstract class SqsMessage : ISqsMessage
    {
        private DateTime dateCreated;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqsMessage"/> class.
        /// </summary>
        protected SqsMessage()
        {
            this.dateCreated = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets or sets the message Id.
        /// </summary>
        public string MessageId { get; set; }

        /// <summary>
        /// Gets or sets the receipt handle of the message.
        /// </summary>
        public string ReceiptHandle { get; set; }

        /// <summary>
        /// Gets or sets the URL of the queue where the message was received from.
        /// </summary>
        public string QueueUrl { get; set; }

        /// <summary>
        /// Gets or sets the name of the queue where the message was received from.
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout of the message.
        /// </summary>
        public decimal VisibilityTimeout { get; set; }

        /// <summary>
        /// Gets the message expiry time in UTC. This is the time when the message
        /// expires in the SQS and can be recevied by another message queue client.
        /// </summary>
        /// <remarks>
        /// If a message has not already been processed before it's expiry time, it should
        /// not be processed. The next client that receives the message should process it.
        /// </remarks>
        public DateTime MessageExpiryTimeUtc
        {
            get { return this.dateCreated.AddSeconds((double)this.VisibilityTimeout); }
        }
    }
    /*
    // Need to decorate with JSON Serializer (ServiceStack) attributes to have properties included.
    // TODO: May need to use fields to provide required serializer access.
    public class AwsSqsMessage : Message, IAwsSqsMessage
    {
        public string MessageId { get; set; }
        public string ReceiptHandle { get; set; }
        public string QueueUrl { get; set; }
    }

    [DataContract]
    public class AwsSqsMessage<T> : Message<T>, IAwsSqsMessage<T>
    {
        public static IAwsSqsMessage<T> FromMessage(IMessage<T> message)
        {
            if (message == null) throw new ArgumentNullException("message");
            var msg = new AwsSqsMessage<T>();
            msg.Body = message.Body;
            msg.CreatedDate = message.CreatedDate;
            msg.Error = message.Error;
            msg.Id = message.Id;
            msg.Options = message.Options;
            msg.Priority = message.Priority;
            msg.ReplyId = message.ReplyId;
            msg.ReplyTo = message.ReplyTo;
            msg.RetryAttempts = message.RetryAttempts;
            return msg;
        }

        [DataMember]
        public string MessageId { get; set; }

        [DataMember]
        public string ReceiptHandle { get; set; }

        [DataMember]
        public string QueueUrl { get; set; }
    }
    */
}
