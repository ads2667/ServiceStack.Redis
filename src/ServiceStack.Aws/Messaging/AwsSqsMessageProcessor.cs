using System;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageProcessor : IMessageProcessor
    {
        protected readonly ILog Log;

        public AwsSqsMessageProcessor(ISqsClient sqsClient)
        {
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }
            
            this.SqsClient = sqsClient;
            this.Log = LogManager.GetLogger(this.GetType());
        }

        public ISqsClient SqsClient { get; private set; }

        /// <summary>
        /// This method is executed before a message is processed.
        /// </summary>
        /// <param name="message">The original message from the message queue.</param>
        /// <remarks>
        /// Implementors can override this method to implement common behavior(s) for all messages
        /// <para />
        /// A common requirement may be to verify that this message has not already been processed.
        /// </remarks>
        public virtual bool CanProcessMessage<T>(IMessage<T> message)
        {
            var messageBody = GetSqsMessageBody(message);            
            Log.DebugFormat("On Pre Message Processing, Queue: {0}. Thread: {1}. Retry Attempt: {2}.", messageBody.QueueName, Thread.CurrentThread.ManagedThreadId, message.RetryAttempts);

            var remainingMessageProcessingTime = DateTime.UtcNow.Subtract(messageBody.MessageExpiryTimeUtc).TotalSeconds;
            if (remainingMessageProcessingTime > 0)
            {
                // The message is expired.
                this.Log.WarnFormat("The message '{0}' has passed it's expiry time by {1} seconds. The message will not be processed.", messageBody.MessageId, remainingMessageProcessingTime);
                return false;
            }

            this.Log.InfoFormat("The message '{0}' has a remaining {1} seconds to be processed. Thread: {2}.", messageBody.MessageId, Math.Abs(remainingMessageProcessingTime), Thread.CurrentThread.ManagedThreadId);
            return true;
        }

        public virtual void OnMessageProcessed<T>(IMessage<T> message)
        {
            var messageBody = GetSqsMessageBody(message);
            Log.DebugFormat("Message processed, deleting Message {0} from Queue: {1}. Thread: {2}.", messageBody.MessageId, messageBody.QueueName, Thread.CurrentThread.ManagedThreadId);

            try
            {
                this.SqsClient.DeleteMessage(messageBody.QueueUrl, messageBody.ReceiptHandle);
            }
            catch (Exception ex)
            {
                Log.Error(string.Format("Error Deleting message {0} from queue {1} after being processed successfully.", messageBody.MessageId, messageBody.QueueName), ex);
            }
        }

        public virtual void OnMessageProcessingFailed<T>(IMessage<T> message, Exception exception, bool moveMessageToDlq)
        {
            var messageBody = GetSqsMessageBody(message);
            Log.DebugFormat("On Message Processing Failed, Queue: {0}. Thread: {1}.", messageBody.QueueName, Thread.CurrentThread.ManagedThreadId);

            if (moveMessageToDlq)
            {
                this.MoveMessageToDlq(message);
                return;
            }

            // This method will never throw an exception.
            this.UpdateMessageVisibilityTimeout(message);            
        }

        /// <summary>
        /// Gets the message body, cast to an <see cref="ISqsMessage"/> type.
        /// </summary>
        /// <param name="message">The original MQ message.</param>
        /// <returns>
        /// An instance of an <see cref="ISqsMessage"/>. 
        /// An exception is thrown if the message body does not implement this interface.
        /// </returns>
        protected static ISqsMessage GetSqsMessageBody<T>(IMessage<T> message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            var messageBody = message.Body as ISqsMessage;            
            if (messageBody == null)
            {
                var messageBodyType = message.Body == null ? "Null" : message.Body.GetType().Name;
                throw new InvalidOperationException(string.Format("The AwsSqsMessageProcessor requires the message body implements ISqsMessage. This message type '{0}' does not implement the required interface.", messageBodyType));
            }

            return messageBody;
        }

        /// <summary>
        /// Gets the message visbility timeout value, in seconds, for a failed message.
        /// This will prevent the message from being processed again for this length of time.
        /// </summary>
        /// <param name="message">The message that failed processing.</param>
        /// <returns>The time to wait before the message is processed again.</returns>
        public virtual TimeSpan GetMessageVisibilityTimeout<T>(IMessage<T> message)
        {
            // Default to 30 seconds. It is recommended any implementations implement their own logic here.
            return TimeSpan.FromSeconds(30);
        }

        protected virtual void UpdateMessageVisibilityTimeout<T>(IMessage<T> message)
        {
            // Update the message visibilityTimeout using a sliding scale to prevent overloading any data store
            var messageBody = GetSqsMessageBody(message);

            try
            {
                var timeoutInSeconds = (decimal)this.GetMessageVisibilityTimeout(message).TotalSeconds;

                Log.DebugFormat("Message {0} failed. Updating msg visibility timeout value to {1} seconds.", messageBody.MessageId, timeoutInSeconds);
                this.SqsClient.ChangeMessageVisibility(messageBody.QueueUrl, messageBody.ReceiptHandle, timeoutInSeconds);
            }
            catch (Exception ex)
            {
                // Log the error, but do not throw as it will prevent the message from being released for re-processing.
                Log.Error(string.Format("Error updating message visibility timeout. Queue: {0}. Message Id: {1}.", messageBody.QueueName, message.Id), ex);
            }
        }


        protected void MoveMessageToDlq<T>(IMessage<T> message)
        {
            // Move the Message to the DLQ (Also need to delete from the current queue)
            var messageBody = GetSqsMessageBody(message);

            Log.DebugFormat("Message {0} has failed {1} times, moving to DLQ.", message.Id, message.RetryAttempts);

            string dlqName = string.Empty;
            try
            {
                var queueNames = new VersionedQueueNames(messageBody.GetType());
                dlqName = queueNames.Dlq;
                var dlqUrl = this.SqsClient.GetOrCreateQueueUrl(dlqName);

                // Put in DLQ before deleting, we can't 'lose' the message.                        
                this.RepublishFailedMessageToQueue(message, dlqUrl, queueNames.Dlq);

                // Note: If this 'DeleteMessage' command fails, it does not matter.
                this.SqsClient.DeleteMessage(messageBody.QueueUrl, messageBody.ReceiptHandle);
            }
            catch (Exception ex)
            {
                Log.Error(string.Format("An error occurred trying to move the message {0} to the DLQ '{1}'.", message.Id, dlqName), ex);
            }
        }

        protected virtual void RepublishFailedMessageToQueue<T>(IMessage<T> message, string queueUrl, string queueName)
        {
            this.SqsClient.PublishMessage(queueUrl, Convert.ToBase64String(message.ToBytes()));
            Log.DebugFormat("Failed message with Id {0} has been re-published to the queue {1}.", message.Id, queueName);
        }
    }
}
