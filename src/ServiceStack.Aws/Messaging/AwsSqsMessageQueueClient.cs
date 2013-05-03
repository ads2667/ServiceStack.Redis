using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Amazon.SQS.Model;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageQueueClient : MessageQueueClient
    {
        // TODO: Make this configurable, pass it in from ctor? Provide a default?
        private const int VisibilityTimeout = 15;

        private static readonly ILog Log = LogManager.GetLogger(typeof (AwsSqsMessageQueueClient));

        public ISqsClient SqsClient { get; private set; }
        public IMessageCoordinator MessageCoordinator { get; private set; }
        public IDictionary<string, string> QueueNames { get; private set; }
        
        public AwsSqsMessageQueueClient(ISqsClient sqsClient, IMessageCoordinator messageCoordinator, IDictionary<string, string> queueNames, Action onPublishedCallback) 
            : base(onPublishedCallback)
        {            
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }

            if (messageCoordinator == null)
            {
                throw new ArgumentNullException("messageCoordinator");
            }

            if (queueNames == null)
            {
                throw new ArgumentNullException("queueNames");
            }

            this.SqsClient = sqsClient;
            this.MessageCoordinator = messageCoordinator;
            this.QueueNames = queueNames;
        }

        protected override void PublishMessage(string queueName, byte[] messageBytes)
        {
            this.CreateMessageQueueIfNotExists(queueName);

            // Publish to queue
            Log.DebugFormat("Publishing to queue: {0}", queueName);
            this.SqsClient.PublishMessage(this.GetQueueNameOrUrl(queueName), Convert.ToBase64String(messageBytes));            
        }

        public override void Notify(string queueName, byte[] messageBytes)
        {
            this.CreateMessageQueueIfNotExists(queueName);

            // Publish to queue the out queue
            Log.DebugFormat("Notification sent to queue: {0}", queueName);
            this.SqsClient.PublishMessage(this.GetQueueNameOrUrl(queueName), Convert.ToBase64String(messageBytes));           
        }

        /// <summary>
        /// Deletes a message from an Amazon SQS queue.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <param name="messageReceiptHandle">The receipt handle of the message to delete.</param>
        public void DeleteMessageFromQueue(string queueName, string messageReceiptHandle)
        {
            // this.CreateMessageQueueIfNotExists(queueName);
            this.SqsClient.DeleteMessage(this.GetQueueNameOrUrl(queueName), messageReceiptHandle);
        }

        /// <summary>
        /// Synchronously gets a message from the specified queue.
        /// The thread will block until a message is received.
        /// </summary>
        /// <param name="queueName">The queue to get the message from.</param>
        /// <param name="timeOut">Optionally, a timeout value to stop waiting for a message.</param>
        /// <returns>The message received from the queue, or null if the timeout expires and a message is not received.</returns>
        /// <remarks>
        /// Once the message has been processed, you will need to delete the message from SQS.
        /// If the message is not deleted, it will be processed again.
        /// </remarks>
        public override byte[] Get(string queueName, TimeSpan? timeOut)
        {
            // TODO: Need to verify that MQ exists, create?, get the URL if not already in the dictionary.
            this.CreateMessageQueueIfNotExists(queueName);

            Log.Debug(string.Format("Sync Get from queue: {0}", queueName));

            // Block the thread until a message is received from the queue, or a timeout occurs.
            var remainingTimeOut = timeOut.HasValue ? timeOut.Value : TimeSpan.MaxValue;
            do 
            {
                var requestTimeout = remainingTimeOut.TotalSeconds > 20 ? 20 : Convert.ToInt16(remainingTimeOut.TotalSeconds);
                var receiveMessageRequest = new ReceiveMessageRequest()
                    .WithQueueUrl(this.GetQueueNameOrUrl(queueName))
                    .WithMaxNumberOfMessages(1) //// Only waiting for a single message, as the queuename should be unique.
                    .WithWaitTimeSeconds(requestTimeout) //// Long polling
                    .WithVisibilityTimeout(VisibilityTimeout); //// Once received, how long until it can be received again.

                var response = this.SqsClient.ReceiveMessage(receiveMessageRequest);
                if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message != null)
                {
                    if (response.ReceiveMessageResult.Message.Count == 1)
                    {           
                        // TODO: Get the Stats printing to log when app closed.

                        // TODO: Need to verify it is a SqsMessage, so the client has access to the message receipt handle
                        var message = response.ReceiveMessageResult.Message[0];
                        var messageBytes = Convert.FromBase64String(message.Body);

                        // =====================
                        // TODO: This code has been copied and pasted (Q-Handler), refactor into SqsMessageUtil class
                        // TODO: Can we get the message type based on the queue name!?! Even for a reply queue? NO.
                        
                        var m1 = messageBytes.ToMessage<IMessage>();
                        var sqsMessage = m1.Body as ISqsMessage;
                        if (sqsMessage != null)
                        {
                            sqsMessage.MessageId = message.MessageId;
                            sqsMessage.ReceiptHandle = message.ReceiptHandle;
                            sqsMessage.QueueUrl = this.GetQueueNameOrUrl(queueName);
                            sqsMessage.QueueName = queueName;
                        }

                        return m1.ToBytes();
                    }
                }

                // Reduce the timeout value.
                remainingTimeOut = remainingTimeOut.Subtract(TimeSpan.FromSeconds(requestTimeout));
            } while (remainingTimeOut.TotalSeconds > 0);
            
            return null;
        }

        protected override string GetQueueNameOrUrl(string queueName)
        {
            return this.QueueNames[queueName];
        }

        public override byte[] GetAsync(string queueName)
        {
            this.CreateMessageQueueIfNotExists(queueName);

            // TODO: Currently; if this is called directly by a user of MqClient class - it will not work
            // as it relies on the server pumping messages into the coordinator...
            // May need to modify code so that both scenarios are supported.
            var message = this.MessageCoordinator.DequeMessage(queueName);
            if (message == null)
            {
                return null;
            }

            Log.DebugFormat("Get Async from queue: {0}", queueName);
            return message.ToBytes();            
        }

        public override string WaitForNotifyOnAny(params string[] channelNames)
        {
            // this.CreateMessageQueueIfNotExists(queueName);
            // TODO: Block until a message is recieved on any of the specified MQs.
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            /*
            if (this.SqsClient != null)
            {
                // TODO: Need to dispose of the client at some stage, consider adding a 'STOP' method to clients?
                // TODO: Or create a factory method for the client and dispose within the context of a client, what is the overhead?
                this.SqsClient.Dispose();
            }
            */
        }

        protected virtual void CreateMessageQueueIfNotExists(string queueName)
        {
            if (this.QueueNames.ContainsKey(queueName))
            {
                // The message queue already exists;
                return;
            }
            
            // We need to either get the URL remotely, or possibly create a new queue.
            var queueUrl = this.SqsClient.GetOrCreateQueueUrl(queueName);
            this.QueueNames.Add(queueName, queueUrl);
        }

        public void DeleteQueue(string queueName)
        {
            if (string.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            this.SqsClient.DeleteQueue(this.GetQueueNameOrUrl(queueName));
            this.QueueNames.Remove(queueName);
        }
    }
}
