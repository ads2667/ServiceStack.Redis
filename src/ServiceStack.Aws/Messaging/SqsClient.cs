using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Logging;

namespace ServiceStack.Aws.Messaging
{
    public class SqsClient : ISqsClient
    {
        private static readonly Regex QueueNameRegex = new Regex("[:\\.]");

        private static ILog Log = LogManager.GetLogger(typeof (SqsClient));

        public SqsClient(AmazonSQS client)
        {
            if (client == null)
            {
                throw new ArgumentNullException("client");
            }

            this.Client = client;
        }

        /// <summary>
        /// Gets the amazon sqs client.
        /// </summary>
        protected AmazonSQS Client { get; private set; }

        /// <summary>
        /// Gets a list of all registered message queues
        /// </summary>
        /// <returns>A list of all registered message queues.</returns>
        public IList<string> GetAllQueueNames()
        {
            var listQueuesRequest = new ListQueuesRequest();
            var listQueuesResponse = this.GetAllQueueNames(listQueuesRequest);
            return listQueuesResponse.ListQueuesResult.QueueUrl;
        }

        /// <summary>
        /// Gets a list of all registered message queues
        /// </summary>
        /// <param name="listQueuesRequest">The list queue request object.</param>
        /// <returns>A list of all registered message queues.</returns>
        public virtual ListQueuesResponse GetAllQueueNames(ListQueuesRequest listQueuesRequest)
        {
            if (listQueuesRequest == null)
            {
                throw new ArgumentNullException("listQueuesRequest");
            }

            Log.Debug("SqsClient: Getting all queue names");
            var listQueuesResponse = this.Client.ListQueues(listQueuesRequest);
            if (!listQueuesResponse.IsSetListQueuesResult())
            {
                throw new InvalidOperationException("Could not retrieve list of message queues from Amazon SQS.");
            }

            return listQueuesResponse;
        }

        /// <summary>
        /// Checks to see if a queue exists, and returns the URL or creates a new message queue on SQS if it does not exist.
        /// </summary>
        /// <param name="queueName">The name of the message queue.</param>
        /// <returns>The url of the message queue.</returns>
        public string GetOrCreateQueueUrl(string queueName)
        {
            // Clean the queue name
            queueName = QueueNameRegex.Replace(queueName, "_");

            // Get a list of queues from SQS            
            var amazonQueueUrls = this.GetAllQueueNames();

            // Check if the queue exists
            foreach (var queueUrl in amazonQueueUrls)
            {
                if (queueUrl.Substring(queueUrl.LastIndexOf('/') + 1).ToUpperInvariant() == queueName.ToUpperInvariant())
                {
                    return queueUrl;
                }
            }

            return this.CreateMessageQueue(queueName);
        }
        

        /// <summary>
        /// Creates a new Message Queue on SQS and returns the queue url.
        /// </summary>
        /// <param name="queueName">The name of the new message queue.</param>
        /// <returns>The url of the new message queue.</returns>
        public string CreateMessageQueue(string queueName)
        {
            // Clean the queue name
            queueName = QueueNameRegex.Replace(queueName, "_");
            var createQueueRequest = new CreateQueueRequest().WithQueueName(queueName);
            var response = this.CreateMessageQueue(createQueueRequest);            
            return response.CreateQueueResult.QueueUrl;
        }

        /// <summary>
        /// Creates a new Message Queue on SQS and returns the queue url.
        /// </summary>
        /// <param name="createQueueRequest">The create new message queue request.</param>
        /// <returns>The create message queue response.</returns>
        public virtual CreateQueueResponse CreateMessageQueue(CreateQueueRequest createQueueRequest)
        {
            if (createQueueRequest == null)
            {
                throw new ArgumentNullException("createQueueRequest");
            }

            Log.DebugFormat("SqsClient: Creating new queue: {0}.", createQueueRequest.QueueName);
            var response = this.Client.CreateQueue(createQueueRequest);
            if (!response.IsSetCreateQueueResult())
            {
                throw new InvalidOperationException(String.Format("Could not create queue name '{0}' on Amazon SQS.", createQueueRequest.QueueName));
            }

            return response;
        }

        /// <summary>
        /// Attempts to receive a message from a message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
        /// <returns>The received message reponse.</returns>
        public ReceiveMessageResponse ReceiveMessage(string queueUrl)
        {
            return this.ReceiveMessage(new ReceiveMessageRequest()
                                             .WithQueueUrl(queueUrl)
                                             .WithMaxNumberOfMessages(1)
                                             .WithWaitTimeSeconds(5));
        }

        /// <summary>
        /// Attempts to receive one or more messages from a message queue.
        /// </summary>
        /// <param name="receiveMessageRequest">The receieve message request.</param>
        /// <returns>The received message reponse.</returns>
        public virtual ReceiveMessageResponse ReceiveMessage(ReceiveMessageRequest receiveMessageRequest)
        {
            if (receiveMessageRequest == null)
            {
                throw new ArgumentNullException("receiveMessageRequest");
            }

            Log.DebugFormat("SqsClient: Receiving message(s) from queue: {0}.", receiveMessageRequest.QueueUrl);
            return this.Client.ReceiveMessage(receiveMessageRequest);
        }

        /// <summary>
        /// Publishes a message to an amazon sqs message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
        /// <param name="message">The message to send.</param>
        /// <returns>The send message response.</returns>
        public SendMessageResponse PublishMessage(string queueUrl, string message)
        {
            return this.PublishMessage(new SendMessageRequest()
                                             .WithQueueUrl(queueUrl)
                                             .WithMessageBody(message));
        }

        /// <summary>
        /// Publishes a message to an amazon sqs message queue.
        /// </summary>
        /// <returns>The send message response.</returns>
        public virtual SendMessageResponse PublishMessage(SendMessageRequest sendMessageRequest)
        {
            if (sendMessageRequest == null)
            {
                throw new ArgumentNullException("sendMessageRequest");
            }

            Log.DebugFormat("SqsClient: Sending message to queue: {0}.", sendMessageRequest.QueueUrl);
            var response = this.Client.SendMessage(sendMessageRequest);
            if (!response.IsSetSendMessageResult())
            {
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", sendMessageRequest.QueueUrl));
            }

            return response;
        }

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="queueUrl">The queue url from which the message will be deleted.</param>
        /// <param name="receiptHandle">The receipt handle of the message to delete.</param>
        public void DeleteMessage(string queueUrl, string receiptHandle)
        {
            if (string.IsNullOrWhiteSpace(queueUrl))
            {
                throw new ArgumentNullException("queueUrl");
            }

            if (string.IsNullOrWhiteSpace(receiptHandle))
            {
                throw new ArgumentNullException("receiptHandle");
            }

           this.DeleteMessage(new DeleteMessageRequest().WithQueueUrl(queueUrl).WithReceiptHandle(receiptHandle));
        }

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="deleteMessageRequest">The delete message request.</param>
        /// <returns>The delete message response.</returns>
        public virtual DeleteMessageResponse DeleteMessage(DeleteMessageRequest deleteMessageRequest)
        {
            if (deleteMessageRequest == null)
            {
                throw new ArgumentNullException("deleteMessageRequest");
            }

            Log.DebugFormat("SqsClient: Deleting message from queue: {0}.", deleteMessageRequest.QueueUrl);
            return this.Client.DeleteMessage(deleteMessageRequest);
        }

        public void Dispose()
        {
            if (this.Client != null)
            {
                this.Client.Dispose();
            }
        }
    }
}
