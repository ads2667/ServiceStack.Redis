using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Logging;
using ServiceStack.Messaging;

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

            // Log.Debug("SqsClient: Getting all queue names");
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

            // Log.DebugFormat("SqsClient: Creating new queue: {0}.", createQueueRequest.QueueName);
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
        /// <param name="waitTimeInSeconds">The time in seconds, to wait for a message to be returned from the SQS message queue.</param>
        /// <param name="maxNumberOfMessages">The maximum number of messges to receive per request.</param>
        /// <param name="visibilityTimeout">The time, in seconds, the client has to process the message before it can be received by another client.</param>
        /// <param name="attributeNames">Optionally, any attributes required.</param>
        /// <returns>The received message reponse.</returns>
        public ReceiveMessageResponse ReceiveMessage(string queueUrl, int waitTimeInSeconds, decimal maxNumberOfMessages, decimal visibilityTimeout, params string[] attributeNames)
        {
            if (waitTimeInSeconds > 20)
            {
                throw new ArgumentException("WaitTimeInSeconds must be a value between 0 and 20 seconds.", "waitTimeInSeconds");    
            }

            if (maxNumberOfMessages < 1)
            {
                throw new ArgumentException("MaxNumberOfMessages must be a minimum value of 1.", "maxNumberOfMessages");
            }

            var request = new ReceiveMessageRequest()                
                .WithQueueUrl(queueUrl)
                .WithVisibilityTimeout(visibilityTimeout)
                .WithMaxNumberOfMessages(maxNumberOfMessages)
                .WithWaitTimeSeconds(waitTimeInSeconds);

            if (attributeNames != null && attributeNames.Length > 0)
            {
                request = request.WithAttributeName(attributeNames);
            }

            return this.ReceiveMessage(request);
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

            // Log.DebugFormat("SqsClient: Receiving message(s) from queue: {0}.", receiveMessageRequest.QueueUrl);
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

            // Log.DebugFormat("SqsClient: Sending message to queue: {0}.", sendMessageRequest.QueueUrl);
            var response = this.Client.SendMessage(sendMessageRequest);
            if (!response.IsSetSendMessageResult())
            {
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", sendMessageRequest.QueueUrl));
            }

            return response;
        }

        /// <summary>
        /// Changes the visibility of a message in an SQS queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue that the message was received from.</param>
        /// <param name="messageReceiptHandle">The receipt handle of the message that will have it's timeout visbility changed.</param>
        /// <param name="visibilityTimeoutInSeconds">The visbility timeout value in seconds.</param>
        /// <returns>The change message visibility response.</returns>
        public virtual ChangeMessageVisibilityResponse ChangeMessageVisibility(string queueUrl, string messageReceiptHandle, decimal visibilityTimeoutInSeconds)
        {
            if (StringExtensions.IsNullOrWhiteSpace(queueUrl))
            {
                throw new ArgumentNullException("queueUrl");
            }

            if (StringExtensions.IsNullOrWhiteSpace(messageReceiptHandle))
            {
                throw new ArgumentNullException("messageReceiptHandle");
            }

            if (visibilityTimeoutInSeconds < 0 || visibilityTimeoutInSeconds > 43200) //// 0 < visibilityTimeOut < 43200 (12 Hour maximum)
            {
                throw new ArgumentException("The visibility timeout must be between 0 and 43200 (12 Hours).", "visibilityTimeoutInSeconds");
            }

            // Log.DebugFormat("SqsClient: Changing message visibility in queue: {0}.", changeMessageVisibilityRequest.QueueUrl);
            return this.ChangeMessageVisibility(new ChangeMessageVisibilityRequest()
                                                    .WithQueueUrl(queueUrl)
                                                    .WithReceiptHandle(messageReceiptHandle)
                                                    .WithVisibilityTimeout(visibilityTimeoutInSeconds));
        }

        /// <summary>
        /// Changes the visibility of a message in an SQS queue.
        /// </summary>
        /// <returns>The change message visibility response.</returns>
        public virtual ChangeMessageVisibilityResponse ChangeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest)
        {
            if (changeMessageVisibilityRequest == null)
            {
                throw new ArgumentNullException("changeMessageVisibilityRequest");
            }

            // Log.DebugFormat("SqsClient: Changing message visibility in queue: {0}.", changeMessageVisibilityRequest.QueueUrl);
            return this.Client.ChangeMessageVisibility(changeMessageVisibilityRequest);            
        }

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="queueUrl">The queue url from which the message will be deleted.</param>
        /// <param name="receiptHandle">The receipt handle of the message to delete.</param>
        public void DeleteMessage(string queueUrl, string receiptHandle)
        {
            if (StringExtensions.IsNullOrWhiteSpace(queueUrl))
            {
                throw new ArgumentNullException("queueUrl");
            }

            if (StringExtensions.IsNullOrWhiteSpace(receiptHandle))
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

            // Log.DebugFormat("SqsClient: Deleting message from queue: {0}.", deleteMessageRequest.QueueUrl);
            return this.Client.DeleteMessage(deleteMessageRequest);
        }

        /// <summary>
        /// Deletes a SQS message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to delete.</param>
        public virtual void DeleteQueue(string queueUrl)
        {
            if (StringExtensions.IsNullOrWhiteSpace(queueUrl))
            {
                throw new ArgumentNullException("queueUrl");
            }

            this.DeleteQueue(new DeleteQueueRequest().WithQueueUrl(queueUrl));
        }

        /// <summary>
        /// Deletes a SQS message queue.
        /// </summary>
        /// <param name="deleteQueueRequest">The delete queue request.</param>
        /// <returns>The delete queue response.</returns>
        public virtual DeleteQueueResponse DeleteQueue(DeleteQueueRequest deleteQueueRequest)
        {
            if (deleteQueueRequest == null)
            {
                throw new ArgumentNullException("deleteQueueRequest");
            }

            // Log.DebugFormat("SqsClient: Deleting queue: {0}.", deleteQueueRequest.QueueUrl);
            return this.Client.DeleteQueue(deleteQueueRequest);
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
