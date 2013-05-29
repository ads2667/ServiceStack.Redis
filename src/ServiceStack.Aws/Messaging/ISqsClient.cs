using System;
using System.Collections.Generic;
using Amazon.SQS.Model;

namespace ServiceStack.Aws.Messaging
{
    public interface ISqsClient : IDisposable
    {
        /// <summary>
        /// Gets a list of all registered message queues
        /// </summary>
        /// <returns>A list of all registered message queues.</returns>
        IList<string> GetAllQueueNames();

        /// <summary>
        /// Gets a list of all registered message queues
        /// </summary>
        /// <param name="listQueuesRequest">The list queue request object.</param>
        /// <returns>A list of all registered message queues.</returns>
        ListQueuesResponse GetAllQueueNames(ListQueuesRequest listQueuesRequest);

        /// <summary>
        /// Creates a new Message Queue on SQS and returns the queue url.
        /// </summary>
        /// <param name="queueName">The name of the new message queue.</param>
        /// <returns>The url of the new message queue.</returns>
        string CreateMessageQueue(string queueName);

        /// <summary>
        /// Checks to see if a queue exists, and returns the URL or creates a new message queue on SQS if it does not exist.
        /// </summary>
        /// <param name="queueName">The name of the message queue.</param>
        /// <returns>The url of the message queue.</returns>
        string GetOrCreateQueueUrl(string queueName);

        /// <summary>
        /// Creates a new Message Queue on SQS and returns the queue url.
        /// </summary>
        /// <param name="createQueueRequest">The create new message queue request.</param>
        /// <returns>The create message queue response.</returns>
        CreateQueueResponse CreateMessageQueue(CreateQueueRequest createQueueRequest);
        
        /// <summary>
        /// Attempts to receive a message from a message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
        /// <param name="waitTimeInSeconds">The time in seconds, to wait for a message to be returned from the SQS message queue.</param>
        /// <param name="maxNumberOfMessages">The maximum number of messges to receive per request.</param>
        /// <param name="visibilityTimeout">The time, in seconds, the client has to process the message before it can be received by another client.</param>
        /// <param name="attributeNames">Optionally, any attributes required.</param>
        /// <returns>The received message reponse.</returns>
        ReceiveMessageResponse ReceiveMessage(string queueUrl, int waitTimeInSeconds, decimal maxNumberOfMessages, decimal visibilityTimeout, params string[] attributeNames);

        /// <summary>
        /// Attempts to receive one or more messages from a message queue.
        /// </summary>
        /// <param name="receiveMessageRequest">The receieve message request.</param>
        /// <returns>The received message reponse.</returns>
        ReceiveMessageResponse ReceiveMessage(ReceiveMessageRequest receiveMessageRequest);        

        /// <summary>
        /// Publishes a message to an amazon sqs message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
        /// <param name="message">The message to send.</param>
        /// <returns>The send message response.</returns>
        SendMessageResponse PublishMessage(string queueUrl, string message);        

        /// <summary>
        /// Publishes a message to an amazon sqs message queue.
        /// </summary>
        /// <returns>The send message response.</returns>
        SendMessageResponse PublishMessage(SendMessageRequest sendMessageRequest);        

        /// <summary>
        /// Changes the visibility of a message in an SQS queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue that the message was received from.</param>
        /// <param name="messageReceiptHandle">The receipt handle of the message that will have it's timeout visbility changed.</param>
        /// <param name="visibilityTimeoutInSeconds">The visbility timeout value in seconds.</param>
        /// <returns>The change message visibility response.</returns>
        ChangeMessageVisibilityResponse ChangeMessageVisibility(string queueUrl, string messageReceiptHandle, decimal visibilityTimeoutInSeconds);
        
        /// <summary>
        /// Changes the visibility of a message in an SQS queue.
        /// </summary>
        /// <returns>The change message visibility response.</returns>
        ChangeMessageVisibilityResponse ChangeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest);

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="queueUrl">The queue url from which the message will be deleted.</param>
        /// <param name="receiptHandle">The receipt handle of the message to delete.</param>
        void DeleteMessage(string queueUrl, string receiptHandle);        

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="deleteMessageRequest">The delete message request.</param>
        /// <returns>The delete message response.</returns>
        DeleteMessageResponse DeleteMessage(DeleteMessageRequest deleteMessageRequest);

        /// <summary>
        /// Deletes a SQS message queue.
        /// </summary>
        /// <param name="queueUrl">The url of the queue to delete.</param>
        void DeleteQueue(string queueUrl);

        /// <summary>
        /// Deletes a SQS message queue.
        /// </summary>
        /// <param name="deleteQueueRequest">The delete queue request.</param>
        /// <returns>The delete queue response.</returns>
        DeleteQueueResponse DeleteQueue(DeleteQueueRequest deleteQueueRequest);
    }
}
