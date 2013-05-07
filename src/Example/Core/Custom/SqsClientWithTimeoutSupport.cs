using System;
using System.Net;
using System.Net.Sockets;
using Amazon.SQS;
using ServiceStack.Aws.Messaging;

namespace Example.Core.Custom
{
    /// <summary>
    /// Provides a SqsClient implementation that throws <see cref="TimeoutException"/>'s 
    /// if any connectivity issues occur with the Amazon SQS service. This allows the SQS service
    /// to be easily used with the <see cref="RetryManager"/>.
    /// </summary>
    public class SqsClientWithTimeoutSupport : SqsClient
    {
        public SqsClientWithTimeoutSupport(AmazonSQS client) 
            : base(client)
        {
        }

        /// <summary>
        /// Creates a new Message Queue on SQS and returns the queue url.
        /// </summary>
        /// <param name="createQueueRequest">The create new message queue request.</param>
        /// <returns>The create message queue response.</returns>
        public override Amazon.SQS.Model.CreateQueueResponse CreateMessageQueue(Amazon.SQS.Model.CreateQueueRequest createQueueRequest)
        {
            return ExecuteWithTimeoutSupport(() => base.CreateMessageQueue(createQueueRequest));
        }

        /// <summary>
        /// Deletes a message from the message queue.
        /// </summary>
        /// <param name="deleteMessageRequest">The delete message request.</param>
        /// <returns>The delete message response.</returns>
        public override Amazon.SQS.Model.DeleteMessageResponse DeleteMessage(Amazon.SQS.Model.DeleteMessageRequest deleteMessageRequest)
        {
            return ExecuteWithTimeoutSupport(() => base.DeleteMessage(deleteMessageRequest));
        }

        /// <summary>
        /// Gets a list of all registered message queues
        /// </summary>
        /// <param name="listQueuesRequest">The list queue request object.</param>
        /// <returns>A list of all registered message queues.</returns>
        public override Amazon.SQS.Model.ListQueuesResponse GetAllQueueNames(Amazon.SQS.Model.ListQueuesRequest listQueuesRequest)
        {
            return ExecuteWithTimeoutSupport(() => base.GetAllQueueNames(listQueuesRequest));
        }

        /// <summary>
        /// Publishes a message to an amazon sqs message queue.
        /// </summary>
        /// <returns>The send message response.</returns>
        public override Amazon.SQS.Model.SendMessageResponse PublishMessage(Amazon.SQS.Model.SendMessageRequest sendMessageRequest)
        {
            return ExecuteWithTimeoutSupport(() => base.PublishMessage(sendMessageRequest));
        }

        /// <summary>
        /// Attempts to receive one or more messages from a message queue.
        /// </summary>
        /// <param name="receiveMessageRequest">The receieve message request.</param>
        /// <returns>The received message reponse.</returns>
        public override Amazon.SQS.Model.ReceiveMessageResponse ReceiveMessage(Amazon.SQS.Model.ReceiveMessageRequest receiveMessageRequest)
        {
            return ExecuteWithTimeoutSupport(() => base.ReceiveMessage(receiveMessageRequest));
        }

        /// <summary>
        /// Executes a SQS function.
        /// </summary>
        /// <typeparam name="T">The data type being returned.</typeparam>
        /// <param name="func">The function to execute.</param>
        /// <returns>The function result, or a <see cref="TimeoutException"/> if any connectivity errors occur.</returns>
        private static T ExecuteWithTimeoutSupport<T>(Func<T> func)
        {
            try
            {
                // TODO: Unit Test
                throw new NotImplementedException("UNIT TESTS");
                return func.Invoke();
            }
            catch (WebException webException)
            {
                // TODO: Need to catch all exceptions that can cause timeout errors
                // TODO: Write unit tests to verify re-tryable exceptions
                // see: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryErrors.html
                throw new TimeoutException("Timeout occurred accessing Amazon SQS.", webException);
            }
            catch (SocketException socketException)
            {
                // TODO: Need to catch all exceptions that can cause timeout errors
                // TODO: Write unit tests to verify re-tryable exceptions
                // see: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryErrors.html
                throw new TimeoutException("Timeout occurred accessing Amazon SQS.", socketException);
            }
        }
    }
}
