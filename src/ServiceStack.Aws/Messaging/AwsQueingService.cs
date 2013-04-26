//using System;
//using System.Collections.Generic;
//using Amazon.SQS;
//using Amazon.SQS.Model;

//namespace ServiceStack.Aws.Messaging
//{
//    public static class AwsQueingService
//    {
//        /// <summary>
//        /// Gets a list of all registered message queues
//        /// </summary>
//        /// <param name="client">The amazon sqs client.</param>
//        /// <returns>A list of all registered message queues.</returns>
//        public static IList<string> GetAllQueueNames(AmazonSQS client)
//        {
//            var listQueuesRequest = new ListQueuesRequest();
//            var listQueuesResponse = client.ListQueues(listQueuesRequest);

//            if (!listQueuesResponse.IsSetListQueuesResult())
//            {
//                throw new InvalidOperationException("Could not retrieve list of message queues from Amazon SQS.");
//            }

//            return listQueuesResponse.ListQueuesResult.QueueUrl;
//        }

//        /// <summary>
//        /// Creates a new Message Queue on SQS and returns the queue url.
//        /// </summary>
//        /// <param name="client">The amazon sqs client.</param>
//        /// <param name="queueName">The name of the new message queue.</param>
//        /// <returns>The url of the new message queue.</returns>
//        public static string CreateMessageQueue(AmazonSQS client, string queueName)
//        {
//            var createQueueRequest = new CreateQueueRequest().WithQueueName(queueName);
//            var response = client.CreateQueue(createQueueRequest);
//            if (!response.IsSetCreateQueueResult())
//            {
//                throw new InvalidOperationException(String.Format("Could not create queue name '{0}' on Amazon SQS.", queueName));
//            }

//            return response.CreateQueueResult.QueueUrl;
//        }

//        /// <summary>
//        /// Attempts to receive a message from a message queue.
//        /// </summary>
//        /// <param name="client">The amazon sqs client.</param>
//        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
//        /// <returns>The received message reponse.</returns>
//        public static ReceiveMessageResponse ReceiveMessage(AmazonSQS client, string queueUrl)
//        {
//            return client.ReceiveMessage(new ReceiveMessageRequest()
//                                             .WithQueueUrl(queueUrl)
//                                             .WithMaxNumberOfMessages(1)
//                                             .WithWaitTimeSeconds(5));
//        }

//        /// <summary>
//        /// Attempts to send a message to a message queue.
//        /// </summary>
//        /// <param name="client">The amazon sqs client.</param>
//        /// <param name="queueUrl">The url of the queue to retrieve a message from.</param>
//        /// <param name="message">The message to send.</param>
//        /// <returns>The send message response.</returns>
//        public static SendMessageResponse PublishMessage(AmazonSQS client, string queueUrl, string message)
//        {
//            return client.SendMessage(new SendMessageRequest()
//                                             .WithQueueUrl(queueUrl)
//                                             .WithMessageBody(message));
//        }

//        public static void DeleteMessage(AmazonSQS client, string queueUrl, string receiptHandle)
//        {
//            client.DeleteMessage(new DeleteMessageRequest().WithQueueUrl(queueUrl).WithReceiptHandle(receiptHandle));
//        }
//    }
//}
