using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageProducer : MessageProducer
    {
        public ISqsClient SqsClient { get; private set; }
        public IDictionary<string, string> QueueUrls { get; private set; }
        
        // Amazon.SQS.AmazonSQS client = Amazon.AWSClientFactory.CreateAmazonSQSClient("key", "secretKey");

        public AwsSqsMessageProducer(ISqsClient sqsClient, IDictionary<string, string> queueUrls, Action onPublishedCallback) 
            : base(onPublishedCallback)
        {
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            this.SqsClient = sqsClient;
            this.QueueUrls = queueUrls;
        }

        protected override string GetQueueNameOrUrl<T>(IMessage<T> message)
        {
            return this.QueueUrls[message.ToInQueueName()];
        }

        protected override void PublishMessage<T>(IMessage<T> message)
        {
            // TODO: Refactor to reusable method. Also use when sending notification messages?
            var messageQueueName = message.ToInQueueName();
            if (!this.QueueUrls.ContainsKey(messageQueueName))
            {
                throw new InvalidOperationException(string.Format("No queue is registered for the message type {0}.", typeof(T).Name));
            }

            // TODO: If a notification response is required, clients should use the 'MessageQueueClient'.
            // TODO: Should messaegs from the Producer be auto-configured to be one-way only?
            // TODO: Need to test DLQ, and write tests to verify all messages are being processed.
            // TODO: Look at using in-memory for these unit tests
            message.Options = (int)MessageOption.None; //// Do not send a reply to the out queue.
            
            Log.DebugFormat("Publishing message to queue {0}.", message.ToInQueueName());
            var response =
                this.SqsClient.PublishMessage(
                    new SendMessageRequest().WithQueueUrl(this.GetQueueNameOrUrl(message))
                                            .WithMessageBody(Convert.ToBase64String(message.ToBytes())));
               
            

            //message. client.SendMessage(new SendMessageRequest(){})
            // throw new NotImplementedException();
        }

        public override void Dispose()
        {
            if (this.SqsClient != null)
            {
                this.SqsClient.Dispose();
            }
        }
    }
}
 