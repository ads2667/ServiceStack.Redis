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
        public IDictionary<string, string> QueueUrls { get; private set; }
        private readonly AmazonSQS client;
        // Amazon.SQS.AmazonSQS client = Amazon.AWSClientFactory.CreateAmazonSQSClient("key", "secretKey");

        public AwsSqsMessageProducer(Amazon.SQS.AmazonSQS client, IDictionary<string, string> queueUrls, Action onPublishedCallback) 
            : base(onPublishedCallback)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            this.QueueUrls = queueUrls;
            this.client = client;
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
                client.SendMessage(
                    new SendMessageRequest().WithQueueUrl(this.GetQueueNameOrUrl(message))
                                            .WithMessageBody(Convert.ToBase64String(message.ToBytes())));
               
            if (!response.IsSetSendMessageResult())
            {
                throw new NotImplementedException("Could not publish message. Error handling not implemented.");    
            }

            //message. client.SendMessage(new SendMessageRequest(){})
            // throw new NotImplementedException();
        }

        public override void Dispose()
        {
            if (client != null)
            {
                client.Dispose();
            }
        }
    }
}
 