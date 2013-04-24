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
        public IDictionary<string, string> QueueUrls { get; set; }
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
            // TODO: Should messaegs from the Producer be auto-configured to be one-way only?
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
 