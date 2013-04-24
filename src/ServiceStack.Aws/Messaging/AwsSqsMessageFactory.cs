using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageFactory : IMessageFactory
    {
        public AwsSqsServer MqServer { get; set; }
        private readonly AmazonSQS client;
        private readonly IDictionary<string, string> queueUrls;

        public AwsSqsMessageFactory(AmazonSQS sqsClient, AwsSqsServer mqServer, IDictionary<string, string> queueUrls)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            this.client = sqsClient;
            this.queueUrls = queueUrls;
            this.MqServer = mqServer;
        }

        public void Dispose()
        {
            if (client != null)
            {
                client.Dispose();
            }
        }

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(client, this.MqServer, queueUrls, null);
        }

        public IMessageProducer CreateMessageProducer()
        {
            return new AwsSqsMessageProducer(client, queueUrls, null);
        }
    }
}
