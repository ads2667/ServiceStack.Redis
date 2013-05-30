using System;
using System.Collections.Generic;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageFactory : IMessageFactory
    {
        public ISqsClient SqsClient { get; private set; }
        public AwsSqsServer MqServer { get; private set; }
        
        private readonly IDictionary<string, string> queueUrls;

        public AwsSqsMessageFactory(ISqsClient sqsClient, AwsSqsServer mqServer, IDictionary<string, string> queueUrls)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            
            this.queueUrls = queueUrls;
            this.SqsClient = sqsClient;
            this.MqServer = mqServer;
        }

        public void Dispose()
        {
            if (this.SqsClient != null)
            {
                //this.SqsClient.Dispose();
            }
        }

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this.SqsClient, this.MqServer, queueUrls, null);
        }

        public IMessageProducer CreateMessageProducer()
        {
            return new AwsSqsMessageProducer(this.SqsClient, queueUrls, null);
        }
    }
}
