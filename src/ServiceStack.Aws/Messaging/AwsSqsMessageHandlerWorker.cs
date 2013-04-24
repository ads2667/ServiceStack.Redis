using System;
using System.Collections.Generic;
using Amazon.SQS;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerWorker : MessageHandlerBackgroundWorker // MessageHandlerWorker
    {
        public AwsSqsServer MqServer { get; set; }
        public IDictionary<string, string> QueueUrls { get; set; }
        private readonly AmazonSQS client;

        public AwsSqsMessageHandlerWorker(Amazon.SQS.AmazonSQS sqsClient, AwsSqsServer mqServer, IDictionary<string, string> queueUrls, IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler) 
            : base(messageHandler, queueName, errorHandler)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            client = sqsClient;
            MqServer = mqServer;
            QueueUrls = queueUrls;
        }

        public override IMessageHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new AwsSqsMessageHandlerWorker(client, this.MqServer, this.QueueUrls, this.messageHandler, this.QueueName, this.ErrorHandler);
        }
        
        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(client, this.MqServer, this.QueueUrls, null);
        }

        public override void Dispose()
        {
            base.Dispose();
            if (client != null)
            {
                client.Dispose();
            }
        }
    }
}
