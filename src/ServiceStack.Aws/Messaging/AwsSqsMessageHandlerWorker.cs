using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerWorker : MessageHandlerWorker
    {
        public IDictionary<string, string> QueueUrls { get; set; }
        private readonly AmazonSQS client;

        public AwsSqsMessageHandlerWorker(Amazon.SQS.AmazonSQS sqsClient, IDictionary<string, string> queueUrls, IMessageHandler messageHandler, string queueName, Action<MessageHandlerWorker, Exception> errorHandler) 
            : base(messageHandler, queueName, errorHandler)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            client = sqsClient;
            QueueUrls = queueUrls;
        }

        public override MessageHandlerWorker Clone()
        {
            return new AwsSqsMessageHandlerWorker(client, this.QueueUrls, this.messageHandler, this.QueueName, this.errorHandler);
        }
        
        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(client, this.QueueUrls, null);
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
