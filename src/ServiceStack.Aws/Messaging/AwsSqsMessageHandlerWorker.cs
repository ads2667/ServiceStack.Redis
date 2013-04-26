using System;
using System.Collections.Generic;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerWorker : MessageHandlerBackgroundWorker // MessageHandlerWorker
    {
        public ISqsClient SqsClient { get; private set; }
        public AwsSqsServer MqServer { get; private set; }
        public IDictionary<string, string> QueueUrls { get; private set; }
        
        public AwsSqsMessageHandlerWorker(ISqsClient sqsClient, AwsSqsServer mqServer, IDictionary<string, string> queueUrls, IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler) 
            : base(messageHandler, queueName, errorHandler)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");

            SqsClient = sqsClient;
            MqServer = mqServer;
            QueueUrls = queueUrls;
        }

        public override IMessageHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new AwsSqsMessageHandlerWorker(this.SqsClient, this.MqServer, this.QueueUrls, this.messageHandler, this.QueueName, this.ErrorHandler);
        }
        
        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this.SqsClient, this.MqServer, this.QueueUrls, null);
        }

        public override void Dispose()
        {
            base.Dispose();
            if (this.SqsClient != null)
            {
                this.SqsClient.Dispose();
            }
        }
    }
}
