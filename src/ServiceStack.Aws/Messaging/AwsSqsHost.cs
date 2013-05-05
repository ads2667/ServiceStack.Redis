using System;
using System.Collections.Generic;
using Amazon.SQS;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsHost : MqHost
    {
        public IDictionary<string, string> QueueUrls { get; set; }
        private readonly AmazonSQS client;

        public AwsSqsHost(AmazonSQS sqsClient, IDictionary<string, string> queueUrls, int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null)
            : base(null, retryCount, requestTimeOut)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (queueUrls == null) throw new ArgumentNullException("queueUrls");
            client = sqsClient;
            QueueUrls = queueUrls;
        }

        public override IMessageQueueClient CreateMessageQueueClient()
        {
            throw new NotImplementedException();
            // return new AwsSqsMessageQueueClient(client, this.QueueUrls, null);
        }

        protected override void StopMqService()
        {
            throw new NotImplementedException();
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
