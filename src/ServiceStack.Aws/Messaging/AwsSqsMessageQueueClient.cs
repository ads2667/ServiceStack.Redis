using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageQueueClient : MessageQueueClient
    {
        public IDictionary<string, string> QueueNames { get; set; }
        private readonly AmazonSQS client;

        public AwsSqsMessageQueueClient(Amazon.SQS.AmazonSQS sqsClient, IDictionary<string, string> queueNames, Action onPublishedCallback) 
            : base(onPublishedCallback)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (queueNames == null) throw new ArgumentNullException("queueNames");
            client = sqsClient;
            QueueNames = queueNames;
        }

        protected override void PublishMessage(string queueName, byte[] messageBytes)
        {
            // TODO: Refactor!
            // Publish to queue
            var response = AwsQueingService.PublishMessage(client, this.GetQueueNameOrUrl(queueName), Convert.ToBase64String(messageBytes));
            if (!response.IsSetSendMessageResult())
            {
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", queueName));
            }
        }

        public override void Notify(string queueName, byte[] messageBytes)
        {
            // TODO: Config messages so no notification is sent! Message.Options
            // Publish to queue
            var response = AwsQueingService.PublishMessage(client, this.GetQueueNameOrUrl(queueName), Convert.ToBase64String(messageBytes));
            if (!response.IsSetSendMessageResult())
            {
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", queueName));
            }
        }

        public override byte[] Get(string queueName, TimeSpan? timeOut)
        {
            // TODO: Timeout?!??!
            var response = AwsQueingService.ReceiveMessage(client, this.GetQueueNameOrUrl(queueName));
            if (response.IsSetReceiveMessageResult())
            {
                return Convert.FromBase64String(response.ReceiveMessageResult.Message[0].Body);
            }

            return null;
        }

        protected override string GetQueueNameOrUrl(string queueName)
        {
            return this.QueueNames[queueName];
        }

        public override byte[] GetAsync(string queueName)
        {
            var response = AwsQueingService.ReceiveMessage(client, this.GetQueueNameOrUrl(queueName));
            if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message.Count > 0)
            {
                return Convert.FromBase64String(response.ReceiveMessageResult.Message[0].Body);
            }

            return null;
        }

        public override string WaitForNotifyOnAny(params string[] channelNames)
        {
            throw new NotImplementedException();
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
