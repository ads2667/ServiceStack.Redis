using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageQueueClient : MessageQueueClient
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (AwsSqsMessageQueueClient));

        public AwsSqsServer MqServer { get; set; }
        public IDictionary<string, string> QueueNames { get; set; }
        private readonly AmazonSQS client;

        public AwsSqsMessageQueueClient(Amazon.SQS.AmazonSQS sqsClient, AwsSqsServer mqServer, IDictionary<string, string> queueNames, Action onPublishedCallback) 
            : base(onPublishedCallback)
        {            
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            if (queueNames == null) throw new ArgumentNullException("queueNames");
            client = sqsClient;
            MqServer = mqServer;
            QueueNames = queueNames;
        }

        protected override void PublishMessage(string queueName, byte[] messageBytes)
        {
            // TODO: Refactor!
            // Publish to queue
            Log.DebugFormat("Publishing to queue: {0}", queueName);
            var response = AwsQueingService.PublishMessage(client, this.GetQueueNameOrUrl(queueName), Convert.ToBase64String(messageBytes));
            if (!response.IsSetSendMessageResult())
            {
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", queueName));
            }
        }

        public override void Notify(string queueName, byte[] messageBytes)
        {
            var message = Convert.ToBase64String(messageBytes);
            // AwsQueingService.DeleteMessage(client, queueName, null);

            // TODO: Config messages so no notification is sent! Message.Options
            // Publish to queue
            Log.DebugFormat("Notify send from queue: {0}", queueName);
            var response = AwsQueingService.PublishMessage(client, this.GetQueueNameOrUrl(queueName), message);
            if (!response.IsSetSendMessageResult())
            {
                // TODO: Do not throw ex from BG Thread
                throw new InvalidOperationException(string.Format("Message could not be published to the queue '{0}'.", queueName));
            }
        }

        public override byte[] Get(string queueName, TimeSpan? timeOut)
        {
            // TODO: Timeout?!??!
            Log.Debug(string.Format("Sync Get from queue: {0}", queueName));
            /*
            var response = AwsQueingService.ReceiveMessage(client, this.GetQueueNameOrUrl(queueName));
            if (response.IsSetReceiveMessageResult())
            {
                // For testing, delete here to see what happens
                AwsQueingService.DeleteMessage(client, this.GetQueueNameOrUrl(queueName), response.ReceiveMessageResult.Message[0].ReceiptHandle);

                return Convert.FromBase64String(response.ReceiveMessageResult.Message[0].Body);
            }
            */

            return null;
        }

        protected override string GetQueueNameOrUrl(string queueName)
        {
            return this.QueueNames[queueName];
        }

        public override byte[] GetAsync(string queueName)
        {            
            var message = this.MqServer.DequeMessage(queueName);
            if (message == null)
            {
                return null;
            }

            // var sqsMessage = message.Body as ISqsMessage;
            

            // For testing, delete here to see what happens
            /*
            Log.Debug(string.Format("Deleting Message From Queue: {0}", queueName));
            AwsQueingService.DeleteMessage(client, this.GetQueueNameOrUrl(queueName), message.ReceiptHandle);
            */

            /*
            var serializedMessage = JsonSerializer.SerializeToString(message);
            return System.Text.Encoding.UTF8.GetBytes(serializedMessage);
            */
            Log.DebugFormat("Get Async from queue: {0}", queueName);
            return message.ToBytes();
            // return Convert.FromBase64String(message.Body);

            /*
            var response = AwsQueingService.ReceiveMessage(client, this.GetQueueNameOrUrl(queueName));
            if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message.Count > 0)
            {
                // For testing, delete here to see what happens
                AwsQueingService.DeleteMessage(client, this.GetQueueNameOrUrl(queueName), response.ReceiveMessageResult.Message[0].ReceiptHandle);

                return Convert.FromBase64String(response.ReceiveMessageResult.Message[0].Body);
            }
            */

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
                // TODO: Need to dispose of the client at some stage, consider adding a 'STOP' method to clients?
                // TODO: Or create a factory method for the client and dispose within the context of a client, what is the overhead?
                //client.Dispose();
            }
        }
    }
}
