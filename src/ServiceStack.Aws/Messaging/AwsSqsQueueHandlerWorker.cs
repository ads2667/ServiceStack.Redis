using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;
using Message = Amazon.SQS.Model.Message;

namespace ServiceStack.Aws.Messaging
{
    /// <summary>
    /// Defines a class that receives messages from a queue, and notifies worker threads that a new message has arrived.
    /// </summary>
    /// <remarks>
    /// TODO: DO NOT LISTEN TO DEAD-LETTER QUEUES!
    /// </remarks>
    public class AwsSqsQueueHandlerWorker : QueueHandlerBackgroundWorker
    {
        public AwsSqsServer MqServer { get; set; }
        private readonly AmazonSQS _client;
        private readonly ILog Log = LogManager.GetLogger(typeof (AwsSqsQueueHandlerWorker));

        private bool listenForMessages = true;

        private readonly object syncLock = new object();

        public AwsSqsQueueHandlerWorker(AmazonSQS client, AwsSqsServer mqServer, KeyValuePair<string, string> queue, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
            : base(queue.Key, errorHandler)
        {
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            this.MqServer = mqServer;
            _client = client;
            this.QueueName = queue.Key;
            this.QueueUrl = queue.Value;
           // this.LocalMessageQueue = new ConcurrentQueue<Message>();
        }


        // protected System.Collections.Concurrent.ConcurrentQueue<Amazon.SQS.Model.Message> LocalMessageQueue { get; private set; }

        public string QueueUrl { get; private set; }

        public string QueueName { get; private set; }

        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this._client, this.MqServer, this.MqServer.QueueUrls, null);
        }

        /*
        public void Start()
        {
            // Start Listening
            lock (syncLock)
            {
                listenForMessages = true;
                this.ReceieveMessages();
            }
        }
        */
        
        protected override void Execute()
        {
            var continuePolling = true;
            // Use Long-Polling to retrieve messages from a specific SQS queue.            
            do
            {
                Log.DebugFormat("Polling SQS Queue '{0}' for messages.", this.QueueName);

                var response = AwsQueingService.ReceiveMessage(_client, this.QueueUrl); // Blocks until timout, or msg received
                if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message.Count > 0)
                {
                    // Place the item in the ready to process queue, and notify workers that a new msg has arrived.                    
                    Log.DebugFormat("Received {0} Message(s) from Queue '{1}'.", response.ReceiveMessageResult.Message.Count, this.QueueName);

                    // Thread-safe local queue -> Provides access to msg reponse, and retrieving multiple msgs!
                    foreach (var message in response.ReceiveMessageResult.Message)
                    {
                        // NOTE: We are not deleting anything, so it will continue to get the same messages
                        // Remember: Need to code so no problem if same message received twice... consider cache?
                        // TODO: Will need to create a seperate queue for each queue litener...
                        // this.LocalMessageQueue.Enqueue(this.QueueName, message);

                        this.MqServer.EnqueMessage(this.QueueName, message);
                        // this.MqServer.NotifyMessageHandlerWorkers(this.QueueName);

                        // For testing only.
                        // AwsQueingService.DeleteMessage(_client, this.QueueUrl, message.ReceiptHandle); 
                    }

                    /*
                    // Notify the workers that there's something to do
                    if (!string.IsNullOrEmpty(this.QueueName))
                    {
                        int[] workerIndexes;
                        if (queueWorkerIndexMap.TryGetValue(this.QueueName, out workerIndexes))
                        {
                            foreach (var workerIndex in workerIndexes)
                            {
                                Log.DebugFormat("Signalling worker '{0}' from Queue '{1}'.", workerIndex, this.QueueName);
                                workers[workerIndex].NotifyNewMessage();
                            }
                        }
                    }
                    */

                    // For testing, delete here to see what happens
                    // AwsQueingService.DeleteMessage(client, this.GetQueueNameOrUrl(queueName), response.ReceiveMessageResult.Message[0].ReceiptHandle);                   
                    lock (syncLock)
                    {
                        continuePolling = listenForMessages;
                    }
                }
            } while (continuePolling);
        }

        protected override void OnStop()
        {
            lock (syncLock)
            {
                listenForMessages = false;
            }
        }

        public override IQueueHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new AwsSqsQueueHandlerWorker(this._client, this.MqServer, new KeyValuePair<string, string>(this.QueueName, this.QueueUrl), this.ErrorHandler);
        }
    }
}
