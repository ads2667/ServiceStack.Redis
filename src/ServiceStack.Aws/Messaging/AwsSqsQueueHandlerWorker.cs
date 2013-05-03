using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
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
    public class AwsSqsQueueHandlerWorker : QueueHandlerBackgroundWorker
    {
        public ISqsClient SqsClient { get; private set; }
        public IMessageCoordinator MessageCoordinator { get; private set; }
        public Type MessageType { get; private set; }
        
        private readonly ILog Log = LogManager.GetLogger(typeof (AwsSqsQueueHandlerWorker));

       //  private bool listenForMessages = true;

        // private readonly object syncLock = new object();

        public AwsSqsQueueHandlerWorker(
            ISqsClient sqsClient, 
            IMessageCoordinator messageCoordinator, 
            Type messageType, 
            string queueName,
            string queueUrl,
            Action<IQueueHandlerBackgroundWorker, Exception> errorHandler,
            int waitTimeInSeconds,
            decimal maxNumberOfMessages,
            decimal messageVisibilityTimeout)
            : base(queueName, errorHandler)
        {
            if (sqsClient == null) throw new ArgumentNullException("sqsClient");
            if (messageCoordinator == null) throw new ArgumentNullException("messageCoordinator");
            if (messageType == null) throw new ArgumentNullException("messageType");
            if (waitTimeInSeconds > 20)
            {
                throw new ArgumentException("WaitTimeInSeconds must be a value between 0 and 20 seconds.", "waitTimeInSeconds");
            }

            if (maxNumberOfMessages < 1)
            {
                throw new ArgumentException("MaxNumberOfMessages must be a minimum value of 1.", "maxNumberOfMessages");
            }

            this.SqsClient = sqsClient;
            this.MessageCoordinator = messageCoordinator;
            this.MessageType = messageType;
            this.QueueName = queueName; // queue.Key;
            this.QueueUrl = queueUrl; // queue.Value;
            this.WaitTimeInSeconds = waitTimeInSeconds;
            this.MaxNumberOfMessages = maxNumberOfMessages;
            this.MessageVisibilityTimeout = messageVisibilityTimeout;
        }

        public decimal MaxNumberOfMessages { get; private set; }
        public decimal MessageVisibilityTimeout { get; private set; }

        public int WaitTimeInSeconds { get; private set; }

        // protected System.Collections.Concurrent.ConcurrentQueue<Amazon.SQS.Model.Message> LocalMessageQueue { get; private set; }

        public string QueueUrl { get; private set; }

        // public string QueueName { get; private set; }

        /*
        protected override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this._client, this.MessageCoordinator, this.MessageCoordinator.QueueUrls, null);
        }
        */

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
        
        protected override void RunLoop()
        {
            // var continuePolling = true;
            // Use Long-Polling to retrieve messages from a specific SQS queue.      

            // do
            while (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)            
            {
                Log.DebugFormat("Polling SQS Queue '{0}' for messages.", this.QueueName);

                var response = this.SqsClient.ReceiveMessage(this.QueueUrl, this.WaitTimeInSeconds, this.MaxNumberOfMessages, this.MessageVisibilityTimeout); // Blocks until timout, or msg received
                if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message.Count > 0)
                {
                    // TODO: Reset noOfContinuousErrors, to notify we had a successful run...

                    // Place the item in the ready to process queue, and notify workers that a new msg has arrived.                    
                    Log.DebugFormat("Received {0} Message(s) from Queue '{1}'.", response.ReceiveMessageResult.Message.Count, this.QueueName);

                    // Thread-safe local queue -> Provides access to msg reponse, and retrieving multiple msgs!
                    foreach (var message in response.ReceiveMessageResult.Message)
                    {
                        // NOTE: We are not deleting anything, so it will continue to get the same messages
                        // Remember: Need to code so no problem if same message received twice... consider cache?
                        // TODO: Will need to create a seperate queue for each queue litener...
                        // this.LocalMessageQueue.Enqueue(this.QueueName, message);

                        // TODO: Need to verify Message MD5, and use 'IsSet()' methods to ensure the msg is valid. Earlier in process.

                        

                        /***** CODE: if the 'MessageHandler' class could be modified to prevent the need to subclass Message Types *****
                        // Use reflection to call the extension method
                        var awsSqsMessage = typeof(AwsSqsMessage<>);
                        var typedAwsSqsMessageType = awsSqsMessage.MakeGenericType(this.MessageType);
                        var typedAwsSqsMessage = (IAwsSqsMessage)Activator.CreateInstance(typedAwsSqsMessageType);
                        
                        // Invoke the method                                                
                        var messageExtensions = typeof (MessageExtensions);
                        var extensionMethod = messageExtensions.GetMethods(BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.Public);
                        MethodInfo method = null;
                        foreach (var methodInfo in extensionMethod)
                        {
                            if (methodInfo.Name == "ToMessage" && methodInfo.IsGenericMethod)
                            {
                                method = methodInfo;
                                break;
                            }
                        }

                        
                        var typedExtensionMethod = method.MakeGenericMethod(this.MessageType);
                        var typedMessage = typedExtensionMethod.Invoke(messageBytes, BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.Public, null, new object[] { messageBytes }, Thread.CurrentThread.CurrentCulture);

                        typedAwsSqsMessageType.InvokeMember("FromMessage", BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.Public, null, typedAwsSqsMessage, new object[] { typedMessage });
                        typedAwsSqsMessage.MessageId = message.MessageId;
                        typedAwsSqsMessage.ReceiptHandle = message.ReceiptHandle;
                        typedAwsSqsMessage.QueueUrl = this.QueueUrl;
                        */
                        var messageBytes = Convert.FromBase64String(message.Body);

                        // =====================
                        var m1 = messageBytes.ToMessage(this.MessageType);
                        var sqsMessage = m1.Body as ISqsMessage;
                        if (sqsMessage != null)
                        {
                            sqsMessage.MessageId = message.MessageId;
                            sqsMessage.ReceiptHandle = message.ReceiptHandle;
                            sqsMessage.QueueUrl = this.QueueUrl;
                            sqsMessage.QueueName = this.QueueName;
                            sqsMessage.VisibilityTimeout = this.MessageVisibilityTimeout;
                            // TODO: Assign the message visibility timeout
                        }
                        else
                        {
                            Log.WarnFormat("The message type '{0}' is using the AwsSqsQueueHandler, but does not implement 'ISqsMessage'. No AwsSqsMessageHandlerRegister pre,post or error message handlers will be executed.", this.MessageType.Name);
                        }
                        // =====================
                        
                        this.MessageCoordinator.EnqueMessage(this.QueueName, m1 /*sqsMessagetypedAwsSqsMessage message*/, this.MessageType);
                        this.IncrementMessageCount(1);
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
                }

                /*
                // For testing, delete here to see what happens
                // AwsQueingService.DeleteMessage(client, this.GetQueueNameOrUrl(queueName), response.ReceiveMessageResult.Message[0].ReceiptHandle);                                       
                lock (syncLock)
                {
                    continuePolling = listenForMessages;
                }*/
            }  // while (continuePolling);
          
            Log.DebugFormat("Stopped polling SQS queue: {0}", this.QueueName);
            
            /*
            WaitHandle.WaitAll()
            if (Monitor.TryEnter(disposeLock))
            {
                Monitor.Pulse(disposeLock);
                Monitor.Exit(disposeLock);
            }
            else
            {
                canDispose = true;
            }*/
            /*
            lock (syncLock)
            {
                Monitor.Pulse(syncLock);
            }*/
        }

        /*
        protected override void OnStop()
        {            
            lock (syncLock)
            {                
                listenForMessages = false;
                Monitor.Wait(syncLock); // TODO: Add Timeout.
            }
        }*/

        public override IQueueHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new AwsSqsQueueHandlerWorker(this.SqsClient, this.MessageCoordinator, this.MessageType, this.QueueName, this.QueueUrl, this.ErrorHandler, this.WaitTimeInSeconds, this.MaxNumberOfMessages, this.MessageVisibilityTimeout);
        }
    }
}
