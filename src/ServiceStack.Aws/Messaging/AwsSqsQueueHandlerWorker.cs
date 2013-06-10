using System;
using System.Threading;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    /// <summary>
    /// Defines a class that receives messages from a queue, and notifies worker threads that a new message has arrived.
    /// </summary>
    public class AwsSqsQueueHandlerWorker : QueueHandlerBackgroundWorker
    {
        public ISqsClient SqsClient { get; private set; }
        public AwsSqsServer MqServer { get; private set; }
        public IMessageCoordinator MessageCoordinator { get; private set; }
        public Type MessageType { get; private set; }
        
        public AwsSqsQueueHandlerWorker(
            ISqsClient sqsClient, 
            // IMessageCoordinator messageCoordinator, 
            AwsSqsServer sqsServer,
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
            if (sqsServer == null) throw new ArgumentNullException("sqsServer");
            // if (messageCoordinator == null) throw new ArgumentNullException("messageCoordinator");
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
            this.MqServer = sqsServer;
            this.MessageCoordinator = sqsServer; // messageCoordinator;
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

        public string QueueUrl { get; private set; }

        protected override void RunLoop()
        {
            // Use Long-Polling to retrieve messages from a specific SQS queue.      
            while (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)            
            {
                Log.DebugFormat("Polling SQS Queue '{0}' for messages.", this.QueueName);

                var response = this.SqsClient.ReceiveMessage(this.QueueUrl, this.WaitTimeInSeconds, this.MaxNumberOfMessages, this.MessageVisibilityTimeout, "ApproximateReceiveCount"); // Blocks until timout, or msg received
                if (response.IsSetReceiveMessageResult() && response.ReceiveMessageResult.Message.Count > 0)
                {
                    // Place the item in the ready to process queue, and notify workers that a new msg has arrived.                    
                    Log.DebugFormat("Received {0} Message(s) from Queue '{1}'.", response.ReceiveMessageResult.Message.Count, this.QueueName);

                    // Thread-safe local queue -> Provides access to msg reponse, and retrieving multiple msgs!
                    foreach (var sqsMessage in response.ReceiveMessageResult.Message)
                    {
                        // TODO: Remember; Need to code so no problem if same message received twice... consider cache?
                        // TODO: Need to verify Message MD5, and use 'IsSet()' methods to ensure the msg is valid. Earlier in process.
                        if (sqsMessage.IsSetMD5OfBody() && !MessageSecurity.IsMd5Valid(sqsMessage.Body, sqsMessage.MD5OfBody))
                        {
                            Log.WarnFormat("Message '{0}' has invalid MD5 signature, will not be processed.");
                            continue;                            
                        }


                        var messageBytes = Convert.FromBase64String(sqsMessage.Body);
                        
                        // Set standard message properties
                        var message = messageBytes.ToMessage(this.MessageType);
                        if (sqsMessage.IsSetAttribute() && sqsMessage.Attribute.Count > 0)
                        {
                            foreach (var attribute in sqsMessage.Attribute)
                            {
                                if (attribute.Name == "ApproximateReceiveCount")
                                {
                                    message.RetryAttempts = int.Parse(attribute.Value);    
                                    break;
                                }
                            }                            
                        }

                        // Set SQS message properties
                        var sqsMessageBody = message.Body as ISqsMessage;
                        if (sqsMessageBody != null)
                        {
                            sqsMessageBody.MessageId = sqsMessage.MessageId;
                            sqsMessageBody.ReceiptHandle = sqsMessage.ReceiptHandle;
                            sqsMessageBody.QueueUrl = this.QueueUrl;
                            sqsMessageBody.QueueName = this.QueueName;
                            sqsMessageBody.VisibilityTimeout = this.MessageVisibilityTimeout;
                            message.RetryAttempts += sqsMessageBody.PreviousRetryAttempts;
                        }
                        else
                        {
                            Log.WarnFormat("The message type '{0}' is using the AwsSqsQueueHandler, but does not implement 'ISqsMessage'. No AwsSqsMessageHandlerRegister pre,post or error message handlers will be executed.", this.MessageType.Name);
                        }
                        // =====================
                        
                        this.MessageCoordinator.EnqueMessage(this.QueueName, message, this.MessageType);
                        this.IncrementMessageCount(1);

                        var messageReceivedArgs = new MessageReceivedArgs { MessageType = this.MessageType, QueueName = this.QueueName, MessageId = sqsMessage.MessageId };
                        this.MqServer.NotifyMessageReceived(messageReceivedArgs);
                    }
                }
            }
          
            Log.DebugFormat("Stopped polling SQS queue: {0}", this.QueueName);            
        }

        public override IQueueHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new AwsSqsQueueHandlerWorker(this.SqsClient, this.MqServer, this.MessageType, this.QueueName, this.QueueUrl, this.ErrorHandler, this.WaitTimeInSeconds, this.MaxNumberOfMessages, this.MessageVisibilityTimeout);
        }
    }
}
