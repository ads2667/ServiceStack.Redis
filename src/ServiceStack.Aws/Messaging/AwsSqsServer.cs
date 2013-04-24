using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{

    public class AwsSqsServer : MqServer2
    {
        private readonly AmazonSQS client;

        public AwsSqsServer(Amazon.SQS.AmazonSQS sqsClient, int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null)
            : base(null, retryCount, requestTimeOut)
        {
            this.QueueUrls = new Dictionary<string, string>();
            client = sqsClient;
        }

        public override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(client, this.QueueUrls, null);
        }
        

        ConcurrentQueue<object> localQueue = new ConcurrentQueue<object>(); 

        IDictionary<string, AwsSqsQueueHandlerWorker> queueWorkers = new Dictionary<string, AwsSqsQueueHandlerWorker>();

        protected void InitMessageQueueHandlerWorkers()
        {
            // TODO: Ensure only 1 -thread can enter here

            // TODO: Do not listen to dead letter queues
            // TODO: Do not listen to 'OUT' queues, these should be monitored by clients - not the server. 
            foreach (var queue in this.QueueUrls)
            {                
                var queueHandlerWorker = new AwsSqsQueueHandlerWorker(client, queue);
                // Amazon Client? or a factory? Post to see if it is thread-safe!!
                // Queue URL
                // Local Queue
                // Access to worker(s)...
                
                queueWorkers.Add(queue.Key, queueHandlerWorker);
                /*
                // Add to dictionary/list, then start. Keep reference for stopping.
                var t = new Thread(queueHandlerWorker.Start)
                    {
                        IsBackground = true
                    };
                t.Start();
                */
            }
        }

        protected override void ProcessMessages()
        {
            // TODO: Look @ Redis implementation.
            // Does AWS provide transient Queues?

            // TODO: Move to 'START' method...
            InitMessageQueueHandlerWorkers();

            foreach (var queueHandlerWorker in this.queueWorkers.Values)
            {
                // Add to dictionary/list, then start. Keep reference for stopping.
                var t = new Thread(queueHandlerWorker.Start)
                    {
                        IsBackground = true
                    };
                Log.DebugFormat("Starting queue handler worker '{0}' on thread {1}.", queueHandlerWorker.QueueName, t.ManagedThreadId);
                t.Start();
            }

            /*
            lock (threadLock)
            {
                Monitor.Wait(threadLock);    
            }
            */

            Log.DebugFormat("Shutting down server...");
            Log.DebugFormat("Shutting down background threads.");
            // Thread.CurrentThread.Join();
            // TODO: Need to block the thread until we need to stop processing...

            /*
            foreach (var queueUrl in this.QueueUrls)
            {
                // Create a new thread for each MQ; use one thread per MQ to perform long polling...

                // if queue gets a stop command, need to stop the thread running.
            }

           */
            // throw new NotImplementedException();
        }

        private readonly object threadLock = new object();

        protected override void StopListeningToMessages()
        {
            // TODO: Unsubsribe from the queue, removing any listeners.
            /*
            lock (threadLock)
            {
                Monitor.Pulse(threadLock);
            }
            */
            // throw new NotImplementedException();
        }

        protected override MessageHandlerWorker CreateMessageHandlerWorker(IMessageHandler messageHandler, string queueName, Action<MessageHandlerWorker, Exception> errorHandler)
        {
           return new AwsSqsMessageHandlerWorker(client, this.QueueUrls, messageHandler, queueName, errorHandler);
        }

        public override void Dispose()
        {
            base.Dispose();
            if (client != null)
            {
                client.Dispose();
            }
        }

        private IMessageFactory messageFactory = null;
        public override IMessageFactory MessageFactory
        {
            get
            {
                // TODO: Throw EX if Handlers are not already configured.
                // TODO: Change CTOR code to remove or null MessageFactory param, remember Redis requirements.
                return messageFactory ?? (messageFactory = new AwsSqsMessageFactory(client, this.QueueUrls));
            }
        }

        public IDictionary<string, string> QueueUrls { get; private set; }

        public override void RegisterMessageHandlers(Action<MessageHandlerRegister> messageHandlerRegister)
        {
            base.RegisterMessageHandlers(messageHandlerRegister);

            // Get the Url for each registered message handler.
            foreach (var handler in this.handlerMap)
            {
                // Get all queue names
                // QueueNames<T>.Out
                
                // for (var priority = 0; priority <= 1; priority++)
                var queueNamesToCreate = this.GetNewQueueNames(handler.Key);
                foreach (var newQueueName in queueNamesToCreate)
                {
                    // var queueName = this.GetQueueName(handler.Key, priority);
                    var queueUrl = this.GetQueueUrl(newQueueName);
                    this.QueueUrls.Add(newQueueName, queueUrl);
                }                
            }

            //mq:Hello.outq
            
            // Remove the local queues
            this.amazonQueueUrls.Clear();
        }

        private IList<string> amazonQueueUrls = null;

        private string GetQueueName(Type messageType, long priority)
        {
            // TODO: Use assembly qualified name for versioned queue's?
            var stronglyTypedMessage = typeof(Message<>).MakeGenericType(messageType);
            var messageInstance = (IMessage)Activator.CreateInstance(stronglyTypedMessage);
            messageInstance.Priority = priority;

            // Use reflection to call the extension method
            var methods = typeof(MessageExtensions).GetMethods(BindingFlags.Static | BindingFlags.Public).Where(mi => mi.Name == "ToInQueueName");

            MethodInfo method = null;
            foreach (var methodInfo in methods)
            {
                var paramType = methodInfo.GetParameters()[0].ParameterType;
                if (paramType.GetGenericArguments().Count() == 1)
                {
                    // we are looking for  Func<TSource, bool>, the other has 3
                    method = methodInfo;
                }
            }

            if (method == null)
            {
                throw new NullReferenceException("Could not find 'ToInQueueName' extension method.");
            }

            // Execute the extension method
            method = method.MakeGenericMethod(messageType);
            return (string)method.Invoke(messageInstance, new object[] { messageInstance });            
        }

        private IList<string> GetNewQueueNames(Type messageType)
        {
            // TODO: Use assembly qualified name for versioned queue's?
            var stronglyTypedMessage = typeof(Message<>).MakeGenericType(messageType);
            var messageInstance = (IMessage)Activator.CreateInstance(stronglyTypedMessage);

            // Use reflection to call the extension method
            var queueNames = typeof(QueueNames<>);
            var properties = queueNames.GetProperties(BindingFlags.Static | BindingFlags.Public);

            var newQueueNames = new List<string>();    
            var queueNamesInstance = queueNames.MakeGenericType(messageType);
            foreach (var property in properties)
            {
                if (property.PropertyType == typeof (string))
                {                
                    newQueueNames.Add((string)queueNamesInstance.InvokeMember(property.Name, BindingFlags.Public | BindingFlags.Static | BindingFlags.GetProperty, null, queueNamesInstance, null));
                }
            }

            return newQueueNames;
        }

        private static readonly Regex QueueNameRegex = new Regex("[:\\.]");

        private string GetQueueUrl(string queueName)
        {
            // Clean the queue name
            queueName = QueueNameRegex.Replace(queueName, "_");

            // Get a list of queues from SQS
            if (amazonQueueUrls == null)
            {
                amazonQueueUrls = AwsQueingService.GetAllQueueNames(client);
            }

            // Check if the queue exists
            foreach (var queueUrl in amazonQueueUrls)
            {
                if (queueUrl.Substring(queueUrl.LastIndexOf('/') + 1).ToUpperInvariant() == queueName.ToUpperInvariant())
                {
                    return queueUrl;
                }
            }

            return AwsQueingService.CreateMessageQueue(client, queueName);
        }
    }
}
