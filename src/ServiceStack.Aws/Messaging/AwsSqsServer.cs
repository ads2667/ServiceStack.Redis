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
using Message = Amazon.SQS.Model.Message;

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
            return new AwsSqsMessageQueueClient(client, this, this.QueueUrls, null);
        }
       
        // ConcurrentQueue<object> localQueue = new ConcurrentQueue<object>(); 

        IDictionary<string, AwsSqsQueueHandlerWorker> queueWorkers = new Dictionary<string, AwsSqsQueueHandlerWorker>();

        protected override IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
        {
           return new AwsSqsMessageHandlerWorker(client, this, this.QueueUrls, messageHandler, queueName, errorHandler);
        }
        
        protected override IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
        {
            var queueHandlers = new List<IQueueHandlerBackgroundWorker>();
            foreach (var queue in messageQueueNames)
            {
                queueHandlers.Add(new AwsSqsQueueHandlerWorker(this.client, this, queue.Value, new KeyValuePair<string, string>(queue.Key, this.QueueUrls[queue.Key]), errorHandler));
            }

            return queueHandlers;
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
                return messageFactory ?? (messageFactory = new AwsSqsMessageFactory(client, this, this.QueueUrls));
            }
        }

        public IDictionary<string, string> QueueUrls { get; private set; }

        protected override MessageHandlerRegister CreateMessageHandlerRegister()
        {
            return new AwsSqsMessageHandlerRegister(this, client);
        }

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
                    // Init the local queue
                    // localMessageQueues.Add(newQueueName, new Queue<IAwsSqsMessage>());
                    localMessageQueues.Add(newQueueName, new Queue<IMessage>());

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
            // var messageInstance = (IMessage)Activator.CreateInstance(stronglyTypedMessage);

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
        
        // private object threadLock = new object();
        private IDictionary<string, Queue<IMessage>> localMessageQueues = new Dictionary<string, Queue<IMessage>>();
        // private IDictionary<string, Queue<IAwsSqsMessage>> localMessageQueues = new Dictionary<string, Queue<IAwsSqsMessage>>(); 

        // private Queue<Message> queue = new Queue<Message>(); 
        public void EnqueMessage(string queueName, IMessage /*IAwsSqsMessage*/ message)
        {
            lock (localMessageQueues)
            {
                localMessageQueues[queueName].Enqueue(message);

                // TODO: If ThreadPooling is to be supported, this code block will need to support both methods
                // For threadPooling; this would involve adding a new [TASK] to the threadpool. Create contained class.
                // Would need to get the HANDLER TYPE, based on the queue, and queue the thread. Use LocalMQ?
                // TODO: Need to manage msg timeouts/retries and multiple processing of msg's. => Wrap in msg obj?

                // TODO: Support mix/match Pooled and Static thread handlers.
                int[] workerIndexes;
                if (queueWorkerIndexMap.TryGetValue(queueName, out workerIndexes))
                {
                    foreach (var workerIndex in workerIndexes)
                    {
                        messageWorkers[workerIndex].NotifyNewMessage();
                    }
                }
            }
        }

        public /*IAwsSqsMessage*/ IMessage DequeMessage(string queueName)
        {
            lock (localMessageQueues)
            {
                if (localMessageQueues[queueName].Count > 0)
                {
                    return localMessageQueues[queueName].Dequeue();
                }

                return null;
            }
        }
    }
}
