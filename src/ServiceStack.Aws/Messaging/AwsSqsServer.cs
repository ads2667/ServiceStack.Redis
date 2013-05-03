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

    public interface IMessageCoordinator
    {
        void EnqueMessage(string queueName, IMessage message, Type messageType);
        
        IMessage DequeMessage(string queueName);
    }

    // TODO: Create a ISqsClient impl that wraps the amazon client object and exposes methods, which also accepts amazon msg objects as input/output
    // TODO: Make the MQs Versioned using the Assmbly qualified name!?
    // ThreadPool Handler
    // Parameterize SQS polling times, visibility, etc...
    // Graceful termination of threads
    public class AwsSqsServer : MqServer2, IMessageCoordinator
    {        
        public AwsSqsServer(ISqsClient sqsClient, int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null, decimal maxNumberOfMessagesToReceivePerRequest = 10, decimal messageVisibilityTimeout = 10)
            : base(null, requestTimeOut.HasValue ? requestTimeOut.Value : TimeSpan.FromSeconds(20), retryCount) //// Default to use Long Polling
        {
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }

            this.SqsClient = sqsClient;
            this.MaxNumberOfMessagesToReceivePerRequest = maxNumberOfMessagesToReceivePerRequest;
            this.MessageVisibilityTimeout = messageVisibilityTimeout;
            this.QueueUrls = new Dictionary<string, string>();
        }

        public ISqsClient SqsClient { get; private set; }
        public decimal MaxNumberOfMessagesToReceivePerRequest { get; private set; }
        public decimal MessageVisibilityTimeout { get; private set; }

        public override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this.SqsClient, this, this.QueueUrls, null);
        }
       
        protected override IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
        {
            return new AwsSqsMessageHandlerWorker(this.SqsClient, this, this.QueueUrls, messageHandler, queueName, errorHandler);
        }
        
        protected override IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
        {
            var queueHandlers = new List<IQueueHandlerBackgroundWorker>();
            foreach (var queue in messageQueueNames)
            {
                queueHandlers.Add(new AwsSqsQueueHandlerWorker(this.SqsClient, this, queue.Value, queue.Key, this.QueueUrls[queue.Key], errorHandler, Convert.ToInt32(this.RequestTimeOut.Value.TotalSeconds), this.MaxNumberOfMessagesToReceivePerRequest, this.MessageVisibilityTimeout));
            }

            return queueHandlers;
        }

        public override void Dispose()
        {
            base.Dispose();
            if (this.SqsClient != null)
            {
                this.SqsClient.Dispose();
            }
        }

        private IMessageFactory messageFactory = null;
        public override IMessageFactory MessageFactory
        {
            get
            {
                if (this.handlerMap.Count == 0)
                {
                    throw new InvalidOperationException("No Message Handlers have been configured. Clients can not be created until one or more message handlers have been registered.");
                }
                
                // TODO: Change CTOR code to remove or null MessageFactory param, remember Redis requirements.
                return messageFactory ?? (messageFactory = new AwsSqsMessageFactory(this.SqsClient, this, this.QueueUrls));
            }
        }

        public IDictionary<string, string> QueueUrls { get; private set; }

        protected override MessageHandlerRegister CreateMessageHandlerRegister()
        {
            return new AwsSqsMessageHandlerRegister(this, this.SqsClient);
        }

        public override void RegisterMessageHandlers(Action<MessageHandlerRegister> messageHandlerRegister)
        {
            base.RegisterMessageHandlers(messageHandlerRegister);

            // For Amazon SQS we need to get the Url for each registered message handler.
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
                    var queueUrl = this.SqsClient.GetOrCreateQueueUrl(newQueueName);
                    this.QueueUrls.Add(newQueueName, queueUrl);
                }                
            }
        }
        
        
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

        // TODO: Refactor local queue management into its own class? Or move to ISqsClient?

        // private object threadLock = new object();
        private IDictionary<string, Queue<IMessage>> localMessageQueues = new Dictionary<string, Queue<IMessage>>();
        // private IDictionary<string, Queue<IAwsSqsMessage>> localMessageQueues = new Dictionary<string, Queue<IAwsSqsMessage>>(); 

        // private Queue<Message> queue = new Queue<Message>(); 
        public void EnqueMessage(string queueName, IMessage /*IAwsSqsMessage*/ message, Type messageType)
        {
            // TODO: If ThreadPooling is to be supported, this code block will need to support both methods
                // For threadPooling; this would involve adding a new [TASK] to the threadpool. Create contained class.
                // Would need to get the HANDLER TYPE, based on the queue, and queue the thread. Use LocalMQ?
                // TODO: Need to manage msg timeouts/retries and multiple processing of msg's. => Wrap in msg obj?
               
            // TODO: Support mix/match Pooled and Static thread handlers.
            var handlerThreadCount = handlerThreadCountMap[messageType];

            if (handlerThreadCount == 0) //// 0 => Threadpool
            {         
                throw new NotImplementedException("Threadpool support not implemented.");
                /*
                // The handler is registered to use the thread pool.
                // TODO: Create a new task, using the message handler, and add it to the thread pool
                // TODO: Can pass MQ Name with EnqueMessage method.
                    
                // ** TODO: Create Factory that creates tasks to assign to threadpool - based on queue name.
                var task = new Task(, , TaskCreationOptions.PreferFairness);
                var taskList = new List<Task>();
                lock (taskList)
                {
                    taskList.Add(task);
                        // Need to track all tasks, enables WaitAll functionality to close gracefully.
                }
                    
                // TODO: ************ NEED TO ADD MESSAGE VISIBILITY TIMEOUT TO MESSAGE **************

                task.Start(); // Executes async using threadpool.
                // Task.WaitAll(taskList); // Example of WaitAll
                task.ContinueWith((t) =>
                    {
                        lock (taskList)
                        {
                            taskList.Remove(t);
                        }
                    }); // Must remove the task after it's been executed. Otherwise, we'll be waiting forever.

                // TODO: On the thread, create a handler and process the message.
                this.handlerMap[typeof(object)].CreateMessageHandler().ProcessQueue(, queueName, null);
                // throw new NotImplementedException("ThreadPool support");
                */
            }
            else
            {
                // Static Thread
                lock (localMessageQueues)
                {
                    localMessageQueues[queueName].Enqueue(message);
                }

                // Notify the static threads that there's messages to process.
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
