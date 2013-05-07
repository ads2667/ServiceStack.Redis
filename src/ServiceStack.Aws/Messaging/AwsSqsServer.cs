using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsServer : MqServer2<AwsSqsHandlerConfiguration, AwsSqsMessageHandlerRegister, AwsSqsBackgroundWorkerFactory> /*<AwsSqsMessageHandlerRegister>*/, IMessageCoordinator
    {        
        public AwsSqsServer(ISqsClient sqsClient, int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null, decimal maxNumberOfMessagesToReceivePerRequest = 10, decimal messageVisibilityTimeout = 10)
            : base(null, retryCount) //// Default to use Long Polling
        {
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }

            this.SqsClient = sqsClient;
            this.RequestTimeOut = requestTimeOut.HasValue ? requestTimeOut.Value : TimeSpan.FromSeconds(20);
            this.MaxNumberOfMessagesToReceivePerRequest = maxNumberOfMessagesToReceivePerRequest;
            this.MessageVisibilityTimeout = messageVisibilityTimeout;
            this.QueueUrls = new Dictionary<string, string>();
        }

        public ISqsClient SqsClient { get; private set; }
        public TimeSpan RequestTimeOut { get; private set; }
        public decimal MaxNumberOfMessagesToReceivePerRequest { get; private set; }
        public decimal MessageVisibilityTimeout { get; private set; }

        public override IMessageQueueClient CreateMessageQueueClient()
        {
            return new AwsSqsMessageQueueClient(this.SqsClient, this, this.QueueUrls, null);
        }

        protected override AwsSqsBackgroundWorkerFactory CreateBackgroundWorkerFactory()
        {
            return new AwsSqsBackgroundWorkerFactory(this);
        }

        public override void Dispose()
        {
            // Wait for all ThreadPool handlers to execute
            Task.WaitAll(taskList.ToArray());

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
                if (this.MessageHandlerRegister.RegisteredHandlers.Count == 0)
                {
                    throw new InvalidOperationException("No Message Handlers have been configured. Clients can not be created until one or more message handlers have been registered.");
                }
                
                // TODO: Change CTOR code to remove or null MessageFactory param, remember Redis requirements.
                return messageFactory ?? (messageFactory = new AwsSqsMessageFactory(this.SqsClient, this, this.QueueUrls));
            }
        }

        public IDictionary<string, string> QueueUrls { get; private set; }

        protected override AwsSqsMessageHandlerRegister CreateMessageHandlerRegister()
        {
            return new AwsSqsMessageHandlerRegister(this, this.SqsClient);
        }

        // public override void RegisterMessageHandlers(Action<AwsSqsMessageHandlerRegister> messageHandlerRegister)
        public override void RegisterMessageHandlers(Action<AwsSqsMessageHandlerRegister> messageHandlerRegister)
        {
            base.RegisterMessageHandlers(messageHandlerRegister);
            
            // For Amazon SQS we need to get the Url for each registered message handler.
            foreach (var handlerConfig in this.MessageHandlerRegister.RegisteredHandlers)
            {
                var queueNamesToCreate = this.GetNewQueueNames(handlerConfig.Key);
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

            // Ensure all of the required response queues are configured.
            foreach (var responseMessageType in this.MessageHandlerRegister.ResponseMessageTypes)
            {
                var queueNamesToCreate = this.GetNewQueueNames(responseMessageType);
                foreach (var newQueueName in queueNamesToCreate)
                {
                    // NOTE: No local queue is required for response queues, as the server does not monitor these.
                    var queueUrl = this.SqsClient.GetOrCreateQueueUrl(newQueueName);
                    this.QueueUrls.Add(newQueueName, queueUrl);
                }
            }
        }
        
        /*
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
        */

        private IList<string> GetNewQueueNames(Type messageType)
        {            
            // Use reflection to call the extension method
            var queueNames = typeof(VersionedQueueNames);
            var properties = queueNames.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            var newQueueNames = new List<string>();
            var queueNamesInstance = new VersionedQueueNames(messageType);
            foreach (var property in properties)
            {
                if (property.PropertyType == typeof(string))
                {
                    newQueueNames.Add((string)queueNames.InvokeMember(property.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty, null, queueNamesInstance, null));
                }
            }

            return newQueueNames;
        }

        // TODO: Refactor local queue management into its own class? Or move to ISqsClient?

        private IDictionary<string, Queue<IMessage>> localMessageQueues = new Dictionary<string, Queue<IMessage>>();
        
        // TODO: Move threadpool code to base class so it can be re-used.
        private void ExecuteUsingThreadPool(object obj)
        {
            var threadPoolTask = obj as MessageReceivedArgs;
            if (threadPoolTask == null)
            {
                // TODO: Log, throw ex? We should never get here.
                return;
            }

            // TODO: On the thread, create a handler and process the message.
            Log.DebugFormat("Executing message {0} using thread pool", threadPoolTask.MessageId);
            using (var client = this.CreateMessageQueueClient())
            {
                threadPoolHandlers[threadPoolTask.MessageType].ProcessQueue(client, threadPoolTask.QueueName, () => false);
            }
        }        

        IList<Task> taskList = new List<Task>();

        public void EnqueMessage(string queueName, IMessage message, Type messageType)
        {
            // Add the message to the local queue.
            lock (localMessageQueues)
            {
                localMessageQueues[queueName].Enqueue(message);
            }      
        }

        public IMessage DequeMessage(string queueName)
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

        public override void NotifyMessageReceived(MessageReceivedArgs messageReceivedArgs)
        {
            var handlerThreadCount = this.MessageHandlerRegister.RegisteredHandlers[messageReceivedArgs.MessageType].Configuration.NoOfThreads;

            if (handlerThreadCount == 0) //// 0 => Threadpool
            {
                Log.DebugFormat("Assigning message {0} to thread pool", messageReceivedArgs.MessageId);

                // ** TODO: Create Factory that creates tasks to assign to threadpool - based on queue name.

                var task = new Task(ExecuteUsingThreadPool, messageReceivedArgs, TaskCreationOptions.PreferFairness);
                task.ContinueWith((t) =>
                {
                    lock (taskList)
                    {
                        // TODO: Should this only be removed if executed successfully, w/out ex or cancellation?
                        taskList.Remove(t);
                    }
                }); // Must remove the task after it's been executed. Otherwise, we'll be waiting forever.

                lock (taskList)
                {
                    taskList.Add(task);
                    // Need to track all tasks, enables WaitAll functionality to close gracefully.
                }

                // TODO: ************ NEED TO ADD MESSAGE VISIBILITY TIMEOUT TO MESSAGE **************

                task.Start(); // Executes async using threadpool.
            }
            else
            {
                // Static Thread                
                base.NotifyMessageReceived(messageReceivedArgs);
            }                   
        }
    }
}
