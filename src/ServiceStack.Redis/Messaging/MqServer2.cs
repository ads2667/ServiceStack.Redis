using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Common;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging.Redis;
using ServiceStack.Service;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{
    /*
    public class MessageHandler<T>
    {
        public Func<IMessage<T>, object> ProcessMessageFn { get; set; }
        public Action<IMessage<T>, Exception> ProcessExceptionEx { get; set; }

        public MessageHandler(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            // TODO: Check for null
            ProcessMessageFn = processMessageFn;
            ProcessExceptionEx = processExceptionEx;
        }
    }
    */
   

    public class MessageHandlerRegister
    {
        protected MqServer2 MessageServer { get; private set; }

        protected ILog Log;

        public MessageHandlerRegister(MqServer2 messageServer)
        {
            if (messageServer == null) throw new ArgumentNullException("messageServer");
            Log = LogManager.GetLogger(this.GetType());
            MessageServer = messageServer;
        }

        public void AddHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            AddHandler(processMessageFn, null, noOfThreads: 1);
        }

        public void AddHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads)
        {
            AddHandler(processMessageFn, null, noOfThreads);
        }

        public void AddHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            AddHandler(processMessageFn, processExceptionEx, noOfThreads: 1);
        }

        public void AddHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {            
            // MessageServer.RegisterHandler(processMessageFn, processExceptionEx, noOfThreads);           
            AddMessageHandler(processMessageFn, processExceptionEx, noOfThreads);
        }

        protected virtual void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            MessageServer.RegisterHandler(processMessageFn, processExceptionEx, noOfThreads);
        }

        public void AddPooledHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            AddPooledHandler(processMessageFn, null);
        }

        public void AddPooledHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            AddPooledMessageHandler(processMessageFn, processExceptionEx);
        }

        protected virtual void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            // A thread count of 0, indicates that the handler should use the thread pool
            MessageServer.RegisterHandler(processMessageFn, processExceptionEx, 0);
        }
    }

    ////public abstract class MqServer2<TMessageHandlerRegister> : MqServer2
    ////    where TMessageHandlerRegister : MessageHandlerRegister
    ////    // where TMessageHandlerBackgroundWorker : IMessageHandlerBackgroundWorker
    ////{
    ////    protected MqServer2(IMessageFactory messageFactory, TimeSpan? requestTimeOut, int retryCount = DefaultRetryCount) : base(messageFactory, requestTimeOut, retryCount)
    ////    {
    ////    }

    ////    protected abstract TMessageHandlerRegister CreateMessageHandlerRegister();
        
    ////    public virtual void RegisterMessageHandlers(Action<TMessageHandlerRegister> messageHandlerRegister)
    ////    {
    ////        messageHandlerRegister.Invoke(this.CreateMessageHandlerRegister());
    ////    }
    ////}

 

    public abstract class MqServer2 : IMessageService
        // where TMessageHandlerBackgroundWorker : IMessageHandlerBackgroundWorker
    {
        protected static ILog Log;
        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

        public int RetryCount { get; protected set; }

        public virtual IMessageFactory MessageFactory { get; private set; }

        public Func<string, IOneWayClient> ReplyClientFactory { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic before a request is processed.
        /// Must be thread-safe.
        /// </summary>
        public Func<IMessage, IMessage> RequestFilter { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic on the response.
        /// Must be thread-safe.
        /// </summary>
        public Func<object, object> ResponseFilter { get; set; }

        /// <summary>
        /// Execute global error handler logic. Must be thread-safe.
        /// </summary>
        public Action<Exception> ErrorHandler { get; set; }

        /// <summary>
        /// If you only want to enable priority queue handlers (and threads) for specific msg types
        /// </summary>
        public Type[] OnlyEnablePriortyQueuesForTypes { get; set; }

        /// <summary>
        /// Don't listen on any Priority Queues
        /// </summary>
        public bool DisableAllPriorityQueues
        {
            set
            {
                OnlyEnablePriortyQueuesForTypes = new Type[0];
            }
        }

        // private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public abstract IMessageQueueClient CreateMessageQueueClient();
        /*
        {
            return new RedisMessageQueueClient(this.clientsManager, null);
        }
        */

        //Stats
        private long timesStarted = 0;
        private long noOfErrors = 0;
        //protected int noOfContinuousErrors = 0;
        private string lastExMsg = null;
        protected int status;

        // private Thread bgThread; //Subscription controller thread
        private long bgThreadCount = 0;
        public long BgThreadCount
        {
            get { return Interlocked.CompareExchange(ref bgThreadCount, 0, 0); }
        }

        /*
        protected readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
            = new Dictionary<Type, IMessageHandlerFactory>();

        protected readonly Dictionary<Type, int> handlerThreadCountMap
            = new Dictionary<Type, int>();
        */

        protected IMessageHandlerBackgroundWorker[] messageWorkers;
        protected IQueueHandlerBackgroundWorker[] queueWorkers;

        protected Dictionary<string, int[]> queueWorkerIndexMap;


        protected internal readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
            = new Dictionary<Type, IMessageHandlerFactory>();

        protected internal readonly Dictionary<Type, int> handlerThreadCountMap
            = new Dictionary<Type, int>();

        public TimeSpan? RequestTimeOut { get; private set; }

        public MqServer2(
            IMessageFactory messageFactory,
            TimeSpan? requestTimeOut,
            int retryCount = DefaultRetryCount)
        {
            Log = LogManager.GetLogger(this.GetType());
            this.RetryCount = retryCount;
            this.RequestTimeOut = requestTimeOut;
            this.MessageFactory = messageFactory; // new RedisMessageFactory(clientsManager);
            this.ErrorHandler = ex => Log.Error("Exception in Redis MQ Server: " + ex.Message, ex);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            RegisterHandler(processMessageFn, null, noOfThreads:1);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads)
        {
            RegisterHandler(processMessageFn, null, noOfThreads);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            RegisterHandler(processMessageFn, processExceptionEx, noOfThreads: 1);
            // throw new NotSupportedException("Register message queue listeners using the 'RegisterMessageHandlers' function.");
        }
        
        internal void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            if (handlerMap.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }
            
            // TODO:? Move to the MsgRegistation Class, and return a dictionary<Type, [T]DTO> Describing the handler
            // TODO:? Create a BackgroundWorkerFactory<[T]Dto> that uses the MsgReg class to create workers?
            handlerMap[typeof(T)] = CreateMessageHandlerFactory(processMessageFn, processExceptionEx);
            handlerThreadCountMap[typeof(T)] = noOfThreads;
        }

        protected virtual MessageHandlerRegister CreateMessageHandlerRegister()
        {
            return new MessageHandlerRegister(this);
        }

        public virtual void RegisterMessageHandlers(Action<MessageHandlerRegister> messageHandlerRegister)
        {
            messageHandlerRegister.Invoke(this.CreateMessageHandlerRegister());
        }

        protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            return new MessageHandlerFactory<T>(this, processMessageFn, processExceptionEx) {
                RequestFilter = this.RequestFilter,
                ResponseFilter = this.ResponseFilter,
                RetryCount = RetryCount,
            };
        }

        protected internal abstract IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(IMessageHandler messageHandler, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler);

        protected internal readonly Dictionary<Type, IMessageHandler> threadPoolHandlers
            = new Dictionary<Type, IMessageHandler>();

        private void Init()
        {
            if (messageWorkers == null)
            {
                var workerBuilder = new List<IMessageHandlerBackgroundWorker>();
                var queuesToMonitor = new Dictionary<string, Type>();
                
                foreach (var entry in handlerMap)
                {
                    var msgType = entry.Key;
                    var handlerFactory = entry.Value;

                    var queueNames = new QueueNames(msgType);
                    var noOfThreads = handlerThreadCountMap[msgType];

                                          
                    if (OnlyEnablePriortyQueuesForTypes == null
                        || OnlyEnablePriortyQueuesForTypes.Any(x => x == msgType))
                    {
                        if (noOfThreads > 0)
                        {
                            noOfThreads.Times(i =>
                                              workerBuilder.Add(this.CreateMessageHandlerWorker(
                                                  handlerFactory.CreateMessageHandler(),
                                                  queueNames.Priority,
                                                  WorkerErrorHandler)));
                        }

                        queuesToMonitor.Add(queueNames.Priority, msgType);
                        /*    
                        new MessageHandlerWorker(
                                clientsManager,
                                handlerFactory.CreateMessageHandler(),
                                queueNames.Priority,
                                WorkerErrorHandler)*/

                    }

                    if (noOfThreads == 0)
                    {
                        threadPoolHandlers.Add(msgType, handlerFactory.CreateMessageHandler());
                    }
                    else
                    {
                        noOfThreads.Times(i =>
                                          workerBuilder.Add(this.CreateMessageHandlerWorker(
                                              handlerFactory.CreateMessageHandler(),
                                              queueNames.In,
                                              WorkerErrorHandler)));
                    }

                    queuesToMonitor.Add(queueNames.In, msgType);
                        /*
                        new MessageHandlerWorker(
                            clientsManager,
                            handlerFactory.CreateMessageHandler(),
                            queueNames.In,
                            WorkerErrorHandler)));
                         * */
                }                

                messageWorkers = workerBuilder.ToArray();

                // Create the background worker thread(s) to monitor message queue(s)
                queueWorkers = this.CreateQueueHandlerWorkers(queuesToMonitor, QueueWorkerErrorHandler).ToArray(); //// New

                queueWorkerIndexMap = new Dictionary<string, int[]>();
                for (var i = 0; i < messageWorkers.Length; i++)
                {
                    var worker = messageWorkers[i];

                    int[] workerIds;
                    if (!queueWorkerIndexMap.TryGetValue(worker.QueueName, out workerIds))
                    {
                        queueWorkerIndexMap[worker.QueueName] = new[] { i };
                    }
                    else
                    {
                        workerIds = new List<int>(workerIds) { i }.ToArray();
                        queueWorkerIndexMap[worker.QueueName] = workerIds;
                    }
                }
            }
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
            {
                //Start any stopped worker threads
                StartWorkerThreads();
                // TODO: Start any stopped queue worker threads.
                return;
            }
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            //Only 1 thread allowed past
            if (Interlocked.CompareExchange(ref status, WorkerStatus.Starting, WorkerStatus.Stopped) == WorkerStatus.Stopped) //Should only be 1 thread past this point
            {
                try
                {
                    Init();

                    if (messageWorkers == null || messageWorkers.Length == 0)
                    {
                        Log.Warn("Cannot start a MQ Server with no Message Handlers registered, ignoring.");
                        Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Starting);
                        return;
                    }

                    /*
                    foreach (var worker in messageWorkers)
                    {
                        worker.Start();
                    }
                    */

                    // TODO: Move this to the Queue Handler
                    // SleepBackOffMultiplier(Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));

                    KillQueueWorkerThreads();

                    // Redis uses a single thread for receiving msgs, but with SQS we need 1 thread per MQ
                    // TODO: Create a thread-safe class used to receive messages from a message queue.
                    // Remember, that after a msg is succesfully received it must be deleted.
                    // Must be BG threads, subscribe only to one queue, and signal to workers when a msg is received
                    // TODO: Configure one Thread to Poll Each MQ, then notify workers that a MSG Is ready to process

                    // TODO: Use the RedisQueueHandlerWorker instead!
                    /*
                    bgThread = new Thread(RunLoop) {
                        IsBackground = true,
                        Name = "Redis MQ Server " + Interlocked.Increment(ref bgThreadCount)
                    };
                    bgThread.Start();
                    Log.Debug("Started Background Thread: " + bgThread.Name);
                    */

                    // Start the worker threads before the MQ listeners, so they're ready to process
                    StartWorkerThreads();

                    if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
                    Interlocked.Increment(ref timesStarted);

                    // Start retrieving messages from the message queue(s)
                    // StartQueueWorkerThreads(); //// New
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                }
            }
        }

        protected abstract IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler);

        /*
        protected abstract void ProcessMessages();

        private void RunLoop()
        {
            if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
            Interlocked.Increment(ref timesStarted);

            try
            {
                this.ProcessMessages();                
            }
            catch (Exception ex)
            {
                lastExMsg = ex.Message;
                Interlocked.Increment(ref noOfErrors);
                Interlocked.Increment(ref noOfContinuousErrors);

                if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Started) != WorkerStatus.Started)
                    Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping);

                StopWorkerThreads();

                if (this.ErrorHandler != null) 
                    this.ErrorHandler(ex);
            }
            // TODO: Finally -> Stop all Background Threads (Messages, and Queues)
        }
        */

        public void Stop()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopping, WorkerStatus.Started) == WorkerStatus.Started)
            {
                Log.Info(this.GetStatsDescription());
                Log.Debug("Stopping MQ Host...");

                //Unblock current bgthread by issuing StopCommand
                try
                {
                    this.DisposeWorkerThreads();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                    Log.Warn("Could not stop bg thread: " + ex.Message);
                }
            }
        }

        public void NotifyAll()
        {
            Log.Debug("Notifying all worker threads to check for new messages...");
            foreach (var worker in messageWorkers)
            {
                worker.NotifyNewMessage();
            }
        }

        public void StartWorkerThreads()
        {
            Log.Debug("Starting all background message worker threads...");
            Array.ForEach(messageWorkers, x => x.Start());

            Log.Debug("Starting all background queue worker threads...");
            Array.ForEach(queueWorkers, x => x.Start());
        }

        public void KillQueueWorkerThreads()
        {
            Log.Debug("Kill all background queue worker threads...");
            Array.ForEach(queueWorkers, x => x.KillBgThreadIfExists());            
        }

        public void ForceRestartWorkerThreads()
        {
            Log.Debug("ForceRestart all background worker threads...");
            Array.ForEach(queueWorkers, x => x.KillBgThreadIfExists());
            Array.ForEach(messageWorkers, x => x.KillBgThreadIfExists());

            StartWorkerThreads();
        }

        public void StopWorkerThreads()
        {
            Log.Debug("Stopping all queue worker threads...");
            Array.ForEach(queueWorkers, x => x.Stop());

            Log.Debug("Stopping all message worker threads...");
            Array.ForEach(messageWorkers, x => x.Stop());
        }

        void DisposeWorkerThreads()
        {
            Log.Debug("Stopping all queue worker threads...");
            if (queueWorkers != null) Array.ForEach(queueWorkers, x => x.Stop());

            // Block & Wait for each of the MQ workers to stop?

            Log.Debug("Disposing all queue worker threads...");
            if (queueWorkers != null) Array.ForEach(queueWorkers, x => x.Dispose());

            Log.Debug("Disposing all message worker threads...");
            if (messageWorkers != null) Array.ForEach(messageWorkers, x => x.Dispose());
        }

        void WorkerErrorHandler(IMessageHandlerBackgroundWorker source, Exception ex)
        {
            Log.Error("Received exception in Message Worker: " + source.QueueName, ex);
            for (int i = 0; i < messageWorkers.Length; i++)
            {
                var worker = messageWorkers[i];
                if (worker == source)
                {
                    Log.Debug("Starting new {0} Message Worker at index {1}...".Fmt(source.QueueName, i));
                    messageWorkers[i] = (IMessageHandlerBackgroundWorker)source.Clone();
                    messageWorkers[i].Start();
                    worker.Dispose();
                    return;
                }
            }
        }

        void QueueWorkerErrorHandler(IQueueHandlerBackgroundWorker source, Exception ex)
        {
            Log.Error("Received exception in Queue Worker: " + source.QueueName, ex);
            for (int i = 0; i < queueWorkers.Length; i++)
            {
                var worker = queueWorkers[i];
                if (worker == source)
                {
                    Log.Debug("Starting new {0} Queue Worker at index {1}...".Fmt(source.QueueName, i));
                    queueWorkers[i] = (IQueueHandlerBackgroundWorker)source.Clone();
                    queueWorkers[i].Start();
                    worker.Dispose();
                    return;
                }
            }
        }

        /*
        private void KillBgThreadIfExists22()
        {
            if (bgThread != null && bgThread.IsAlive)
            {
                //give it a small chance to die gracefully
                if (!bgThread.Join(500))
                {
                    //Ideally we shouldn't get here, but lets try our hardest to clean it up
                    Log.Warn("Interrupting previous Background Thread: " + bgThread.Name);
                    bgThread.Interrupt();
                    if (!bgThread.Join(TimeSpan.FromSeconds(3)))
                    {
                        Log.Warn(bgThread.Name + " just wont die, so we're now aborting it...");
                        bgThread.Abort();
                    }
                }
                bgThread = null;
            }
        }
        */        

        public virtual void Dispose()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                return;

            Stop();

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopped) != WorkerStatus.Stopped)
                Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopping);

            try
            {
                DisposeWorkerThreads();
            }
            catch (Exception ex)
            {
                Log.Error("Error DisposeWorkerThreads(): ", ex);
            }

            try
            {
                //Thread.Sleep(100); //give it a small chance to die gracefully
                // TODO:??? KillBgThreadIfExists(); -> Performed by Dispose
            }
            catch (Exception ex)
            {
                if (this.ErrorHandler != null) this.ErrorHandler(ex);
            }
        }

        public string GetStatus()
        {
            switch (Interlocked.CompareExchange(ref status, 0, 0))
            {
                case WorkerStatus.Disposed:
                    return "Disposed";
                case WorkerStatus.Stopped:
                    return "Stopped";
                case WorkerStatus.Stopping:
                    return "Stopping";
                case WorkerStatus.Starting:
                    return "Starting";
                case WorkerStatus.Started:
                    return "Started";
            }
            return null;
        }

        public IMessageHandlerStats GetStats()
        {
            lock (messageWorkers)
            {
                var total = new MessageHandlerStats("All Handlers");
                messageWorkers.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        public string GetStatsDescription()
        {
            lock (messageWorkers)
            {
                var sb = new StringBuilder("===============\n");
                sb.AppendLine("#MQ SERVER STATS:");
                sb.AppendLine("===============");
                sb.AppendLine("Current Status: " + GetStatus());
                sb.AppendLine("Listening On: " + string.Join(", ", messageWorkers.ToList().ConvertAll(x => x.QueueName).ToArray()));
                sb.AppendLine("Times Started: " + Interlocked.CompareExchange(ref timesStarted, 0, 0));
                sb.AppendLine("Num of Errors: " + Interlocked.CompareExchange(ref noOfErrors, 0, 0));
                // sb.AppendLine("Num of Continuous Errors: " + Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));
                sb.AppendLine("Last ErrorMsg: " + lastExMsg);
                sb.AppendLine("===============");
                foreach (var queueWorker in queueWorkers)
                {
                    sb.AppendLine(queueWorker.GetStats().ToString());
                    sb.AppendLine("---------------\n");
                }
                foreach (var worker in messageWorkers)
                {
                    sb.AppendLine(worker.GetStats().ToString());
                    sb.AppendLine("---------------\n");
                }
                return sb.ToString();
            }
        }

        public List<string> WorkerThreadsStatus()
        {
            return messageWorkers.ToList().ConvertAll(x => x.GetStatus());
        }
    }
}