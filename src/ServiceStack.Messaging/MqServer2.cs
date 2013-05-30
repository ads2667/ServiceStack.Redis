using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Common;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Service;
using ServiceStack.Text;

namespace ServiceStack.Messaging
{        
    public abstract class MqServer2<THandlerConfiguration, TMessageHandlerRegister, TBackgroundWorkerFactory> 
        : MqServer2
        where TMessageHandlerRegister : MessageHandlerRegister<THandlerConfiguration>
        where TBackgroundWorkerFactory : BackgroundWorkerFactory<THandlerConfiguration> 
        where THandlerConfiguration : DefaultHandlerConfiguration, new()
    {
        protected MqServer2(IMessageFactory messageFactory, int retryCount)
            : base(messageFactory, retryCount)
        {
        }

        private readonly Dictionary<Type, IMessageHandler> threadPoolHandlers = new Dictionary<Type, IMessageHandler>();
        private readonly Dictionary<Type, IThreadPoolMessageHandlerStats> threadPoolHandlerStats = new Dictionary<Type, IThreadPoolMessageHandlerStats>();

        protected override sealed bool HasRegisteredMessageHandlers()
        {
            if (threadPoolHandlers != null && threadPoolHandlers.Count > 0)
            {
                return true;
            }

            return base.HasRegisteredMessageHandlers();
        }

        private static readonly object messageHandlerRegisterLock = new object();
        private TMessageHandlerRegister messageHandlerRegister = null;
        protected TMessageHandlerRegister MessageHandlerRegister
        {
            get
            {
                if (messageHandlerRegister == null)
                {
                    lock (messageHandlerRegisterLock)
                    {
                        if (messageHandlerRegister == null)
                        {
                            messageHandlerRegister = this.CreateMessageHandlerRegister();
                        }
                    }
                }

                return messageHandlerRegister;
            }
        }

        private static readonly object backgroundWorkerFactoryLock = new object();
        private TBackgroundWorkerFactory backgroundWorkerFactory = null;
        protected TBackgroundWorkerFactory BackgroundWorkerFactory
        {
            get
            {
                if (this.backgroundWorkerFactory == null)
                {
                    lock (backgroundWorkerFactoryLock)
                    {
                        if (this.backgroundWorkerFactory == null)
                        {
                            this.backgroundWorkerFactory = this.CreateBackgroundWorkerFactory();
                        }
                    }
                }

                return this.backgroundWorkerFactory;
            }
        }

        protected abstract TBackgroundWorkerFactory CreateBackgroundWorkerFactory();

        protected abstract TMessageHandlerRegister CreateMessageHandlerRegister();

        public virtual void RegisterMessageHandlers(Action<TMessageHandlerRegister> messageHandlerRegister)
        {
            messageHandlerRegister.Invoke(this.MessageHandlerRegister);
        }

        #region "ThreadPool Support"

        private void ExecuteUsingQueuedWorkItem(object obj)
        {
            Log.DebugFormat("Starting threadpool thread: {0}.", Thread.CurrentThread.ManagedThreadId);
            var threadPoolTask = obj as MessageReceivedArgs;
            if (threadPoolTask == null)
            {
                Log.Warn("A message was queued to be executed using a threadpool work item, but did not pass the required 'MessageReceivedArgs' parameter.");
                return;
            }

            Log.DebugFormat("Executing message {0} using thread pool. Thread: {1}.", threadPoolTask.MessageId, Thread.CurrentThread.ManagedThreadId);

            try
            {
                using (var client = this.CreateMessageQueueClient())
                {
                    threadPoolHandlers[threadPoolTask.MessageType].ProcessQueue(client, threadPoolTask.QueueName, () => false);
                }
                
                // TODO: Need to code 'RetryCode' stat for all message handlers
                threadPoolHandlerStats[threadPoolTask.MessageType].IncrementMessageProcessedCount(1);                
            }
            catch (Exception ex)
            {
                threadPoolHandlerStats[threadPoolTask.MessageType].IncrementMessageFailedCount(1);
                Log.Error(string.Format("Failed to process message {0} using thread pool handler.", threadPoolTask.MessageId), ex);
                // TODO: Need to UnitTest to ensure that failed messages are moved to DLQ!
                // TODO: Optionally, execute a custom ex handler?
                throw;
            }
            finally
            {
                // Remove a listener from the list
                // lock (manualResetEvents)
                // {
                    // pulse if flag set and count == 0
                    Interlocked.Decrement(ref threadPoolWorkItemCount);
                    // manualResetEvents.Remove(manualResetEvent);
                // }

                // Pulse if required -> Notifying that processing of worker items is complete.
                lock (locker)
                {
                    if (disposing && Interlocked.Read(ref threadPoolWorkItemCount) == 0)
                    {
                        Log.Debug("Threadpool Worker Items Complete.");
                        // TODO: Pulse
                        canDispose = true;
                        Monitor.Pulse(locker);
                    }
                }
            }

            Log.DebugFormat("Finished threadpool thread: {0}.", Thread.CurrentThread.ManagedThreadId);
        }

        internal override sealed void DisposeWorkerThreads()
        {
            base.DisposeWorkerThreads();

            // TODO: Need to write unit tests to ensure that all work items are completed before the server is stopped.
            // TODO: Why are pooled items being deleted twice?

            // TODO: Move to DiposeWorkerThreads Method; make virtual (seal), and place code in this class.
            // Set flag to indicate that a pulse is required, and wait.
            Log.Debug("Dispose threadpool worker items.");
            lock (locker)
            {
                Log.Debug("Waiting for threadpool worker items to complete.");
                if (Interlocked.Read(ref threadPoolWorkItemCount) > 0)
                {
                    Log.DebugFormat("Worker items to complete: {0}", Interlocked.Read(ref threadPoolWorkItemCount));

                    // Need to wait for pending work items to complete.
                    disposing = true;
                    while (!canDispose)
                    {
                        Monitor.Wait(locker);
                    }

                    Log.Debug("Finished processing all worker items.");
                }
            }

            Log.Debug("Threadpool worker items disposed.");
        }

        // TODO: Investigate why DELETE is being executed/logged twice for pooled worker items!?!?

        private bool disposing = false;
        private bool canDispose = false;
        private static readonly object locker = new object();
        private static long threadPoolWorkItemCount = 0;

        // IList<WaitHandle> manualResetEvents = new List<WaitHandle>();
        public override void NotifyMessageReceived(MessageReceivedArgs messageReceivedArgs)
        {
            var handlerThreadCount = this.MessageHandlerRegister.RegisteredHandlers[messageReceivedArgs.MessageType].Configuration.NoOfThreads;
            if (handlerThreadCount == 0) //// 0 => Threadpool
            {
                // Threadpool
                // Queue the item to execute using the thread pool.

                // TODO: Use Wait and Pulse; Use a incremental lock counter to ensure that all queued work items complete.
                // var manualResetEvent = new ManualResetEvent(false);
                // lock (manualResetEvents)
                // {
                // TODO: Verify that a lock is not required.
                    // manualResetEvents.Add(manualResetEvent);
                    Interlocked.Increment(ref threadPoolWorkItemCount);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(ExecuteUsingQueuedWorkItem), messageReceivedArgs);
                    // TODO: How to wait for all tasks to complete using this method?
                // }

                /*
                // TODO: Change Project Framework to 3.5
                // TODO: Wait for all queued work items to complete when stop is called, in DISPOSE method.
                lock (manualResetEvents)
                {                    
                    // After stop is called, set a flag that indicates pulse should be called when count == 0
                    // TODO: Loop until count is 0; then pulse (Or, if already 0; pulse instantly)
                    WaitHandle.WaitAll(manualResetEvents.ToArray());
                }*/
            }
            else
            {
                // Static Thread
                base.NotifyMessageReceived(messageReceivedArgs);
            }            
        }

        #endregion

        public override sealed string GetStatsDescription()
        {
            var statsBuilder = new StringBuilder();
            statsBuilder.Append(base.GetStatsDescription());
            statsBuilder.AppendLine("---------------");
            statsBuilder.AppendLine("ThreadPool Message Handler Stats");
            statsBuilder.AppendLine("---------------");
            foreach (var threadPoolStats in this.threadPoolHandlerStats)
            {
                statsBuilder.AppendLine(threadPoolStats.Value.ToString());
                statsBuilder.AppendLine("---------------");
            }

            return statsBuilder.ToString();
        }

        protected override void Init()
        {
            if (messageWorkers == null)
            {
                var workerBuilder = new List<IMessageHandlerBackgroundWorker>();
                var queuesToMonitor = new Dictionary<string, Type>();
                
                foreach (var handler in this.MessageHandlerRegister.RegisteredHandlers)
                {
                    // TODO: Should produce the same results as the loop below, but use custom config constructs.
                    var messageType = handler.Key;
                    var handlerRegistration = handler.Value;
                    var queueNames = new VersionedQueueNames(messageType); //// new QueueNames(messageType);

                    if (OnlyEnablePriortyQueuesForTypes == null
                        || OnlyEnablePriortyQueuesForTypes.Any(x => x == messageType))
                    {
                        // Create a priority queue, and associated message handlers
                        if (handlerRegistration.Configuration.NoOfThreads > 0)
                        {
                            // Called for each required background message handler.
                            handlerRegistration.Configuration.NoOfThreads.Times(i =>
                                              workerBuilder.Add(
                                                  this.BackgroundWorkerFactory.CreateMessageHandlerWorker(
                                                      handlerRegistration,
                                                      queueNames.Priority,
                                                      WorkerErrorHandler)));
                        }

                        queuesToMonitor.Add(queueNames.Priority, messageType);
                    }

                    if (handlerRegistration.Configuration.NoOfThreads == 0)
                    {
                        threadPoolHandlers.Add(messageType, handlerRegistration.MessageHandlerFactory.CreateMessageHandler());
                        threadPoolHandlerStats.Add(messageType, new ThreadPoolMessageHandlerStats(messageType.Name));
                    }
                    else
                    {
                        handlerRegistration.Configuration.NoOfThreads.Times(i =>
                                          workerBuilder.Add(
                                            this.BackgroundWorkerFactory.CreateMessageHandlerWorker(
                                                handlerRegistration,
                                                queueNames.In,
                                                WorkerErrorHandler)));
                    }

                    queuesToMonitor.Add(queueNames.In, messageType);
                }

                messageWorkers = workerBuilder.ToArray();

                // Create the background worker thread(s) to monitor message queue(s)
                queueWorkers = this.BackgroundWorkerFactory.CreateQueueHandlerWorkers(
                    queuesToMonitor,
                    this.MessageHandlerRegister.RegisteredHandlers,
                    QueueWorkerErrorHandler).ToArray();

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

        [Obsolete]
        public override void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            this.MessageHandlerRegister.RegisterHandler(processMessageFn);
        }

        [Obsolete]
        public override void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            this.MessageHandlerRegister.RegisterHandler(processMessageFn, processExceptionEx);
        }

        [Obsolete]
        public override void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            this.MessageHandlerRegister.RegisterHandler(processMessageFn, processExceptionEx, noOfThreads);
        }

        [Obsolete]
        public override void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads)
        {
            this.MessageHandlerRegister.RegisterHandler(processMessageFn, noOfThreads);
        }
    }

    public abstract class MqServer2 : IMessageService
    {
        protected static ILog Log;
        
        public virtual IMessageFactory MessageFactory { get; private set; }

        public Func<string, IOneWayClient> ReplyClientFactory { get; set; }

        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

        public virtual int RetryCount { get; private set; }

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

        public abstract IMessageQueueClient CreateMessageQueueClient();
        
        //Stats
        private long timesStarted = 0;
        private long noOfErrors = 0;
        private string lastExMsg = null;
        protected int status;

        private long bgThreadCount = 0;
        public long BgThreadCount
        {
            get { return Interlocked.CompareExchange(ref bgThreadCount, 0, 0); }
        }

        protected IMessageHandlerBackgroundWorker[] messageWorkers;
        protected IQueueHandlerBackgroundWorker[] queueWorkers;

        protected Dictionary<string, int[]> queueWorkerIndexMap;

        public MqServer2(
            IMessageFactory messageFactory,
            int retryCount)
        {
            Log = LogManager.GetLogger(this.GetType());
            this.RetryCount = retryCount;
            this.MessageFactory = messageFactory; // new RedisMessageFactory(clientsManager);
            this.ErrorHandler = ex => Log.Error("Exception ins MQ Server: " + ex.Message, ex);
        }

        protected abstract void Init();

        protected virtual bool HasRegisteredMessageHandlers()
        {
            return messageWorkers == null || messageWorkers.Length == 0;
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

                    if (!this.HasRegisteredMessageHandlers())
                    {
                        Log.Warn("Cannot start a MQ Server with no Message Handlers registered, ignoring.");
                        Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Starting);
                        return;
                    }

                    // TODO: Move this to the Queue Handler
                    // SleepBackOffMultiplier(Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));

                    KillQueueWorkerThreads();

                    // Start the worker threads before the MQ listeners, so they're ready to process
                    StartWorkerThreads();

                    if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
                    Interlocked.Increment(ref timesStarted);
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                }
            }
        }

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

        private object threadLock = new object();
        public virtual void NotifyMessageReceived(MessageReceivedArgs messageReceivedArgs)
        {               
            // Static Thread
            if (!string.IsNullOrEmpty(messageReceivedArgs.QueueName))
            {
                // TODO: Is this lock required?
                lock (threadLock)
                {
                    int[] workerIndexes;
                    if (queueWorkerIndexMap.TryGetValue(messageReceivedArgs.QueueName, out workerIndexes))
                    {
                        foreach (var workerIndex in workerIndexes)
                        {
                            messageWorkers[workerIndex].NotifyNewMessage();
                        }
                    }
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

        internal virtual void DisposeWorkerThreads()
        {
            Log.Debug("Stopping all queue worker threads...");
            if (queueWorkers != null) Array.ForEach(queueWorkers, x => x.Stop());

            // Block & Wait for each of the MQ workers to stop?

            Log.Debug("Disposing all queue worker threads...");
            if (queueWorkers != null) Array.ForEach(queueWorkers, x => x.Dispose());

            Log.Debug("Disposing all message worker threads...");
            if (messageWorkers != null) Array.ForEach(messageWorkers, x => x.Dispose());
        }

        internal void WorkerErrorHandler(IMessageHandlerBackgroundWorker source, Exception ex)
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

        internal void QueueWorkerErrorHandler(IQueueHandlerBackgroundWorker source, Exception ex)
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

        [Obsolete]
        public abstract void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn);
        
        [Obsolete]
        public abstract void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx);
        
        [Obsolete]
        public abstract void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads);        

        [Obsolete]
        public abstract void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads);

        public IMessageHandlerStats GetStats()
        {
            lock (messageWorkers)
            {
                var total = new MessageHandlerStats("All Handlers");
                messageWorkers.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        public virtual string GetStatsDescription()
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
                    sb.AppendLine("---------------");
                }
                foreach (var worker in messageWorkers)
                {
                    sb.AppendLine(worker.GetStats().ToString());
                    sb.AppendLine("---------------");
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