using System;
using System.Collections.Generic;
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
    public abstract class MessageHandlerRegister<TQueueHandlerConfiguration>
        where TQueueHandlerConfiguration : DefaultHandlerConfiguration, new()
    {
        protected internal IMessageService MessageServer { get; private set; }

        protected ILog Log;

        protected MessageHandlerRegister(IMessageService messageServer, int retryCount)
        {
            if (messageServer == null)
            {
                throw new ArgumentNullException("messageServer");
            }

            this.Log = LogManager.GetLogger(this.GetType());
            this.MessageServer = messageServer;
            this.RetryCount = retryCount;
            this.ResponseMessageTypes = new List<Type>();
            this.RegisteredHandlers = new Dictionary<Type, HandlerRegistration<TQueueHandlerConfiguration>>();
        }

        // ================== ORIGINAL HANDLER REGISTRATION ============================
        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            RegisterHandler<T>(processMessageFn, 1);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads)
        {
            // RegisterHandler(processMessageFn, null, noOfThreads);
            this.AddMessageHandler(processMessageFn, null, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads }, null);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            this.RegisterHandler(processMessageFn, processExceptionEx, 1);
        }

        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {            
            // RegisterHandler(processMessageFn, processExceptionEx, 1);
            this.AddMessageHandler(processMessageFn, processExceptionEx, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads }, null);
        }

        // ================== NEW HANDLER REGISTRATION ============================
        /*
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
            AddMessageHandler(processMessageFn, processExceptionEx, noOfThreads);
        }

        protected virtual void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            if (RegisteredHandlers.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            this.RegisteredHandlers.Add(typeof(T), RegisterHandler(processMessageFn, processExceptionEx, noOfThreads));
        }
        */
        // ========== REQUIRE MESSAGE OUTPUT TYPES TO BE DEFINED, Enable auto-registration of response queues =========
        /*
        public void AddPooledHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn)
        {
            this.AddPooledHandler(processMessageFn, null);
        }

        public void AddPooledHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            this.AddPooledHandler(processMessageFn, processExceptionEx, queueHandlerConfiguration: null);
        }
        */

        public void AddPooledHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddPooledHandler(processMessageFn, null, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public void AddPooledHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddPooledMessageHandler(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        // ==========
        /*
        public void AddPooledHandler<T>(Action<IMessage<T>> processMessageFn)
        {
            this.AddPooledHandler(processMessageFn, queueHandlerConfiguration: null);
        }
        */

        public void AddPooledHandler<T>(Action<IMessage<T>> processMessageFn, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddPooledHandler(processMessageFn, null, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public void AddPooledHandler<T>(Action<IMessage<T>> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            // A thread count of 0, indicates that the handler should use the thread pool
            var wrappedMessageFn = WrapActionHandler(processMessageFn);
            this.AddPooledMessageHandler(wrappedMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
            //this.AddMessageHandler(wrappedMessageFn, processExceptionEx, 0);
        }

        // ==========================================================================================================

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn)
        {
            this.AddHandler(processMessageFn, 1);
        }

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, int noOfThreads)
        {
            this.AddHandler(processMessageFn, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads });
        }

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddHandler(processMessageFn, null, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            this.AddHandler(processMessageFn, processExceptionEx, 1);
        }

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            this.AddMessageHandler(processMessageFn, processExceptionEx, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads }, messageHandlerConfiguration: null);
        }

        public void AddHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddMessageHandler(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn)
        {
            AddHandler(processMessageFn, 1);
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn, int noOfThreads)
        {
            this.AddHandler(processMessageFn, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads });
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            this.AddHandler(processMessageFn, null, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            this.AddHandler(processMessageFn, processExceptionEx, 1);
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            this.AddHandler(processMessageFn, processExceptionEx, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads });
        }

        public void AddHandler<T>(Action<IMessage<T>> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration = null, MessageHandlerConfiguration messageHandlerConfiguration = null)
        {
            var wrappedMessageFn = WrapActionHandler(processMessageFn);
            this.AddMessageHandler(wrappedMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        protected void AddMessageHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            this.AddMessageHandler(processMessageFn, processExceptionEx, new TQueueHandlerConfiguration { NoOfThreads = noOfThreads }, messageHandlerConfiguration: null);           
        }

        protected void AddMessageHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (RegisteredHandlers.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            var wrappedTypedResponseFn = WrapTypedResponseHandler(processMessageFn);
            this.AddMessageHandler<T>(wrappedTypedResponseFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
            // this.RegisteredHandlers.Add(typeof(T), RegisterHandler(wrappedTypedResponseFn, processExceptionEx, noOfThreads));

            if (typeof(TResponse) == typeof(object))
            {
                return;
            }

            this.ResponseMessageTypes.Add(typeof(TResponse)); //// Need to enable queue creation
        }

        protected void AddPooledMessageHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            // A thread count of 0, indicates that the handler should use the thread pool
            var wrappedTypedResponseFn = WrapTypedResponseHandler(processMessageFn);
            this.AddPooledMessageHandler<T>(wrappedTypedResponseFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);

            if (typeof(TResponse) == typeof(object))
            {
                return;
            }

            this.ResponseMessageTypes.Add(typeof(TResponse)); //// Need to enable queue creation
        }

        protected virtual void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            // A thread count of 0, indicates that the handler should use the thread pool
            queueHandlerConfiguration = queueHandlerConfiguration ?? new TQueueHandlerConfiguration();
            queueHandlerConfiguration.NoOfThreads = 0; // 0 => ThreadPool
            this.AddMessageHandler<T>(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        protected virtual void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (RegisteredHandlers.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            this.RegisteredHandlers.Add(typeof(T), RegisterHandler(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration));
        }

        private static Func<IMessage<T>, object> WrapActionHandler<T>(Action<IMessage<T>> processMessageFn)
        {
            return message =>
                {
                    processMessageFn.Invoke(message);
                    return null;
                };
        }

        private static Func<IMessage<T>, object> WrapTypedResponseHandler<T, TResponse>(Func<IMessage<T>, TResponse> processMessageFn)
        {
            return message => processMessageFn.Invoke(message);
        }

        // ==========
        /*
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
            this.AddMessageHandler(processMessageFn, processExceptionEx, 0);
        }
        */

        public const int DefaultRetryCount = 3; //Will be a total of 3 attempts

        public int RetryCount { get; protected set; }

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

        public IDictionary<Type, HandlerRegistration<TQueueHandlerConfiguration>> RegisteredHandlers { get; private set; }

        public IList<Type> ResponseMessageTypes { get; private set; }

        public abstract HandlerRegistration<TQueueHandlerConfiguration> RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, TQueueHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration);

        // TODO:Add 'Retry Count' to default config parameters? Request/Response Filters?
        // TODO: Add Error Count to Queue (And Msg?) BG Handlers
        // TODO: Switch 'Task' approach with std ThreadPool, so it is 3.5 compatable? => Finish.

        protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            messageHandlerConfiguration = messageHandlerConfiguration ?? new MessageHandlerConfiguration();
            return new MessageHandlerFactory<T>(this.MessageServer, processMessageFn, processExceptionEx)
            {
                RequestFilter = messageHandlerConfiguration.RequestFilter ?? this.RequestFilter,
                ResponseFilter = messageHandlerConfiguration.ResponseFilter ?? this.ResponseFilter,
                RetryCount =  messageHandlerConfiguration.RetryCount ?? this.RetryCount 
            };
        }
    }
}
