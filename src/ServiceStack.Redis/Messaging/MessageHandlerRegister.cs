using System;
using System.Collections.Generic;
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
    public abstract class MessageHandlerRegister<THandlerConfiguration>
        where THandlerConfiguration : DefaultHandlerConfiguration
    {
        protected IMessageService MessageServer { get; private set; }

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
            this.HandlerConfigurations = new Dictionary<Type, THandlerConfiguration>();
        }

        // ================== ORIGINAL HANDLER REGISTRATION ============================
        [Obsolete("Use RegisterMessageHandlers instead.")]
        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            RegisterHandler(processMessageFn, null, noOfThreads: 1);
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
        }

        // ================== NEW HANDLER REGISTRATION ============================
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
            if (HandlerConfigurations.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            this.HandlerConfigurations.Add(typeof(T), RegisterHandler(processMessageFn, processExceptionEx, noOfThreads));
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
            this.AddMessageHandler(processMessageFn, processExceptionEx, 0);
        }

        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

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

        public IDictionary<Type, THandlerConfiguration> HandlerConfigurations { get; private set; }

        public abstract THandlerConfiguration RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads);

        protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            return new MessageHandlerFactory<T>(this.MessageServer, processMessageFn, processExceptionEx)
            {
                RequestFilter = this.RequestFilter,
                ResponseFilter = this.ResponseFilter,
                RetryCount = RetryCount,
            };
        }
    }

}
