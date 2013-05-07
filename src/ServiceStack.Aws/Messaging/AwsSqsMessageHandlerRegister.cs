using System;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerRegister : MessageHandlerRegister<AwsSqsHandlerConfiguration>
    {
        public ISqsClient SqsClient { get; private set; }

        public AwsSqsMessageHandlerRegister(MqServer2 messageServer, ISqsClient sqsClient, int retryCount = DefaultRetryCount) 
            : base(messageServer, retryCount)
        {
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }

            SqsClient = sqsClient;
        }

        /// <summary>
        /// This method is executed before a message is processed.
        /// </summary>
        /// <param name="message">The message that will be processed.</param>
        /// <remarks>
        /// Implementors can override this method to implement common behavior(s) for all messages
        /// that implement <see cref="ISqsMessage"/>. 
        /// <para />
        /// A common requirement may be to verify that this message has not already been processed.
        /// </remarks>
        protected virtual void OnPreMessageProcessed(ISqsMessage message)
        {            
            Log.DebugFormat("On Pre Message Processing, Queue: {0}.", message.QueueName);
            var remainingMessageProcessingTime = DateTime.UtcNow.Subtract(message.MessageExpiryTimeUtc).TotalSeconds;
            if (remainingMessageProcessingTime > 0)
            {
                // The message is expired.
                this.Log.WarnFormat("The message '{0}' has passed it's expiry time by {1} seconds. The message will not be processed.", message.MessageId, remainingMessageProcessingTime);
                throw new NotImplementedException("Exception is not ideal now, should return a bool or alter message");
            }
            
            this.Log.InfoFormat("The message '{0}' has a remaining {1} seconds to be processed.", message.MessageId, Math.Abs(remainingMessageProcessingTime));
        }

        /// <summary>
        /// This method is executed after a message has been successfully processed.
        /// The default behavior deletes the message that was processed from the SQS message queue.
        /// </summary>
        /// <param name="message">The message that was processed.</param>
        /// <remarks>
        /// Implementors can override this method to implement custom behavior(s) for all messages
        /// that implement <see cref="ISqsMessage"/>.
        /// <para />
        /// A common requirement may be to flag that this message has now been processed.
        /// </remarks>
        protected virtual void OnMessageProcessed(ISqsMessage message)
        {            
            Log.DebugFormat("Message processed, deleting Message {0} from Queue: {1}.", message.MessageId, message.QueueName);
            this.SqsClient.DeleteMessage(message.QueueUrl, message.ReceiptHandle);
        }

        /// <summary>
        /// This method is executed after a message has processed and an exception has been thrown.
        /// </summary>
        /// <param name="message">The message that failed to be processed.</param>
        /// <remarks>
        /// Implementors can override this method to implement custom behavior(s) for all messages
        /// that implement <see cref="ISqsMessage"/>.
        /// </remarks>
        protected virtual void OnMessageProcessingFailed(ISqsMessage message)
        {
            Log.DebugFormat("On Message Processing Failed, Queue: {0}.", message.QueueName);
        }

        // Allow default values for queue configuration to be overridden
        /*
        public void AddHandler<T>(
            Func<IMessage<T>, object> processMessageFn,
            Action<IMessage<T>, Exception> processExceptionEx,
            int noOfThreads,
            int? retryCount,
            TimeSpan? requestTimeOut,
            decimal? maxNumberOfMessagesToReceivePerRequest,
            decimal? waitTimeInSeconds,
            decimal? messageVisibilityTimeout)
        {
            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            //this.AddMessageHandler(processWrapper, exceptionWrapper, noOfThreads);
            throw new NotImplementedException("Does not require response type, fix this!");
            this.RegisteredHandlers.Add(typeof(T), RegisterHandler(processWrapper, exceptionWrapper, noOfThreads, maxNumberOfMessagesToReceivePerRequest, waitTimeInSeconds, messageVisibilityTimeout));
        }

        public void AddPooledHandler<T>(
            Func<IMessage<T>, object> processMessageFn,
            Action<IMessage<T>, Exception> processExceptionEx,
            int? retryCount,
            TimeSpan? requestTimeOut,
            decimal? maxNumberOfMessagesToReceivePerRequest,
            decimal? waitTimeInSeconds,
            decimal? messageVisibilityTimeout)
        {
            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            throw new NotImplementedException("Does not require response type, fix this!");
            // TODO: instead of passing 'noOfThreads', just pass a TYPED config object to the base class! Which calls an abstract method to assign to the handler registration.
            this.RegisteredHandlers.Add(typeof(T), RegisterHandler(processWrapper, exceptionWrapper, 0, maxNumberOfMessagesToReceivePerRequest, waitTimeInSeconds, messageVisibilityTimeout));
        }
        */

        /*
        protected sealed override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddMessageHandler(processWrapper, exceptionWrapper, noOfThreads);
        }
        */

        protected override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddMessageHandler<T>(processWrapper, exceptionWrapper, queueHandlerConfiguration, messageHandlerConfiguration);
        }
        /*
        protected override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration handlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddMessageHandler<T>(processWrapper, exceptionWrapper, handlerConfiguration);
        }
        */
        /*
        protected sealed override void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddPooledMessageHandler(processWrapper, exceptionWrapper);
        }
        */

        protected override void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddPooledMessageHandler<T>(processWrapper, exceptionWrapper, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        /*
        protected override void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration handlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddPooledMessageHandler<T>(processWrapper, exceptionWrapper, handlerConfiguration);
        }
        */
        /*
        public override AwsSqsHandlerConfiguration RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            return this.RegisterHandler(processMessageFn, processExceptionEx, noOfThreads, null, null, null);            
        }
        */

        public override HandlerRegistration<AwsSqsHandlerConfiguration> RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            return new HandlerRegistration<AwsSqsHandlerConfiguration>(
                this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx, messageHandlerConfiguration),
                queueHandlerConfiguration);
        }

        /*
        public override HandlerRegistration<AwsSqsHandlerConfiguration> RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx,
                                                               AwsSqsHandlerConfiguration handlerConfiguration)
        {
            return new HandlerRegistration<AwsSqsHandlerConfiguration>(
                this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx),
                handlerConfiguration);
        }
        */

        /*
        public AwsSqsHandlerConfiguration RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads, decimal? maxNumberOfMessagesToReceivePerRequest, decimal? waitTimeInSeconds, decimal? messageVisibilityTimeout)
        {
            return new AwsSqsHandlerConfiguration(
                    this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx),
                    noOfThreads,
                    maxNumberOfMessagesToReceivePerRequest,
                    waitTimeInSeconds,
                    messageVisibilityTimeout
                );
        }
        */

        private Func<IMessage<T>, object> WrapMessageProcessor<T>(Func<IMessage<T>, object> processMessageFn)
        {
            // Wrap func with another that enables message to be deleted from queue after successful processing...
            var processWrapper = new Func<IMessage<T>, object>(message =>
            {
                var sqsMessage = message.Body as ISqsMessage;
                if (sqsMessage != null)
                {
                    this.OnPreMessageProcessed(sqsMessage);
                }
                else
                {
                    Log.InfoFormat(
                        "The message body for message {0} does not implement the interface {1}, no custom AwsSqsMessageHandlerRegister methods will be executed.",
                        typeof(T).Name, typeof(ISqsMessage).Name);
                }

                // Process the messge using the handler registration method.
                var result = processMessageFn.Invoke(message);

                if (sqsMessage != null)
                {
                    this.OnMessageProcessed(sqsMessage);
                }

                return result;
            });
            return processWrapper;
        }

        private Action<IMessage<T>, Exception> WrapExceptionHandler<T>(Action<IMessage<T>, Exception> processExceptionEx)
        {
            var exceptionWrapper = new Action<IMessage<T>, Exception>((message, exception) =>
            {
                Log.WarnFormat(
                    "An error occurred processing a message of type '{0}{1}' with the AwsSqsMessageHandler, error handlers should catch and log/handle the error. The exception message is: {2}.",
                    message.GetType().Name,
                    message.Body == null ? string.Empty : string.Format("<{0}>", message.Body.GetType().Name),
                    exception.Message);
                try
                {
                    var sqsMessage = message.Body as ISqsMessage;
                    if (sqsMessage != null)
                    {
                        this.OnMessageProcessingFailed(sqsMessage);
                    }
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message failed processing exception handler threw an error", exHandlerEx);
                }

                if (processExceptionEx == null)
                {
                    return;
                }

                try
                {
                    processExceptionEx.Invoke(message, exception);
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message exception handler threw an error", exHandlerEx);
                }
            });
            return exceptionWrapper;
        }
    }
}
