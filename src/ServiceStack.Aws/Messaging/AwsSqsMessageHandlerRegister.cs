using System;
using System.Threading;
using ServiceStack.Aws.Messaging.Data;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerRegister : MessageHandlerRegister<AwsSqsHandlerConfiguration>
    {
        public ISqsClient SqsClient { get; private set; }

        public AwsSqsMessageHandlerRegister(IMessageService messageServer, ISqsClient sqsClient, IMessageStateRepository messageStateRepository, int retryCount = DefaultRetryCount) 
            : base(messageServer, retryCount)
        {
            if (sqsClient == null)
            {
                throw new ArgumentNullException("sqsClient");
            }
            
            if (messageStateRepository == null)
            {
                throw new ArgumentNullException("messageStateRepository");
            }

            this.SqsClient = sqsClient;
            this.MessageStateRepository = messageStateRepository;
        }

        /// <summary>
        /// Gets the <see cref="IMessageStateRepository"/> implementation.
        /// </summary>
        public IMessageStateRepository MessageStateRepository { get; private set; }

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
        protected internal virtual bool OnPreMessageProcessed(ISqsMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            Log.DebugFormat("On Pre Message Processing, Queue: {0}. Thread: {1}.", message.QueueName, Thread.CurrentThread.ManagedThreadId);
            var remainingMessageProcessingTime = DateTime.UtcNow.Subtract(message.MessageExpiryTimeUtc).TotalSeconds;
            if (remainingMessageProcessingTime > 0)
            {
                // The message is expired.
                this.Log.WarnFormat("The message '{0}' has passed it's expiry time by {1} seconds. The message will not be processed.", message.MessageId, remainingMessageProcessingTime);
                return false;
            }

            this.Log.InfoFormat("The message '{0}' has a remaining {1} seconds to be processed. Thread: {2}.", message.MessageId, Math.Abs(remainingMessageProcessingTime), Thread.CurrentThread.ManagedThreadId);
            return this.MessageStateRepository.CanProcessMessage(message.MessageId);
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
        protected internal virtual void OnMessageProcessed(ISqsMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            Log.DebugFormat("Message processed, deleting Message {0} from Queue: {1}. Thread: {2}.", message.MessageId, message.QueueName, Thread.CurrentThread.ManagedThreadId);
            this.SqsClient.DeleteMessage(message.QueueUrl, message.ReceiptHandle);
            this.MessageStateRepository.MessageProcessingSucceeded(message.MessageId);
        }

        /// <summary>
        /// This method is executed after a message has processed and an exception has been thrown.
        /// </summary>
        /// <param name="message">The message that failed to be processed.</param>
        /// <param name="retryAttempts">The number of times this message has failed to execute.</param>
        /// <param name="b"></param>
        /// <param name="messageWillBeMovedToDlq">Indicates if the message will now be moved to the Dead Letter Queue.</param>
        /// <remarks>
        /// Implementors can override this method to implement custom behavior(s) for all messages
        /// that implement <see cref="ISqsMessage"/>.
        /// </remarks>
        protected internal virtual void OnMessageProcessingFailed(ISqsMessage message, int retryAttempts, bool messageWillBeMovedToDlq)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            Log.DebugFormat("On Message Processing Failed, Queue: {0}. Thread: {1}.", message.QueueName, Thread.CurrentThread.ManagedThreadId);
            if (!messageWillBeMovedToDlq)
            {
                this.UpdateMessageVisibilityTimeout(message, retryAttempts);
            }

            this.MessageStateRepository.MessageProcessingFailed(message.MessageId);
        }

        /// <summary>
        /// Gets the message visbility timeout value, in seconds, for a failed message.
        /// This will prevent the message from being processed again for this length of time.
        /// </summary>
        /// <param name="message">The message that failed processing.</param>
        /// <param name="retryAttempts">The number of times the message has been retried.</param>
        /// <returns>The time to wait before the message is processed again.</returns>
        protected virtual TimeSpan GetMessageVisibilityTimeout(ISqsMessage message, int retryAttempts)
        {
            // Default to 5 seconds. It is recommended any implementations implement their own logic here.
            return TimeSpan.FromSeconds(5);
        }

        private void UpdateMessageVisibilityTimeout(ISqsMessage message, int retryAttempts)
        {
            // Update the message visibilityTimeout using a sliding scale to prevent overloading any data store
            try
            {                
                var timeoutInSeconds = (decimal)this.GetMessageVisibilityTimeout(message, retryAttempts).TotalSeconds;

                Log.DebugFormat("Message {0} failed. Updating msg visibility timeout value to {1} seconds.", message.MessageId, timeoutInSeconds);
                this.SqsClient.ChangeMessageVisibility(message.QueueUrl, message.ReceiptHandle, timeoutInSeconds);
            }
            catch (Exception ex)
            {
                // Log the error, but do not throw as it will prevent the message from being released for re-processing.
                Log.Error(string.Format("Error updating message visibility timeout. Queue: {0}. Message Id: {1}.", message.QueueName, message.MessageId), ex);
            }
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

            base.AddPooledMessageHandler<T>(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
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
                    if (!this.OnPreMessageProcessed(sqsMessage))
                    {
                        return null;
                    }
                }
                else
                {
                    Log.WarnFormat(
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
                Log.Warn(string.Format(
                    "An error occurred processing a message of type '{0}{1}' with the AwsSqsMessageHandler, error handlers should catch and log/handle the error.",
                    message.GetType().Name,
                    message.Body == null ? string.Empty : string.Format("<{0}>", message.Body.GetType().Name)),
                    exception);
                try
                {
                    var sqsMessage = message.Body as ISqsMessage;
                    if (sqsMessage != null)
                    {
                        this.OnMessageProcessingFailed(sqsMessage, message.RetryAttempts, exception is UnRetryableMessagingException || message.RetryAttempts >= this.RetryCount);
                    }
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message failed processing exception handler threw an error", exHandlerEx);
                }

                try
                {
                    if (processExceptionEx != null)
                    {
                        processExceptionEx.Invoke(message, exception);    
                    }                    
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message exception handler threw an error", exHandlerEx);
                }

                if (exception is UnRetryableMessagingException || message.RetryAttempts >= this.RetryCount)
                {
                    // Move the Message to the DLQ (Also need to delete from the current queue)
                    var sqsMessage = message.Body as ISqsMessage;
                    if (sqsMessage == null)
                    {
                        throw new InvalidOperationException("Expected a SQS Message.");
                    }

                    Log.DebugFormat("Message {0} has failed {1} times, moving to DLQ.", message.Id, message.RetryAttempts + 1);

                    var queueNames = new VersionedQueueNames(sqsMessage.GetType());
                    try
                    {
                        // Put in DLQ before deleting, we can't 'lose' the message.
                        var dlqUrl = this.SqsClient.GetOrCreateQueueUrl(queueNames.Dlq);
                        this.SqsClient.PublishMessage(dlqUrl, Convert.ToBase64String(message.ToBytes()));
                        this.SqsClient.DeleteMessage(sqsMessage.QueueUrl, sqsMessage.ReceiptHandle);                    
                    }
                    catch (Exception ex)
                    {
                        Log.Error(string.Format("An error occurred trying to move a message to the DLQ '{0}'.", queueNames.Dlq), ex);
                    }
                }
            });
            return exceptionWrapper;
        }
    }
}
