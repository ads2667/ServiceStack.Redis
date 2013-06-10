using System;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerRegister : MessageHandlerRegister<AwsSqsHandlerConfiguration>
    {
        public AwsSqsMessageHandlerRegister(IMessageService messageServer, IMessageProcessor messageProcessor, int retryCount = DefaultRetryCount) 
            : base(messageServer, retryCount)
        {            
            if (messageProcessor == null)
            {
                throw new ArgumentNullException("messageProcessor");
            }

            this.MessageProcessor = messageProcessor;
        }

        public IMessageProcessor MessageProcessor { get; private set; }
        
        protected override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            var processWrapper = WrapMessageProcessor(processMessageFn);
            var exceptionWrapper = WrapExceptionHandler(processExceptionEx);
            base.AddMessageHandler(processWrapper, exceptionWrapper, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        protected override void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

            base.AddPooledMessageHandler(processMessageFn, processExceptionEx, queueHandlerConfiguration, messageHandlerConfiguration);
        }

        public override HandlerRegistration<AwsSqsHandlerConfiguration> RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, AwsSqsHandlerConfiguration queueHandlerConfiguration, MessageHandlerConfiguration messageHandlerConfiguration)
        {
            return new HandlerRegistration<AwsSqsHandlerConfiguration>(
                this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx, messageHandlerConfiguration),
                queueHandlerConfiguration);
        }

        internal Func<IMessage<T>, object> WrapMessageProcessor<T>(Func<IMessage<T>, object> processMessageFn)
        {
            // Wrap func with another that enables message to be deleted from queue after successful processing...
            var processWrapper = new Func<IMessage<T>, object>(message =>
            {
                if (!this.MessageProcessor.CanProcessMessage(message))
                {
                    // Need to return an exception so that the exception handler is run, and DLQ is supported.
                    return new MessageNotProcessedException();
                }
               
                // Process the messge using the handler registration method.
                Log.DebugFormat("Executing messge {0}, retry attempt: {1}.", message.Id, message.RetryAttempts);
                var result = processMessageFn.Invoke(message);

                this.MessageProcessor.OnMessageProcessed(message);                
                return result;
            });

            return processWrapper;
        }

        internal Action<IMessage<T>, Exception> WrapExceptionHandler<T>(Action<IMessage<T>, Exception> processExceptionEx)
        {
            var exceptionWrapper = new Action<IMessage<T>, Exception>((message, exception) =>
            {
                Log.Warn(string.Format(
                    "An error occurred processing a message of type '{0}{1}' with the AwsSqsMessageHandler, error handlers should catch and log/handle the error.",
                    message.GetType().Name,
                    message.Body == null ? string.Empty : string.Format("<{0}>", message.Body.GetType().Name)),
                    exception);

                var moveMessageToDlq = exception is UnRetryableMessagingException || message.RetryAttempts >= this.RetryCount + 2; // Base 1, + 1 for the original attempt (that is, the 1st attempt is NOT a retry) = 2
                bool messageProcessingWasAttempted = !(exception is MessageNotProcessedException);

                // Execute any custom ex handling code configured by the user.
                try
                {
                    if (messageProcessingWasAttempted && processExceptionEx != null)
                    {
                        processExceptionEx.Invoke(message, exception);    
                    }                    
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message exception handler threw an error", exHandlerEx);
                }

                this.MessageProcessor.OnMessageProcessingFailed(message, exception, moveMessageToDlq);           
            });

            return exceptionWrapper;
        }
    }
}
