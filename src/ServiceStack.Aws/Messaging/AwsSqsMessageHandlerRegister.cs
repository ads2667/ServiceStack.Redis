using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsMessageHandlerRegister : MessageHandlerRegister
    {
        public ISqsClient SqsClient { get; private set; }

        public AwsSqsMessageHandlerRegister(MqServer2 messageServer, ISqsClient  sqsClient) 
            : base(messageServer)
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
        
        protected sealed override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            if (processMessageFn == null)
            {
                throw new ArgumentNullException("processMessageFn");
            }

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
                    Log.InfoFormat("The message body for message {0} does not implement the interface {1}, no custom AwsSqsMessageHandlerRegister methods will be executed.", typeof(T).Name, typeof(ISqsMessage).Name);
                }

                // Process the messge using the handler registration method.
                var result = processMessageFn.Invoke(message);

                if (sqsMessage != null)
                {
                    this.OnMessageProcessed(sqsMessage);
                }

                return result;               
            });

            var exceptionWrapper = new Action<IMessage<T>, Exception>((message, exception) =>
            {
                Log.WarnFormat("An error occurred processing a message of type '{0}{1}' with the AwsSqsMessageHandler, error handlers should catch and log/handle the error. The exception message is: {2}.",  message.GetType().Name, message.Body == null ? string.Empty : string.Format("<{0}>", message.Body.GetType().Name), exception.Message);
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

            base.AddMessageHandler(processWrapper, exceptionWrapper, noOfThreads);
        }

        protected sealed override void AddPooledMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            base.AddPooledMessageHandler(processMessageFn, processExceptionEx, noOfThreads);
        }
    }
}
