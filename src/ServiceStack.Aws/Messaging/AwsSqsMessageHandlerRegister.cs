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
        public AwsSqsMessageHandlerRegister(MqServer2 messageServer, Amazon.SQS.AmazonSQS client) 
            : base(messageServer)
        {
            if (client == null) throw new ArgumentNullException("client");
            this.Client = client;
        }

        public AmazonSQS Client { get; private set; }

        protected virtual void OnPreMessageProcessed<T>(ISqsMessage message)
        {            
            Log.DebugFormat("On Pre Message Processing, Queue: {0}.", message.QueueName);
            // Allows custom implementations to check that the message has not already been processed.
        }

        protected virtual void OnMessageProcessed<T>(ISqsMessage message)
        {            
            // TODO: May need to update a datastore that the message was successfully processed.

            // Is it a message from an SQS MQ?
            // If so, delete it from the queue as long as no exceptions occurred.
            // TODO: This approach will do for now... Though, a 'processOnSuccess' func would be an easier way, and provide assurances that Notification Callbacks/Responses had been successful also.
            // TODO: Have to involve the processExceptionEx method also, wrap it and if it occurs, do not delete...                    

            // TODO: Need to delete any AWS SQS Messages
            // TODO: Will need access to the message receipt handle, may need to extend the IMessage interface
            Log.DebugFormat("Message processed, deleting Message {0} from Queue: {1}.", message.MessageId, message.QueueName);
            AwsQueingService.DeleteMessage(this.Client, message.QueueUrl, message.ReceiptHandle);
        }

        protected virtual void OnMessageProcessingFailed<T>(ISqsMessage message)
        {
            Log.DebugFormat("On Message Processing Failed, Queue: {0}.", message.QueueName);
            // Allows custom implementations to perform any required processing if message processing fails.
        }
        
        protected sealed override void AddMessageHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            // Wrap func with another that enables message to be deleted from queue after successful processing...
            var processWrapper = new Func<IMessage<T>, object>(message =>
            {
                try
                {
                    // var genericArgs = message.GetType().GetGenericArguments();
                    // var m1 = message as ISqsMessage<T>;
                    var sqsMessage = message.Body as ISqsMessage;
                    if (sqsMessage != null)
                    {
                        this.OnPreMessageProcessed<T>(sqsMessage);
                    }
                    else
                    {
                        Log.InfoFormat("The message body for message {0} does not implement the interface {1}, no custom AwsSqsMessageHandlerRegister methods will be executed.", typeof(T).Name, typeof(ISqsMessage).Name);
                    }

                    // Process the messge using the handler registration method.
                    var result = processMessageFn.Invoke(message);

                    if (sqsMessage != null)
                    {
                        this.OnMessageProcessed<T>(sqsMessage);
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    // TODO: The error handling is already done elsewhere.....
                    /*
                    if (processExceptionEx != null)
                    {
                        processExceptionEx.Invoke(message, ex);
                    }
                    else
                    {
                        throw;
                    }
                    */
                    throw;
                }
            });

            var exceptionWrapper = new Action<IMessage<T>, Exception>((message, exception) =>
            {
                try
                {
                    var sqsMessage = message as ISqsMessage<T>;
                    if (sqsMessage != null)
                    {
                        // TODO: Consider wrapping this in it's own try/catch block.
                        this.OnMessageProcessingFailed<T>(sqsMessage);
                    }
                }
                catch (Exception exHandlerEx)
                {
                    Log.Error("Message failed processing exception handler threw an error", exHandlerEx);
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
