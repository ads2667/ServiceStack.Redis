using System;

namespace ServiceStack.Messaging
{
    public interface IMessageProcessor
    {
        /// <summary>
        /// This method is executed before a message is processed.
        /// </summary>
        /// <param name="message">The original message from the message queue.</param>
        /// <remarks>
        /// Implementors can override this method to implement common behavior(s) for all messages
        /// <para />
        /// A common requirement may be to verify that this message has not already been processed.
        /// </remarks>
        bool CanProcessMessage<T>(IMessage<T> message);
        
        /// <summary>
        /// This method is executed after a message has been successfully processed.
        /// The default behavior deletes the message that was processed from the SQS message queue.
        /// </summary>
        /// <param name="message">The message from the message queue.</param>
        /// <remarks>
        /// Implementors can override this method to implement custom behavior(s) for all messages
        /// <para />
        /// A common requirement may be to flag that this message has now been processed.
        /// </remarks>
        void OnMessageProcessed<T>(IMessage<T> message);       

        /// <summary>
        /// This method is executed after a message has processed and an exception has been thrown.
        /// </summary>
        /// <param name="message">The message from the message queue.</param>
        /// <param name="exception">The exception that caused message processing to fail.</param>
        /// <param name="moveMessageToDlq">True if the message should be moved to the DLQ.</param>
        void OnMessageProcessingFailed<T>(IMessage<T> message, Exception exception, bool moveMessageToDlq);
    }
}
