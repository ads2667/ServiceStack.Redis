using System;
namespace ServiceStack.Aws.Messaging.Data
{
    /// <summary>
    /// Defines a repository that tracks the state of MQ messages.
    /// </summary>
    /// <remarks>
    /// Implementations should be thread-safe.
    /// </remarks>
    public interface IMessageStateRepository
    {
        /// <summary>
        /// Determines if the message can be processed. 
        /// Should take action required to prevent other brokers from processing the same message.
        /// </summary>
        /// <param name="messageId">The Id of the message to be processed.</param>
        /// <returns>True if the message can be processed, otherwise false.</returns>
        bool CanProcessMessage(string messageId);

        /// <summary>
        /// Indicates that the message was processed successfully.
        /// </summary>
        /// <param name="messageId">The Id of the message.</param>
        void MessageProcessingSucceeded(string messageId);

        /// <summary>
        /// Indicates that the message did not execute succesfully.
        /// </summary>
        /// <param name="messageId">The Id of the message.</param>
        void MessageProcessingFailed(string messageId);
    }
}
