namespace ServiceStack.Aws.Messaging.Data
{
    /// <summary>
    /// Provides a default implementation of the <see cref="IMessageStateRepository"/>.
    /// <para />
    /// This implementation does not maintain any message state, and will allow any message to be
    /// processed, any number of times.
    /// </summary>
    public class DefaultMessageStateRespository : IMessageStateRepository
    {
        /// <summary>
        /// Determines if the message can be processed. 
        /// Should take action required to prevent other brokers from processing the same message.
        /// </summary>
        /// <param name="messageId">The Id of the message to be processed.</param>
        /// <returns>True if the message can be processed, otherwise false.</returns>
        public bool CanProcessMessage(string messageId)
        {
            return true;
        }

        /// <summary>
        /// Indicates that the message was processed successfully.
        /// </summary>
        /// <param name="messageId">The Id of the message.</param>
        public void MessageProcessingSucceeded(string messageId)
        {
            return;            
        }

        /// <summary>
        /// Indicates that the message did not execute succesfully.
        /// </summary>
        /// <param name="messageId">The Id of the message.</param>
        public void MessageProcessingFailed(string messageId)
        {
            return;
        }
    }
}
