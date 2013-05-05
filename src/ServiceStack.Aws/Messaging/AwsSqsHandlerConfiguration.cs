using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsHandlerConfiguration : DefaultHandlerConfiguration
    {
        public AwsSqsHandlerConfiguration(IMessageHandlerFactory messageHandlerFactory, int noOfThreads, decimal? maxNumberOfMessagesToReceivePerRequest, decimal? waitTimeInSeconds, decimal? messageVisibilityTimeout)
            : base(messageHandlerFactory, noOfThreads)
        {
            this.MaxNumberOfMessagesToReceivePerRequest = maxNumberOfMessagesToReceivePerRequest;
            this.MessageVisibilityTimeout = messageVisibilityTimeout;
            this.WaitTimeInSeconds = waitTimeInSeconds;
        }

        public decimal? MaxNumberOfMessagesToReceivePerRequest { get; private set; }

        public decimal? MessageVisibilityTimeout { get; private set; }

        public decimal? WaitTimeInSeconds { get; private set; }
    }
}
