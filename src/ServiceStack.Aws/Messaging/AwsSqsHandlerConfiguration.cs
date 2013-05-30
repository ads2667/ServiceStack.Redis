using ServiceStack.Messaging;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public class AwsSqsHandlerConfiguration : DefaultHandlerConfiguration
    {
        public AwsSqsHandlerConfiguration()
            : base()
        {            
        }

        public AwsSqsHandlerConfiguration(decimal? maxNumberOfMessagesToReceivePerRequest, decimal? waitTimeInSeconds, decimal? messageVisibilityTimeout)
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
