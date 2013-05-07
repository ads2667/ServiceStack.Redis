using System;
using System.Collections.Generic;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Aws.Messaging
{    
    public class AwsSqsBackgroundWorkerFactory : BackgroundWorkerFactory<AwsSqsHandlerConfiguration>
    {        
        public AwsSqsBackgroundWorkerFactory(AwsSqsServer sqsServer)
        {
            if (sqsServer == null)
            {
                throw new ArgumentNullException("sqsServer");
            }

            this.SqsServer = sqsServer;
        }

        public AwsSqsServer SqsServer { get; private set; }

        /*
        protected override IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(AwsSqsHandlerConfiguration messageHandlerConfiguration,
                                                                                      string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
        {            
            var messageHandler = messageHandlerConfiguration.MessageHandlerFactory.CreateMessageHandler();
            return new AwsSqsMessageHandlerWorker(this.SqsServer.SqsClient, this.SqsServer, this.SqsServer.QueueUrls, messageHandler, queueName, errorHandler);
        }

        protected override IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, IDictionary<Type, AwsSqsHandlerConfiguration> messageHandlerConfigurations,
                                                           Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
        {
            var queueHandlers = new List<IQueueHandlerBackgroundWorker>();
            foreach (var queue in messageQueueNames)
            {
                var messageConfiguration = messageHandlerConfigurations[queue.Value];
                var waitTimeInSeconds = messageConfiguration.WaitTimeInSeconds.HasValue ? Convert.ToInt32(messageConfiguration.WaitTimeInSeconds.Value) : Convert.ToInt32(this.SqsServer.RequestTimeOut.TotalSeconds);
                var maxNumberOfMessagesToReceivePerRequest = messageConfiguration.MaxNumberOfMessagesToReceivePerRequest.HasValue ? messageConfiguration.MaxNumberOfMessagesToReceivePerRequest.Value : this.SqsServer.MaxNumberOfMessagesToReceivePerRequest;
                var messageVisibilityTimeout = messageConfiguration.MessageVisibilityTimeout.HasValue ? messageConfiguration.MessageVisibilityTimeout.Value : this.SqsServer.MessageVisibilityTimeout;
                queueHandlers.Add(new AwsSqsQueueHandlerWorker(this.SqsServer.SqsClient, this.SqsServer, queue.Value, queue.Key, this.SqsServer.QueueUrls[queue.Key], errorHandler, waitTimeInSeconds, maxNumberOfMessagesToReceivePerRequest, messageVisibilityTimeout));
            }

            return queueHandlers;
        }
        */

        // TODO:Add 'Retry Count' to default config parameters?
        // TODO: Switch 'Task' approach with std ThreadPool, so it is 3.5 compatable?

        protected override IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(HandlerRegistration<AwsSqsHandlerConfiguration> messageHandlerRegistration, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
        {
            var messageHandler = messageHandlerRegistration.MessageHandlerFactory.CreateMessageHandler();
            return new AwsSqsMessageHandlerWorker(this.SqsServer.SqsClient, this.SqsServer, this.SqsServer.QueueUrls, messageHandler, queueName, errorHandler);
        }

        protected override IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, IDictionary<Type, HandlerRegistration<AwsSqsHandlerConfiguration>> messageHandlerRegistrations, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
        {
            var queueHandlers = new List<IQueueHandlerBackgroundWorker>();
            foreach (var queue in messageQueueNames)
            {
                var messageRegistration = messageHandlerRegistrations[queue.Value];
                var messageConfiguration = messageRegistration.Configuration;
                var waitTimeInSeconds = messageConfiguration.WaitTimeInSeconds.HasValue ? Convert.ToInt32(messageConfiguration.WaitTimeInSeconds.Value) : Convert.ToInt32(this.SqsServer.RequestTimeOut.TotalSeconds);
                var maxNumberOfMessagesToReceivePerRequest = messageConfiguration.MaxNumberOfMessagesToReceivePerRequest.HasValue ? messageConfiguration.MaxNumberOfMessagesToReceivePerRequest.Value : this.SqsServer.MaxNumberOfMessagesToReceivePerRequest;
                var messageVisibilityTimeout = messageConfiguration.MessageVisibilityTimeout.HasValue ? messageConfiguration.MessageVisibilityTimeout.Value : this.SqsServer.MessageVisibilityTimeout;
                queueHandlers.Add(new AwsSqsQueueHandlerWorker(this.SqsServer.SqsClient, this.SqsServer, queue.Value, queue.Key, this.SqsServer.QueueUrls[queue.Key], errorHandler, waitTimeInSeconds, maxNumberOfMessagesToReceivePerRequest, messageVisibilityTimeout));
            }

            return queueHandlers;
        }
    }
}
