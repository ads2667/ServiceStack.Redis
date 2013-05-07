using System;
using System.Collections.Generic;

namespace ServiceStack.Redis.Messaging
{
    public abstract class BackgroundWorkerFactory<THandlerConfiguration>
        where THandlerConfiguration : DefaultHandlerConfiguration
    {
        protected internal abstract IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(HandlerRegistration<THandlerConfiguration> messageHandlerRegistration, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler);

        protected internal abstract IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, IDictionary<Type, HandlerRegistration<THandlerConfiguration>> messageHandlerRegistrations, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler);
    }
}
