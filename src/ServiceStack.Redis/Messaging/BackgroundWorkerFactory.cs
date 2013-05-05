using System;
using System.Collections.Generic;

namespace ServiceStack.Redis.Messaging
{
    public abstract class BackgroundWorkerFactory<THandlerConfiguration>
        where THandlerConfiguration : DefaultHandlerConfiguration
    {
        protected internal abstract IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(THandlerConfiguration messageHandlerConfiguration, string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler);

        protected internal abstract IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, IDictionary<Type, THandlerConfiguration> messageHandlerConfigurations, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler);
    }
}
