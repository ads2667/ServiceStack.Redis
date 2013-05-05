using System;
using System.Collections.Generic;

namespace ServiceStack.Redis.Messaging.Redis
{
    public class RedisBackgroundWorkerFactory : BackgroundWorkerFactory<DefaultHandlerConfiguration>
    {
        public RedisBackgroundWorkerFactory(IRedisClientsManager clientsManager, RedisMqServer redisMqServer)
        {            
            if (clientsManager == null)
            {
                throw new ArgumentNullException("clientsManager");
            }

            if (redisMqServer == null)
            {
                throw new ArgumentNullException("redisMqServer");
            }

            this.ClientsManager = clientsManager;
            this.RedisMqServer = redisMqServer;
        }

        public IRedisClientsManager ClientsManager { get; private set; }
        public RedisMqServer RedisMqServer { get; set; }

        protected internal override IMessageHandlerBackgroundWorker CreateMessageHandlerWorker(DefaultHandlerConfiguration messageHandlerConfiguration,
                                                                                               string queueName, Action<IMessageHandlerBackgroundWorker, Exception> errorHandler)
        {
            return new RedisMessageHandlerWorker(
                this.ClientsManager,
                messageHandlerConfiguration.MessageHandlerFactory.CreateMessageHandler(), // messageHandler,
                queueName,
                errorHandler);
        }

        protected internal override IList<IQueueHandlerBackgroundWorker> CreateQueueHandlerWorkers(IDictionary<string, Type> messageQueueNames, IDictionary<Type, DefaultHandlerConfiguration> messageHandlerConfigurations,
                                                                    Action<IQueueHandlerBackgroundWorker, Exception> errorHandler)
        {
            return new List<IQueueHandlerBackgroundWorker>
                {
                    new RedisQueueHandlerWorker(this.ClientsManager, this.RedisMqServer, "RedisMq", errorHandler)
                };
        }
    }
}
