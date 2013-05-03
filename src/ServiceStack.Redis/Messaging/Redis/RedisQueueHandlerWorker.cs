using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging.Redis
{
    public class RedisQueueHandlerWorker : QueueHandlerBackgroundWorker
    {
        public RedisMqServer MqServer { get; set; }
        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public RedisQueueHandlerWorker(IRedisClientsManager clientsManager, RedisMqServer mqServer, string queueName, Action<IQueueHandlerBackgroundWorker, Exception> errorHandler) 
            : base(queueName, errorHandler)
        {            
            if (clientsManager == null) throw new ArgumentNullException("clientsManager");
            if (mqServer == null) throw new ArgumentNullException("mqServer");
            this.clientsManager = clientsManager;
            this.MqServer = mqServer;
        }

        protected override void RunLoop()
        {
            using (var redisClient = clientsManager.GetReadOnlyClient())
            {                
                using (var subscription = redisClient.CreateSubscription())
                {
                    subscription.OnUnSubscribe = channel => Log.Debug("OnUnSubscribe: " + channel);

                    subscription.OnMessage = (channel, msg) =>
                    {

                        if (msg == WorkerStatus.StopCommand)
                        {
                            Log.Debug("Stop Command Issued");

                            if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Started) != WorkerStatus.Started)
                                Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping);

                            Log.Debug("UnSubscribe From All Channels...");
                            subscription.UnSubscribeFromAllChannels(); //Un block thread.
                            return;
                        }
                        
                        this.MqServer.NotifyMessageHandlerWorkers(msg);
                        this.IncrementMessageCount(1);
                        /*
                        // this.Server.NotifyMessageHandlerWorkers(msg);
                        // Just need to do a little reading on events in multi-threaded environements...
                        if (!string.IsNullOrEmpty(msg))
                        {
                            int[] workerIndexes;
                            if (queueWorkerIndexMap.TryGetValue(msg, out workerIndexes))
                            {
                                foreach (var workerIndex in workerIndexes)
                                {
                                    messageWorkers[workerIndex].NotifyNewMessage();
                                }
                            }
                        }
                        */
                    };

                    subscription.SubscribeToChannels(QueueNames.TopicIn); //blocks thread
                }

                // Only needs to be called on the main thread, not from any queue handler worker.
                // StopWorkerThreads();
            }
        }

        protected override void OnStop()
        {
            using (var redis = clientsManager.GetClient())
            {
                redis.PublishMessage(QueueNames.TopicIn, WorkerStatus.StopCommand);
            }
        }

        public override IQueueHandlerBackgroundWorker CloneBackgroundWorker()
        {
            return new RedisQueueHandlerWorker(clientsManager, this.MqServer, this.QueueName, this.ErrorHandler);
        }

        /*
        protected override IMessageQueueClient CreateMessageQueueClient()
        {
           return new RedisMessageQueueClient(clientsManager);
        }
        */
    }
}
