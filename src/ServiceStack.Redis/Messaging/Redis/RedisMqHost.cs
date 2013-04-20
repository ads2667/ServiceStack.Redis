using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging.Redis
{
    /// <summary>
    /// Creates an MQ Host that processes all messages on a single background thread. 
    /// i.e. If you register 3 handlers it will only create 1 background thread.
    /// 
    /// The same background thread that listens to the Redis MQ Subscription for new messages 
    /// also cycles through each registered handler processing all pending messages one-at-a-time:
    /// first in the message PriorityQ, then in the normal message InQ.
    /// 
    /// The Start/Stop methods are idempotent i.e. It's safe to call them repeatedly on multiple threads 
    /// and the Redis MQ Host will only have Started/Stopped once.
    /// </summary>
    public class RedisMqHost : MqHost
    {
        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory
        
        public override IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager);
        }

        public RedisMqHost(IRedisClientsManager clientsManager,
            int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null)
            : base(new RedisMessageFactory(clientsManager), retryCount, requestTimeOut)
        {
            this.clientsManager = clientsManager;            
        }

        protected override void StopMqService()
        {
            //Unblock current bgthread by issuing StopCommand
            try
            {
                using (var redis = clientsManager.GetClient())
                {
                    redis.PublishMessage(QueueNames.TopicIn, WorkerStatus.StopCommand);
                }
            }
            catch (Exception ex)
            {
                if (this.ErrorHandler != null) this.ErrorHandler(ex);
                Log.Warn("Could not send STOP message to bg thread: " + ex.Message);
            }
        }
    }

}
