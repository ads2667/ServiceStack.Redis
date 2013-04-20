//////
////// https://github.com/mythz/ServiceStack.Redis
////// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//////
////// Authors:
//////   Demis Bellot (demis.bellot@gmail.com)
//////
////// Copyright 2010 Liquidbit Ltd.
//////
////// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//////

////using System;
////using ServiceStack.Messaging;

////namespace ServiceStack.Redis.Messaging
////{
////    /// <summary>
////    /// Transient message queues are a one-pass message queue service that starts
////    /// processing messages when Start() is called. Any subsequent Start() calls 
////    /// while the service is running is ignored.
////    /// 
////    /// The transient service will continue to run until all messages have been 
////    /// processed after which time it will shutdown all processing until Start() is called again.
////    /// </summary>
////    public abstract class TransientMessageFactory
////        : IMessageFactory
////    {
////        // public IRedisClientsManager ClientsManager { get; private set; }

////        public IMessageService MessageService { get; private set; }

////        /*
////        protected TransientMessageFactory()
////            : this(2, null)
////        {
////        }

////        protected TransientMessageFactory(int retryAttempts, TimeSpan? requestTimeOut)
////        {
////            // this.ClientsManager = clientsManager ?? new BasicRedisClientManager();
////            MessageService = new RedisTransientMessageService(
////                retryAttempts, requestTimeOut, this);
////        }
////        */

////        public abstract IMessageQueueClient CreateMessageQueueClient();
////        /*
////        {
////            return new RedisMessageQueueClient(this.ClientsManager, OnMessagePublished);
////        }
////        */

////        public abstract IMessageProducer CreateMessageProducer();
////        /*
////        {
////            return new RedisMessageProducer(this.ClientsManager, OnMessagePublished);
////        }
////        */

////        public abstract IMessageService CreateMessageService();
////        /*
////        {
////            return MessageService;
////        }
////        */

////        public void OnMessagePublished()
////        {
////            if (this.MessageService != null)
////            {
////                this.MessageService.Start();
////            }
////        }

////        public void Dispose()
////        {
////            if (this.MessageService != null)
////            {
////                this.MessageService.Dispose();
////                this.MessageService = null;
////            }

////            if (this.ClientsManager != null)
////            {
////                this.ClientsManager.Dispose();
////                this.ClientsManager = null;
////            }
////        }

////    }
////}