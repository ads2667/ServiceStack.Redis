//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2010 Liquidbit Ltd.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System;
using ServiceStack.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging.Redis
{
	public class RedisMessageProducer : MessageProducer 
	{
		private readonly IRedisClientsManager clientsManager;
		
		public RedisMessageProducer(IRedisClientsManager clientsManager)
			: this(clientsManager, null) {}

		public RedisMessageProducer(IRedisClientsManager clientsManager, Action onPublishedCallback)
            : base(onPublishedCallback)
		{
			this.clientsManager = clientsManager;
		}

		private IRedisNativeClient readWriteClient;
		public IRedisNativeClient ReadWriteClient
		{
			get
			{
				if (this.readWriteClient == null)
				{
					this.readWriteClient = (IRedisNativeClient)clientsManager.GetClient();
				}
				return readWriteClient;
			}
		}

	    protected override void PublishMessage<T>(IMessage<T> message)
	    {
            var messageBytes = message.ToBytes();
            this.ReadWriteClient.LPush(this.GetQueueNameOrUrl(message), messageBytes);
            this.ReadWriteClient.Publish(QueueNames.TopicIn, message.ToInQueueName().ToUtf8Bytes());
	    }

        protected override string GetQueueNameOrUrl<T>(IMessage<T> message)
        {
            return message.ToInQueueName();
        }

	    public override void Dispose()
		{
			if (readWriteClient != null)
			{
				readWriteClient.Dispose();
			}
		}
	}
}