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
	public class RedisMessageQueueClient : MessageQueueClient
	{
		private readonly IRedisClientsManager clientsManager;
       
		public RedisMessageQueueClient(IRedisClientsManager clientsManager)
			: this(clientsManager, null) {}

		public RedisMessageQueueClient(
			IRedisClientsManager clientsManager, Action onPublishedCallback)
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

		private IRedisNativeClient readOnlyClient;
		public IRedisNativeClient ReadOnlyClient
		{
			get
			{
				if (this.readOnlyClient == null)
				{
					this.readOnlyClient = (IRedisNativeClient)clientsManager.GetReadOnlyClient();
				}
				return readOnlyClient;
			}
		}

        protected override string GetQueueNameOrUrl(string queueName)
        {
            return queueName;
        }

	    protected override string GetInQueueName(IMessage message)
	    {
	        return message.ToInQueueName();
	    }

	    protected override string GetInQueueName<T>(IMessage<T> message)
	    {
            return message.ToInQueueName();
	    }

	    protected override void PublishMessage(string queueName, byte[] messageBytes)
	    {
            this.ReadWriteClient.LPush(queueName, messageBytes);
            this.ReadWriteClient.Publish(QueueNames.TopicIn, queueName.ToUtf8Bytes());
	    }

	    public override void Notify(string queueName, byte[] messageBytes)
		{
			this.ReadWriteClient.LPush(queueName, messageBytes);
            this.ReadWriteClient.LTrim(queueName, 0, this.MaxSuccessQueueSize);
			this.ReadWriteClient.Publish(QueueNames.TopicOut, queueName.ToUtf8Bytes());
		}

		public override byte[] Get(string queueName, TimeSpan? timeOut)
		{
			var unblockingKeyAndValue = this.ReadOnlyClient.BRPop(queueName, (int) timeOut.GetValueOrDefault().TotalSeconds);
            return unblockingKeyAndValue.Length != 2 
                ? null 
                : unblockingKeyAndValue[1];
		}

		public override byte[] GetAsync(string queueName)
		{
			return this.ReadOnlyClient.RPop(queueName);
		}

		public override string WaitForNotifyOnAny(params string[] channelNames)
		{
			string result = null;
			var subscription = new RedisSubscription(readOnlyClient);
			subscription.OnMessage = (channel, msg) => {
				result = msg;
				subscription.UnSubscribeFromAllChannels();
			};
			subscription.SubscribeToChannels(channelNames); //blocks
			return result;
		}

		public override void Dispose()
		{
			if (this.readOnlyClient != null)
			{
				this.readOnlyClient.Dispose();
			}
			if (this.readWriteClient != null)
			{
				this.readWriteClient.Dispose();
			}
		}
	}
}