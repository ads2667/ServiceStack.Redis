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

namespace ServiceStack.Redis.Messaging
{
	public abstract class MessageQueueClient
		: IMessageQueueClient
	{
		private readonly Action onPublishedCallback;
		// private readonly IRedisClientsManager clientsManager;

        public int MaxSuccessQueueSize { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageQueueClient"/> class.
        /// </summary>
        /// <param name="onPublishedCallback">A callback method to be executed after a message is published, can be null.</param>
        protected MessageQueueClient(Action onPublishedCallback)
		{
			this.onPublishedCallback = onPublishedCallback;
		    this.MaxSuccessQueueSize = 100;
		}

        protected abstract string GetQueueNameOrUrl(string queueName);

		public void Publish<T>(T messageBody)
		{
            if (typeof(IMessage).IsAssignableFrom(typeof(T)))
                Publish((IMessage)messageBody);
            else
                Publish<T>(new Message<T>(messageBody));
        }

        public void Publish(IMessage message)
        {
            var messageBytes = message.ToBytes();
            var queueName = this.GetQueueName(message);
            Publish(queueName, messageBytes);
        }

        public void Publish<T>(IMessage<T> message)
        {
            var queueName = this.GetQueueName(message);
            var messageBytes = message.ToBytes();
            Publish(queueName, messageBytes);
        }

		public void Publish(string queueName, byte[] messageBytes)
		{
            this.PublishMessage(queueName, messageBytes);			
			if (onPublishedCallback != null)
			{
				onPublishedCallback();
			}
		}

	    protected abstract string GetQueueName(IMessage message);

        protected abstract string GetQueueName<T>(IMessage<T> message);

        protected abstract void PublishMessage(string queueName, byte[] messageBytes);

	    public abstract void Notify(string queueName, byte[] messageBytes);
        /*
		{
			this.ReadWriteClient.LPush(queueName, messageBytes);
            this.ReadWriteClient.LTrim(queueName, 0, this.MaxSuccessQueueSize);
			this.ReadWriteClient.Publish(QueueNames.TopicOut, queueName.ToUtf8Bytes());
		}
        */

	    public abstract byte[] Get(string queueName, TimeSpan? timeOut);
        /*
		{
			var unblockingKeyAndValue = this.ReadOnlyClient.BRPop(queueName, (int) timeOut.GetValueOrDefault().TotalSeconds);
            return unblockingKeyAndValue.Length != 2 
                ? null 
                : unblockingKeyAndValue[1];
		}
        */

	    public abstract byte[] GetAsync(string queueName);
        /*
		{
			return this.ReadOnlyClient.RPop(queueName);
		}
        */

	    public abstract string WaitForNotifyOnAny(params string[] channelNames);
        /*
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
        */
	    public abstract void Dispose();
	}
}