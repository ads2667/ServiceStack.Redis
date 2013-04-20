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
	public abstract class MessageProducer
		: IMessageProducer 
	{		
		private readonly Action onPublishedCallback;

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageProducer"/> class.
        /// </summary>
        /// <param name="onPublishedCallback">A callback method to be executed after a message is published, can be null.</param>
	    protected MessageProducer(Action onPublishedCallback)
		{
			this.onPublishedCallback = onPublishedCallback;
		}

        /*
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
        */

	    protected abstract string GetQueueNameOrUrl<T>(IMessage<T> message);

		public void Publish<T>(T messageBody)
		{
            if (typeof(IMessage<T>).IsAssignableFrom(typeof(T)))
                Publish((IMessage<T>)messageBody);
            else
                Publish((IMessage<T>)new Message<T>(messageBody));
        }

		public void Publish<T>(IMessage<T> message)
		{
		    this.PublishMessage(message);						
			if (onPublishedCallback != null)
			{
				onPublishedCallback();
			}
		}

	    protected abstract void PublishMessage<T>(IMessage<T> message);
        /*
            var messageBytes = message.ToBytes();
		    this.ReadWriteClient.LPush(message.ToInQueueName(), messageBytes);
		    this.ReadWriteClient.Publish(QueueNames.TopicIn, message.ToInQueueName().ToUtf8Bytes());
        */

	    public abstract void Dispose();
	}
}