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
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
	public abstract class MessageProducer
		: IMessageProducer
	{
	    protected static ILog Log;

		private readonly Action onPublishedCallback;

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageProducer"/> class.
        /// </summary>
        /// <param name="onPublishedCallback">A callback method to be executed after a message is published, can be null.</param>
	    protected MessageProducer(Action onPublishedCallback)
		{
            Log = LogManager.GetLogger(this.GetType());
			this.onPublishedCallback = onPublishedCallback;
		}

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
        
	    public abstract void Dispose();
	}
}