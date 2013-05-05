using System;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Messaging
{
    public interface IMessageCoordinator
    {
        void EnqueMessage(string queueName, IMessage message, Type messageType);

        IMessage DequeMessage(string queueName);
    }
}
