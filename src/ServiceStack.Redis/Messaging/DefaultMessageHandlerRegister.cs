using System;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
    public class DefaultMessageHandlerRegister : MessageHandlerRegister<DefaultHandlerConfiguration>
    {
        public DefaultMessageHandlerRegister(IMessageService messageServer, int retryCount)
            : base(messageServer, retryCount)
        {
        }

        public override DefaultHandlerConfiguration RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int noOfThreads)
        {
            return new DefaultHandlerConfiguration(this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx), noOfThreads);
        }
    }
}
