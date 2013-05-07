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

        public override HandlerRegistration<DefaultHandlerConfiguration> RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, DefaultHandlerConfiguration handlerConfiguration)
        {
            return new HandlerRegistration<DefaultHandlerConfiguration>(this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx), handlerConfiguration);
        }
    }
}
