using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.Redis.Messaging.Redis;

namespace Messaging.Core
{
    public class MessageServerFactory
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MessageServerFactory));

        public static IMessageService CreateMessageService()
        {
            return CreateAwsMessageService();
        }

        private static IMessageService CreateRedisMessageService()
        {
            var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            return RegisterMessageHandlers(svc);
        }

        private static IMessageService CreateAwsMessageService()
        {
            var svc = new ServiceStack.Aws.Messaging.AwsSqsServer(new Amazon.SQS.AmazonSQSClient(null, null));            
            return RegisterMessageHandlers(svc);
        }

        private static IMessageService RegisterMessageHandlers(MqServer2 messageService)
        {            
            // TODO: Create Handler Wrappers to delete messages when processed successfully.
            messageService.RegisterMessageHandlers(register =>
            {
                register.AddHandler<Hello>((m) =>
                {
                    Log.Debug("Server Says: " + m.GetBody().Text);
                    // return m.GetBody();                        
                    return null;
                });

                register.AddHandler<Hello2>((m) =>
                {
                    Log.Debug("Server Says: " + m.GetBody().Text);
                    // return m.GetBody();
                    return null;
                });
            });

            return messageService;
        }
    }
}
