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
            var svc = new ServiceStack.Aws.Messaging.AwsSqsServer(new Amazon.SQS.AmazonSQSClient("AKIAIORYIECWONNZHDOA", "NyBWos/5P/xqTpx7l7CN2XaldXjkLkHWIQtuqUnC"));            
            return RegisterMessageHandlers(svc);
        }

        private static IMessageService RegisterMessageHandlers(MqServer2 messageService)
        {            
            // TODO: Code verification checks that a MQ exists before a msg is sent, and when a svr is started
            // TODO: Code Graceful Shutdown of all worker threads, log msgs, should take ~30secs to stop
            // TODO: Create QueueWorker Stats
            // TODO: Need to refactor QueueHandlers with 'noContinuosErrors' etc...
            // TODO: Create Handler Wrappers to delete messages when processed successfully for AWS.            
            messageService.RegisterMessageHandlers(register =>
                {
                    // TODO: Create [Thread]PooledWorkerHandler!?!
                    // register.AddPooledHandler<TypeName>(() => );

                    // Standard Background Message Handlers
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

                    /*
                    register.AddHandler<Hello3>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        // return m.GetBody();
                        return null;
                    });
                    */
            });

            return messageService;
        }
    }
}
