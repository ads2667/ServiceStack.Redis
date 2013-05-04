﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Aws.Messaging;
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
            var svc = new AwsSqsServer(new SqsClient(new Amazon.SQS.AmazonSQSClient("AKIAI32WJMKWXTRJ6EHQ", "pjpRGOLvT0WsHrXC0DcKaSENKaNygJKs9zJg1TeG")));

            // TODO: Use customer registration to override default values
            // svc.RegisterMessageHandlers(register => register.AddPooledHandler());
            
            return RegisterMessageHandlers(svc);
        }

        //private static IMessageService RegisterMessageHandlers<T>(MqServer2<T> messageService) 
        //    where T : MessageHandlerRegister
        private static IMessageService RegisterMessageHandlers(MqServer2 messageService) 
        {            
            // TODO: Add all 'GetStats' to the MqHandlers
            // TODO: Verify that handlers have been registered before creating any clients/server
            // TODO: Code verification checks that a MQ exists before a msg is sent, and when a svr is started
            // TODO: Code Graceful Shutdown of all worker threads, log msgs, should take ~30secs to stop
            // TODO: Create QueueWorker Stats, display stats on console when closing console.
            // TODO: Need to refactor QueueHandlers with 'noContinuosErrors' etc... remove from main thread.
            messageService.RegisterMessageHandlers(register =>
                {
                    // TODO: Create [Thread]PooledWorkerHandler!?!
                    // register.AddPooledHandler<TypeName>(() => );

                    // Standard Background Message Handlers
                    // register.AddHandler<Hello>((m) =>
                    register.AddPooledHandler<Hello>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        return null;
                    }, null);

                    register.AddHandler<Hello2>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        
                        // No Response
                        return null;
                    });
                   
                    register.AddHandler<Hello3>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);

                        // The client needs to 'Get()' the response -> How to register/create queue
                        // What if the client calls 'GetAsync()' directly? Currently, it won't work!
                        return new Hello3Response { ResponseText = "The message was processed by the server!" };
                    });
                    
                    register.AddHandler<Hello4>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        return new Hello4Response {ResponseText = "Hello4 Response Text"};
                    });
            });

            return messageService;
        }
    }
}
