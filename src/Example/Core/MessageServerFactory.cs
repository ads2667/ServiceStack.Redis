using System;
using ServiceStack.Aws.Messaging;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.Redis.Messaging.Redis;

namespace Example.Core
{
    public class MessageServerFactory
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MessageServerFactory));

        public static IMessageService CreateMessageService()
        {
            // return CreateRedisMessageService();
            return CreateAwsMessageService();
        }

        private static IMessageService CreateRedisMessageService()
        {
            var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            return RegisterMessageHandlers(svc);
        }

        private static IMessageService CreateAwsMessageService()
        {
            var svc = new AwsSqsServer(new SqsClient(new Amazon.SQS.AmazonSQSClient("AKIAIZ65P5ZEGIU3X2FA", "OeGi9c0lH3791+SBZkTTuDtuIg+t5TNOE6oLjCrf")));

            // TODO: Use customer registration to override default values            
            return RegisterMessageHandlers(svc);

            /********* Custom AWS Handler Configuration **********
            svc.RegisterMessageHandlers(register =>
                {
                    register.AddPooledHandler<Hello>((m) =>
                        {
                            Log.Debug("Server Says: " + m.GetBody().Text);
                            return null;
                        }, new AwsSqsHandlerConfiguration() null, null, null, null, null, null); //// Override default values.

                    register.AddHandler<Hello2>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        
                        // No Response
                        return null;
                    });
                   
                });
              
             return svc;
            */
        }

        private static IMessageService RegisterMessageHandlers<T, Th, TBg>(MqServer2<T, Th, TBg> messageService) 
            where T : DefaultHandlerConfiguration, new() // MessageHandlerRegister<Th>
            where Th : MessageHandlerRegister<T>
            where TBg : BackgroundWorkerFactory<T> // private static IMessageService RegisterMessageHandlers(MqServer2 messageService) 
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
                        // return null;
                    });

                    register.AddPooledHandler<FailingMessage>((m) =>
                    {
                        throw new InvalidOperationException("Fail. Should be moved to DLQ.");
                    });

                    register.AddHandler<Hello2>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        
                        // No Response
                        // return null; // TODO: Null should not be required if no response type is defined.
                    });

                    register.AddHandler<Hello3, Hello3Response>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);

                        // The client needs to 'Get()' the response -> How to register/create queue
                        // What if the client calls 'GetAsync()' directly? Currently, it won't work!
                        return new Hello3Response { ResponseText = "The message was processed by the server!" };
                    });

                    register.AddHandler<Hello4, Hello4Response>((m) =>
                    {
                        Log.Debug("Server Says: " + m.GetBody().Text);
                        return new Hello4Response {ResponseText = "Hello4 Response Text"};
                    });
            });

            return messageService;
        }
    }
}
