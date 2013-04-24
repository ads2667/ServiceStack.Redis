using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Aws.Messaging;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.Redis.Messaging.Redis;

namespace Playground
{
    public class Hello { public string Text { get; set; } }
    public class Hello2 { public string Text { get; set; } }

    class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (Program));

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo("Lab.exe.config"));
            // var Log4Net = log4net.LogManager.GetLogger(typeof(Program));

            // For some reason, in a console app you need to set the LogFactory manually!
            ServiceStack.Logging.LogManager.LogFactory = new ServiceStack.Logging.Log4Net.Log4NetFactory("Lab.exe.config");
            var Log = LogManager.GetLogger(typeof(Program));
            /*
            Log.Info("Info");
            Log.Debug("Debug");
            Log.Warn("Warn");
            Log.Error("Error");
            Log.Fatal("Fatal");
            */

            Log.Info("===== Starting Program =====");
            IMessageQueueClient c;
            // var svc = new InMemoryTransientMessageService();
            // var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            var svc = new ServiceStack.Aws.Messaging.AwsSqsServer(new Amazon.SQS.AmazonSQSClient("AKIAIORYIECWONNZHDOA", "NyBWos/5P/xqTpx7l7CN2XaldXjkLkHWIQtuqUnC"));
            
            // Do it from the server, can use the server to pass to MessageFactory, to create other objects.
            IMessage<Hello> h;
            
            svc.RegisterMessageHandlers(register =>
                {
                    register.AddHandler<Hello>((m) =>
                    {
                        Log.Info("Server Says: " + m.GetBody().Text);
                        // return m.GetBody();                        
                        return null;
                    });

                    register.AddHandler<Hello2>((m) =>
                    {
                        Log.Info("Server Says: " + m.GetBody().Text);
                        // return m.GetBody();
                        return null;
                    });
                });

            // TODO: ***** Throw exception if no handlers are registered, and server is started or client is created *****
            // TODO: Use the manager to create the client and server objects.
            // messageQueueManager.CreateMessageProducer()

            // Register the handlers before creating any client objects.
            var producer = svc.MessageFactory.CreateMessageProducer();
            // var mq = svc.MessageFactory.CreateMessageQueueClient().Publish();
            /*
            // TODO: Before starting the server, verify that all queues exist, and get the queue URLs.
            // When creating the producer/client from the server; can the queue URL's be passed?  
            svc.RegisterHandler<Hello>((m) =>
                {
                    Log.Info("Server Says: " + m.GetBody().Text);
                    // return m.GetBody();
                    return null;
                });
            */

            Log.Info("===== Message Queue Server Started =====");
            Log.Info("Press ENTER to exit.");
            svc.Start();

            producer.Publish(new Hello {Text = "This comes from the client"});
            producer.Publish(new Hello2 { Text = "This comes from the client #2" });

            
            Console.ReadLine();            
        }

    }
}
