using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Aws.Messaging;
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
        static void Main(string[] args)
        {
            IMessageQueueClient c;
            // var svc = new InMemoryTransientMessageService();
            // var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            var svc = new ServiceStack.Aws.Messaging.AwsSqsServer(new Amazon.SQS.AmazonSQSClient("192FKM4094Y33B3BGX02", "Bc3Q1VUOL+O/0Ll8V1OYy0JdvA0hbkvEoXOLki7/"));
            
            // Do it from the server, can use the server to pass to MessageFactory, to create other objects.
            IMessage<Hello> h;
            
            svc.RegisterMessageHandlers(register =>
                {
                    register.AddHandler<Hello>((m) =>
                    {
                        Console.WriteLine("Server Says: " + m.GetBody().Text);
                        // return m.GetBody();
                        return null;
                    });

                    register.AddHandler<Hello2>((m) =>
                    {
                        Console.WriteLine("Server Says: " + m.GetBody().Text);
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
                    Console.WriteLine("Server Says: " + m.GetBody().Text);
                    // return m.GetBody();
                    return null;
                });
            */

            Console.WriteLine("===== Message Queue Server Started =====");
            Console.WriteLine("Press ENTER to exit.");
            svc.Start();

            producer.Publish(new Hello{Text = "This comes from the client"});
            producer.Publish(new Hello { Text = "This comes from the client #2" });

            
            Console.ReadLine();            
        }

    }
}
