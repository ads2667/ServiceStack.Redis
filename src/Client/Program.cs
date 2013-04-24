using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Core;
using ServiceStack.Aws.Messaging;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.Redis.Messaging.Redis;

namespace Playground
{    
    class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (Program));

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo("Client.exe.config"));
            // var Log4Net = log4net.LogManager.GetLogger(typeof(Program));

            // For some reason, in a console app you need to set the LogFactory manually!
            ServiceStack.Logging.LogManager.LogFactory = new ServiceStack.Logging.Log4Net.Log4NetFactory("Client.exe.config");
            var Log = LogManager.GetLogger(typeof(Program));
            /*
            Log.Info("Info");
            Log.Debug("Debug");
            Log.Warn("Warn");
            Log.Error("Error");
            Log.Fatal("Fatal");
            */

            Log.Info("===== Starting Client =====");
            IMessageQueueClient c;
            // var svc = new InMemoryTransientMessageService();
            // var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            var svc = MessageServerFactory.CreateMessageService();

            // Register the handlers before creating any client objects.
            var producer = svc.MessageFactory.CreateMessageProducer();
            
            Log.Info("===== Message Queue Server Started =====");
            Log.Info("Type 'EXIT' to close.");
            // svc.Start();

            // TODO: Figure out why 'GetAsync' is called when the server starts for the AWS Service....

            while (true)
            {
                producer.Publish(new Hello {Text = "This comes from the client"});
                producer.Publish(new Hello2 {Text = "This comes from the client #2"});

                var text = Console.ReadLine() ?? string.Empty;
                if (text.ToUpperInvariant() == "EXIT")
                {
                    break;
                }
            }

            Log.Info("Stopping Client.");            
        }

    }
}
