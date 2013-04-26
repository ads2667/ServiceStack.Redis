using System;
using System.IO;
using Messaging.Core;
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace Server
{    
    class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (Program));

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo("Server.exe.config"));
            // var Log4Net = log4net.LogManager.GetLogger(typeof(Program));

            // For some reason, in a console app you need to set the LogFactory manually!
            ServiceStack.Logging.LogManager.LogFactory = new ServiceStack.Logging.Log4Net.Log4NetFactory("Server.exe.config");
            var Log = LogManager.GetLogger(typeof(Program));
           
            Log.Info("===== Starting Program =====");
            IMessageQueueClient c;
            // var svc = new InMemoryTransientMessageService();
            // var svc = new RedisMqServer(new PooledRedisClientManager(new[] { "localhost:6379" }));
            var svc = MessageServerFactory.CreateMessageService();
            
            Log.Info("===== Message Queue Server Started =====");
            Log.Info("Press ENTER to exit.");
            svc.Start();

            Console.ReadLine();

            Log.Info("Stopping server...");
            svc.Stop();
            Log.Info("Server stopped. Press enter to close.");
            Console.ReadLine();
        }

    }
}
