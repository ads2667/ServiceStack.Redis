using System;
using System.IO;
using Example.Core;
using ServiceStack.Logging;

namespace Example.Server
{    
    class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (Program));

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo("Server.exe.config"));
            
            // For some reason, in a console app you need to set the LogFactory manually!
            LogManager.LogFactory = new ServiceStack.Logging.Log4Net.Log4NetFactory("Server.exe.config");
            var Log = LogManager.GetLogger(typeof(Program));
           
            Log.Info("===== Starting Program =====");
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
