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
            var messageQueueClient = svc.MessageFactory.CreateMessageQueueClient();
            
            Log.Info("===== Message Queue Client Started =====");
            Log.Info("Type 'EXIT' to close.");
            // svc.Start();

            // TODO: Figure out why 'GetAsync' is called when the server starts for the AWS Service....

            while (true)
            {
                // The MqClient should result in a message in the out q, as the messages are not configured to be one-way
                messageQueueClient.Publish(new Hello {Text = "This comes from the client"});

                // This message should return a response.
                messageQueueClient.Publish(new Hello3 {Text = "This comes from the client #3" });

                Log.Info("Waiting for message response.");
                var responseQueueName = QueueNames<Hello3Response>.In;
                var response = messageQueueClient.Get(responseQueueName, TimeSpan.FromSeconds(20));
                if (response != null)
                {
                    var obj = response.ToMessage<Hello3Response>().GetBody();
                    Log.InfoFormat("Message response received: {0}", obj.ResponseText);
                    ((AwsSqsMessageQueueClient)messageQueueClient).DeleteMessageFromQueue(responseQueueName, obj.ReceiptHandle);
                    Log.InfoFormat("Deleted response message from queue: {0}", obj.QueueName);
                }
                else
                {
                    Log.Info("No message response received.");
                }
                
                // ================== REQ/REPLY MQ ===============
                var uniqueCallbackQ = "MyUniqueQueueName";
                messageQueueClient.Publish(new Message<Hello4>(new Hello4 { Text = "This comes from the client #4" })
                    {
                        ReplyTo = uniqueCallbackQ
                    });

                Log.Info("Waiting for message response.");
                var uniqueResponse = messageQueueClient.Get(uniqueCallbackQ, TimeSpan.FromSeconds(20));
                if (uniqueResponse != null)
                {
                    var obj = uniqueResponse.ToMessage<Hello4Response>().GetBody();
                    Log.InfoFormat("Message response received: {0}", obj.ResponseText);
                    ((AwsSqsMessageQueueClient)messageQueueClient).DeleteMessageFromQueue(uniqueCallbackQ, obj.ReceiptHandle);
                    Log.InfoFormat("Deleted response message from queue: {0}", obj.QueueName);
                }
                else
                {
                    Log.Info("No message response received.");
                }

                // TODO: Optionally, delete the mq?

                // ===============================================

                // TODO: TEST - messageQueueClient.WaitForNotifyOnAny()

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
