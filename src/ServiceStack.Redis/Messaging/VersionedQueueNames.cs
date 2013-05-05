using System;

namespace ServiceStack.Redis.Messaging
{
    /// <summary>
    /// Util class to create unique versioned queue names for runtime types
    /// </summary>
    public class VersionedQueueNames
    {
        public static string TopicIn = "mq:topic:in";
        public static string TopicOut = "mq:topic:out";
        public static string QueuePrefix = "";

        public static void SetQueuePrefix(string prefix)
        {
            TopicIn = prefix + "mq:topic:in";
            TopicOut = prefix + "mq:topic:out";
            QueuePrefix = prefix;
        }

        private readonly Type messageType;
        private readonly string messageVersion;

        public VersionedQueueNames(Type messageType)
        {
            this.messageType = messageType;
            this.messageVersion = messageType.Assembly.GetName().Version.ToString();
        }

        public string Priority
        {
            get { return string.Format("{0}mq:{1}:{2}.priorityq", QueuePrefix, messageType.Name, this.messageVersion); }
        }

        public string In
        {
            get { return string.Format("{0}mq:{1}:{2}.inq", QueuePrefix, messageType.Name, this.messageVersion); }
        }

        public string Out
        {
            get { return string.Format("{0}mq:{1}:{2}.outq", QueuePrefix, messageType.Name, this.messageVersion); }
        }

        public string Dlq
        {
            get { return string.Format("{0}mq:{1}:{2}.dlq", QueuePrefix, messageType.Name, this.messageVersion); }
        }
    }

}
