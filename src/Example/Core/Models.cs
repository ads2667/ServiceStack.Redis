using ServiceStack.Aws.Messaging;

namespace Example.Core
{
    public class FailingMessage : SqsMessageBody
    {
        public string Text { get; set; }
    }

    public class Hello : SqsMessageBody
    {
        public string Text { get; set; }
    }

    public class Hello2 : SqsMessageBody
    {
        public string Text { get; set; }
    }

    public class Hello3 : SqsMessageBody
    {
        public string Text { get; set; }
    }
    
    public class Hello3Response : SqsMessageBody
    {
        public string ResponseText { get; set; }
    }

    public class Hello4 : SqsMessageBody
    {
        public string Text { get; set; }
    }

    public class Hello4Response : SqsMessageBody
    {
        public string ResponseText { get; set; }
    }
}
