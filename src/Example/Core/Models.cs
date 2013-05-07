using ServiceStack.Aws.Messaging;

namespace Example.Core
{    
    public class Hello : SqsMessage
    {
        public string Text { get; set; }
    }

    public class Hello2 : SqsMessage
    {
        public string Text { get; set; }
    }

    public class Hello3 : SqsMessage
    {
        public string Text { get; set; }
    }
    
    public class Hello3Response : SqsMessage
    {
        public string ResponseText { get; set; }
    }

    public class Hello4 : SqsMessage
    {
        public string Text { get; set; }
    }

    public class Hello4Response : SqsMessage
    {
        public string ResponseText { get; set; }
    }
}
