using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Aws.Messaging;
using ServiceStack.Messaging;

namespace Messaging.Core
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
