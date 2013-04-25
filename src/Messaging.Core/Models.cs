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

    public class Hello3
    {
        public string Text { get; set; }
    }
}
