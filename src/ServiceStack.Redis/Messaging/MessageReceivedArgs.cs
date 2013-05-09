using System;

namespace ServiceStack.Redis.Messaging
{
    public class MessageReceivedArgs
    {
        public MessageReceivedArgs()
        {
            this.Id = Guid.NewGuid();
        }

        /// <summary>
        /// Gets a unique Id for this instance of <see cref="MessageReceivedArgs"/>.
        /// </summary>
        public Guid Id { get; private set; }

        public Type MessageType { get; set; }

        public string QueueName { get; set; }

        public string MessageId { get; set; }

        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
        /// </returns>
        /// <param name="obj">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>. </param><filterpriority>2</filterpriority>
        public override bool Equals(object obj)
        {
            var compare = obj as MessageReceivedArgs;
            if (compare == null)
            {
                return false;
            }

            return this.Id.Equals(compare.Id);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return this.Id.GetHashCode();
        }
    }
}
