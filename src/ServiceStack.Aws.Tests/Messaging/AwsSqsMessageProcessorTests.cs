using System;
using Amazon.SQS.Model;
using Moq;
using NUnit.Framework;
using ServiceStack.Aws.Messaging;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Tests.Messaging
{
    [TestFixture]
    public class AwsSqsMessageProcessorTests
    {
        public AwsSqsMessageProcessorTests()
        {
            this.SqsClient = new Mock<ISqsClient>(MockBehavior.Strict);
            this.MessageProcessor = new AwsSqsMessageProcessor(this.SqsClient.Object);
        }

        protected AwsSqsMessageProcessor MessageProcessor { get; set; }

        protected Mock<ISqsClient> SqsClient { get; set; }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullSqsClient()
        {
            new AwsSqsMessageProcessor(null);
        }

        [Test]
        public void CtorCorrectlyAssignsArgsToProperties()
        {
            Assert.AreSame(this.MessageProcessor.SqsClient, this.SqsClient.Object);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnPreMessageProcessedWithNullMessageThrowsEx()
        {
            this.MessageProcessor.CanProcessMessage((IMessage<int>)null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnMessageProcessedSuccessfullyWithNullMessageThrowsEx()
        {
            this.MessageProcessor.OnMessageProcessed((IMessage<int>)null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnMessageProcessingFailedWithNullMessageThrowsEx()
        {
            this.MessageProcessor.OnMessageProcessingFailed((IMessage<int>)null, null, false);
        }

        [Test]
        public void CanProcessMessageReturnsFalseIfMessageHasPassedItsExpiryTime()
        {
            var message = this.CreateNewMessage(DateTime.UtcNow.AddHours(-2));

            var result = this.MessageProcessor.CanProcessMessage(message);
            Assert.IsFalse(result);
        }

        [Test]
        public void CanProcessMessageReturnsTrueIfMessageHasNotPassedItsExpiryTime()
        {
            var message = this.CreateNewMessage();

            var result = this.MessageProcessor.CanProcessMessage(message);

            Assert.IsTrue(result);            
        }

        [Test]
        public void OnMessageProcessedDeletesMessageFromQueue()
        {
            var message = this.CreateNewMessage();
            var messageBody = message.GetBody();

            this.SqsClient
                .Setup(x => x.DeleteMessage(messageBody.QueueUrl, messageBody.ReceiptHandle))
                .Verifiable();

            this.MessageProcessor.OnMessageProcessed(message);

            this.SqsClient.VerifyAll();            
        }

        [Test]
        public void OnMessageProcessingFailedUpdatesMessageVisibilityIfNotMovingMessageToDlq()
        {
            var message = this.CreateNewMessage();
            var messageBody = message.GetBody();

            this.SqsClient
                .Setup(x => x.ChangeMessageVisibility(messageBody.QueueUrl, messageBody.ReceiptHandle, (decimal)this.MessageProcessor.GetMessageVisibilityTimeout(message).TotalSeconds))
                .Returns(new ChangeMessageVisibilityResponse())
                .Verifiable();

            this.MessageProcessor.OnMessageProcessingFailed(message, null, false);

            this.SqsClient.VerifyAll();
            this.SqsClient.Verify(x => x.ChangeMessageVisibility(messageBody.QueueUrl, messageBody.ReceiptHandle, (decimal)this.MessageProcessor.GetMessageVisibilityTimeout(message).TotalSeconds), Times.Once());
        }

        [Test]
        public void OnMessageProcessingFailedDoesNotUpdateMessageVisibilityIfMovingMessageToDlq()
        {
            var message = this.CreateNewMessage();
            var messageBody = message.GetBody();
            var queueNames = new VersionedQueueNames(messageBody.GetType());
            var dlqUrl = "http://queue.com/dlq/";

            // GetOrCreateQueue
            // PublishMessage (To DLQ) -> Test that a new MessageId is assigned!
            // Delete Message
            this.SqsClient
                .Setup(x => x.GetOrCreateQueueUrl(queueNames.Dlq))
                .Returns(dlqUrl)
                .Verifiable();

            this.SqsClient
                .Setup(x => x.PublishMessage(dlqUrl, Convert.ToBase64String(message.ToBytes())))
                .Returns(new SendMessageResponse())
                .Verifiable();

            this.SqsClient
                .Setup(x => x.DeleteMessage(messageBody.QueueUrl, messageBody.ReceiptHandle))
                .Verifiable();

            this.MessageProcessor.OnMessageProcessingFailed(message, null, true);
            
            this.SqsClient.VerifyAll();
            
            // Verify that 'UpdateMessageVisibility' is NOT called.
            this.SqsClient.Verify(x => x.ChangeMessageVisibility(messageBody.QueueUrl, messageBody.ReceiptHandle, It.IsAny<decimal>()), Times.Never());
        }

        private IMessage<SqsMessageBody> CreateNewMessage(DateTime expiryDateTime = default(DateTime))
        {
            return new Message<SqsMessageBody>(
                new SqsMessageBody
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        QueueName = "TestQueue",
                        QueueUrl = "http://testqueue.com/",
                        ReceiptHandle = "ABC123",
                        VisibilityTimeout = 10,
                        MessageExpiryTimeUtc = expiryDateTime == default(DateTime) ? DateTime.UtcNow.AddSeconds(10) : expiryDateTime
                    });
        }

        /// <summary>
        /// Helper class for testing.
        /// </summary>
        private class SqsMessageBody : ISqsMessageBody
        {
            public string MessageId { get; set; }
            public string ReceiptHandle { get; set; }
            public string QueueUrl { get; set; }
            public string QueueName { get; set; }
            public decimal VisibilityTimeout { get; set; }
            public int PreviousRetryAttempts { get; set; }
            public DateTime MessageExpiryTimeUtc { get; set; }
        }
    }
}
