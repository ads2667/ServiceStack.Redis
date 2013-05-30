using System;
using Amazon.SQS.Model;
using Moq;
using NUnit.Framework;
using ServiceStack.Aws.Messaging;
using ServiceStack.Aws.Messaging.Data;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Tests.Messaging
{
    [TestFixture]
    public class AwsSqsMessageHandlerRegisterTests
    {
        public AwsSqsMessageHandlerRegisterTests()
        {
            this.MessageServer = new Mock<IMessageService>(MockBehavior.Strict);
            this.SqsClient = new Mock<ISqsClient>(MockBehavior.Strict);
            this.MessageStateRepository = new Mock<IMessageStateRepository>(MockBehavior.Strict);
            this.MessageHandlerRegister = new AwsSqsMessageHandlerRegister(this.MessageServer.Object, this.SqsClient.Object, this.MessageStateRepository.Object);
        }

        protected AwsSqsMessageHandlerRegister MessageHandlerRegister { get; set; }

        protected Mock<IMessageStateRepository> MessageStateRepository { get; set; }

        protected Mock<ISqsClient> SqsClient { get; set; }

        protected Mock<IMessageService> MessageServer { get; set; }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullMessageServer()
        {
            new AwsSqsMessageHandlerRegister(null, this.SqsClient.Object, this.MessageStateRepository.Object);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullSqsClient()
        {
            new AwsSqsMessageHandlerRegister(this.MessageServer.Object, null, this.MessageStateRepository.Object);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullMessageStateRepository()
        {
            new AwsSqsMessageHandlerRegister(this.MessageServer.Object, this.SqsClient.Object, null);
        }

        [Test]
        public void CtorCorrectlyAssignsArgsToProperties()
        {
            // Assert.AreSame(this.MessageServer.Object, this.MessageHandlerRegister.MessageServer);
            Assert.AreSame(this.SqsClient.Object, this.MessageHandlerRegister.SqsClient);
            Assert.AreSame(this.MessageStateRepository.Object, this.MessageHandlerRegister.MessageStateRepository);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnPreMessageProcessedWithNullMessageThrowsEx()
        {
            this.MessageHandlerRegister.OnPreMessageProcessed(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnMessageProcessedSuccessfullyWithNullMessageThrowsEx()
        {
            this.MessageHandlerRegister.OnMessageProcessed(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void OnMessageProcessingFailedWithNullMessageThrowsEx()
        {
            this.MessageHandlerRegister.OnMessageProcessingFailed(null, 0, false);
        }

        [Test]
        public void OnPreMessageProcessedReturnsFalseIfMessageHasPassedItsExpiryTime()
        {
            var message = new SqsMessage
                {
                    MessageId = Guid.NewGuid().ToString(),
                    QueueName = "TestQueue",
                    QueueUrl = "http://testqueue.com/",
                    ReceiptHandle = "ABC123",
                    VisibilityTimeout = 10,
                    MessageExpiryTimeUtc = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(5))
                };

            var result = this.MessageHandlerRegister.OnPreMessageProcessed(message);
            Assert.IsFalse(result);
        }

        [Test]
        public void OnPreMessageProcessedExecutesMessageStateRepositoryCanProcessMessageMethod()
        {
            var message = new SqsMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                QueueName = "TestQueue",
                QueueUrl = "http://testqueue.com/",
                ReceiptHandle = "ABC123",
                VisibilityTimeout = 10,
                MessageExpiryTimeUtc = DateTime.UtcNow.AddSeconds(10)
            };
            
            this.MessageStateRepository
                .Expect(x => x.CanProcessMessage(message.MessageId))
                .Returns(true)
                .Verifiable();

            var result = this.MessageHandlerRegister.OnPreMessageProcessed(message);

            Assert.IsTrue(result);
            this.MessageStateRepository.VerifyAll();
        }

        [Test]
        public void OnMessageProcessedDeletesMessageAndUpdatesMessageState()
        {
            var message = new SqsMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                QueueName = "TestQueue",
                QueueUrl = "http://testqueue.com/",
                ReceiptHandle = "ABC123",
                VisibilityTimeout = 10,
                MessageExpiryTimeUtc = DateTime.UtcNow.AddSeconds(10)
            };

            this.SqsClient
                .Expect(x => x.DeleteMessage(message.QueueUrl, message.ReceiptHandle))
                .Verifiable();

            this.MessageStateRepository
                .Expect(x => x.MessageProcessingSucceeded(message.MessageId))
                .Verifiable();

            this.MessageHandlerRegister.OnMessageProcessed(message);

            this.SqsClient.VerifyAll();
            this.MessageStateRepository.VerifyAll();
        }

        [Test]
        public void OnMessageProcessingFailedUpdatesMessageVisibilityTimeoutAndUpdatesMessageState()
        {
            var message = new SqsMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                QueueName = "TestQueue",
                QueueUrl = "http://testqueue.com/",
                ReceiptHandle = "ABC123",
                VisibilityTimeout = 10,
                MessageExpiryTimeUtc = DateTime.UtcNow.AddSeconds(10)
            };

            this.SqsClient
                .Expect(x => x.ChangeMessageVisibility(message.QueueUrl, message.ReceiptHandle, 5))
                .Returns(new ChangeMessageVisibilityResponse())
                .Verifiable();

            this.MessageStateRepository
                .Expect(x => x.MessageProcessingFailed(message.MessageId))
                .Verifiable();

            this.MessageHandlerRegister.OnMessageProcessingFailed(message, 3, false);

            this.SqsClient.VerifyAll();
            this.MessageStateRepository.VerifyAll();
        }

        /// <summary>
        /// Helper class for testing.
        /// </summary>
        private class SqsMessage : ISqsMessage
        {
            public string MessageId { get; set; }
            public string ReceiptHandle { get; set; }
            public string QueueUrl { get; set; }
            public string QueueName { get; set; }
            public decimal VisibilityTimeout { get; set; }
            public DateTime MessageExpiryTimeUtc { get; set; }
        }
    }
}
