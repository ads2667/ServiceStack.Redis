using System;
using Moq;
using NUnit.Framework;
using ServiceStack.Aws.Messaging;
using ServiceStack.Messaging;

namespace ServiceStack.Aws.Tests.Messaging
{
    [TestFixture]
    public class AwsSqsMessageHandlerRegisterTests
    {
        public AwsSqsMessageHandlerRegisterTests()
        {
            this.MessageServer = new Mock<IMessageService>(MockBehavior.Strict);
            this.MessageProcessor = new Mock<IMessageProcessor>(MockBehavior.Strict);
            this.MessageHandlerRegister = new AwsSqsMessageHandlerRegister(this.MessageServer.Object, this.MessageProcessor.Object);
        }

        protected AwsSqsMessageHandlerRegister MessageHandlerRegister { get; set; }

        protected Mock<IMessageProcessor> MessageProcessor { get; set; }

        protected Mock<IMessageService> MessageServer { get; set; }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullMessageServer()
        {
            new AwsSqsMessageHandlerRegister(null, this.MessageProcessor.Object);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CtorThrowsExWithNullMessageProcessor()
        {
            new AwsSqsMessageHandlerRegister(this.MessageServer.Object, null);
        }

        [Test]
        public void CtorCorrectlyAssignsArgsToProperties()
        {
            Assert.AreSame(this.MessageProcessor.Object, this.MessageHandlerRegister.MessageProcessor);
        }

        [Test]
        public void WrapExceptionHandlerExecutesCustomExceptionHandler()
        {
            // Arrange    
            var msg = new Message<int>(1);
            var ex = new InvalidOperationException();
            var exFuncInvoked = false;
            this.MessageProcessor
                .Setup(x => x.OnMessageProcessingFailed(msg, ex, false))
                .Verifiable();

            var exHandler = this.MessageHandlerRegister.WrapExceptionHandler<int>((message, exception) => exFuncInvoked = true);

            // Test the the MessageProcessor is invoked!  
            exHandler.Invoke(msg, ex);

            // Assert
            Assert.IsTrue(exFuncInvoked);
            this.MessageProcessor.Verify(x => x.OnMessageProcessingFailed(msg, ex, false), Times.Once());
        }

        [Test]
        public void WrapExceptionHandlerDoesNotExecuteCustomExHandlerIfMessageWasNotProcessed()
        {
            // Arrange    
            var msg = new Message<int>(1);
            var ex = new MessageNotProcessedException();
            var exFuncInvoked = false;
            this.MessageProcessor
                .Setup(x => x.OnMessageProcessingFailed(msg, ex, false))
                .Verifiable();

            var exHandler = this.MessageHandlerRegister.WrapExceptionHandler<int>((message, exception) => exFuncInvoked = true);

            // Test the the MessageProcessor is invoked!  
            exHandler.Invoke(msg, ex);

            // Assert
            Assert.IsFalse(exFuncInvoked);
            this.MessageProcessor.Verify(x => x.OnMessageProcessingFailed(msg, ex, false), Times.Once());
        }

        [Test]
        public void WrapExceptionHandlerIndicatesMoveToDlqWhenRetryCountIsExceeded()
        {
            // Arrange    
            var msg = new Message<int>(1);
            msg.RetryAttempts = 1000; // Ensure it exceeds the retry limit.
            var ex = new MessageNotProcessedException();
            var exFuncInvoked = false;
            this.MessageProcessor
                .Setup(x => x.OnMessageProcessingFailed(msg, ex, true))
                .Verifiable();

            var exHandler = this.MessageHandlerRegister.WrapExceptionHandler<int>((message, exception) => exFuncInvoked = true);

            // Test that when the retry count is exceeded, the parameter 'moveMessageToDlq' is true. 
            exHandler.Invoke(msg, ex);

            // Assert
            Assert.IsFalse(exFuncInvoked);
            this.MessageProcessor.Verify(x => x.OnMessageProcessingFailed(msg, ex, true), Times.Once());
        }

        [Test]
        public void WrapMessageProcessorReturnsExceptionWhenMessageProcessorCanNotProcessMessage()
        {
           // Arrange    
            var msg = new Message<int>(1);
            var expectedResult = 10;

            this.MessageProcessor
                .Setup(x => x.CanProcessMessage(msg))
                .Returns(false)
                .Verifiable();

            var wrapper = this.MessageHandlerRegister.WrapMessageProcessor<int>(message => expectedResult);

            // Act
            var result = wrapper.Invoke(msg);

            // Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf<MessageNotProcessedException>(result);
            this.MessageProcessor.VerifyAll();
        }

        [Test]
        public void WrapMessageInvokesCustomProcessHandlerFollowedByOnMessageProcessed()
        {
            // Arrange    
            var msg = new Message<int>(1);
            var expectedResult = 10;

            this.MessageProcessor
                .Setup(x => x.CanProcessMessage(msg))
                .Returns(true)
                .Verifiable();

            var wrapper = this.MessageHandlerRegister.WrapMessageProcessor<int>(message => expectedResult);

            this.MessageProcessor
                .Setup(x => x.OnMessageProcessed(msg))
                .Verifiable();

            // Act
            var result = wrapper.Invoke(msg);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(expectedResult, result);
            this.MessageProcessor.VerifyAll();
            this.MessageProcessor.Verify(x => x.CanProcessMessage(msg), Times.Once());
            this.MessageProcessor.Verify(x => x.OnMessageProcessed(msg), Times.Once());
        }

        [Test]
        public void WrapMessageInvokesCustomProcessAndDoesNotCatchException()
        {
            // Arrange    
            var msg = new Message<int>(1);
            var expectedResult = 10;

            this.MessageProcessor
                .Setup(x => x.CanProcessMessage(msg))
                .Throws(new InvalidOperationException())
                .Verifiable();

            var wrapper = this.MessageHandlerRegister.WrapMessageProcessor<int>(message => expectedResult);

            // Act
            object result = null;
            bool exceptionThrown = false;
            try
            {
                result = wrapper.Invoke(msg);
            }
            catch (Exception ex)
            {
                // Bury, for testing.
                exceptionThrown = true;
            }

            // Assert
            Assert.IsNull(result);
            Assert.IsTrue(exceptionThrown);

            this.MessageProcessor.VerifyAll();
            this.MessageProcessor.Verify(x => x.CanProcessMessage(msg), Times.Once());
            this.MessageProcessor.Verify(x => x.OnMessageProcessed(msg), Times.Never());
        }
    }
}
