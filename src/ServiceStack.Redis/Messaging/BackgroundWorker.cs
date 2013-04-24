using ServiceStack.Logging;
using System;
using System.Threading;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{
    namespace ServiceStack.Redis.Messaging
    {
        public interface IBackgroundWorker : IDisposable, ICloneable
        {
            void Start();

            void Stop();            

            void ForceRestart();

            string GetStatus();

            void KillBgThreadIfExists();
        }

        public abstract class BackgroundWorker : IBackgroundWorker
        {
            protected static ILog Log;

            // readonly object msgLock = new object();
            // private bool receivedNewMsgs = false;
            // protected readonly IMessageHandler messageHandler;

            // public string QueueName { get; set; }

            protected int status;
            /*public int Status
            {
                get { return status; }
            }*/

            private Thread bgThread;
            private int timesStarted = 0;
            
            protected BackgroundWorker()
            {
                Log = LogManager.GetLogger(this.GetType());                
            }
            
            
            public void Start()
            {
                if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
                    return;
                if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                    throw new ObjectDisposedException("MQ Host has been disposed");
                if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Stopping)
                    KillBgThreadIfExists();

                if (Interlocked.CompareExchange(ref status, WorkerStatus.Starting, WorkerStatus.Stopped) == WorkerStatus.Stopped)
                {
                    Log.Debug("Starting MQ Handler Worker: {0}...".Fmt(this.ThreadName));

                    //Should only be 1 thread past this point
                    bgThread = new Thread(Run)
                    {
                        Name = this.ThreadName,
                        IsBackground = true,
                    };
                    bgThread.Start();
                }
            }

            public void ForceRestart()
            {
                KillBgThreadIfExists();
                Start();
            }

            // protected abstract IMessageQueueClient CreateMessageQueueClient();

            /// <summary>
            /// The code that will be executed by the background worker.
            /// </summary>
            protected abstract void Execute();

            private void Run()
            {
                if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
                timesStarted++;

                try
                {
                    this.Execute();                   
                }
                catch (Exception ex)
                {
                    //Ignore handling rare, but expected exceptions from KillBgThreadIfExists()
                    if (ex is ThreadInterruptedException || ex is ThreadAbortException)
                    {
                        Log.Warn("Received {0} in BackgroundWorker: {1}".Fmt(ex.GetType().Name, this.ThreadName));
                        return;
                    }

                    Stop();
                    this.ExecuteErrorHandler(ex);                    
                }
                finally
                {
                    //If it's in an invalid state, Dispose() this worker.
                    if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping) != WorkerStatus.Stopping)
                    {
                        Dispose();
                    }
                }
            }

            protected abstract void ExecuteErrorHandler(Exception ex);

            public void Stop()
            {
                if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                    return;

                if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopping, WorkerStatus.Started) == WorkerStatus.Started)
                {
                    Log.Debug("Stopping Background Worker: {0}...".Fmt(this.ThreadName));
                    Thread.Sleep(100);
                    this.OnStop();                    
                }
            }

            protected abstract void OnStop();

            public void KillBgThreadIfExists()
            {
                try
                {
                    if (bgThread != null && bgThread.IsAlive)
                    {
                        //give it a small chance to die gracefully
                        if (!bgThread.Join(500))
                        {
                            //Ideally we shouldn't get here, but lets try our hardest to clean it up
                            Log.Warn("Interrupting previous Background Worker: " + bgThread.Name);
                            bgThread.Interrupt();
                            if (!bgThread.Join(TimeSpan.FromSeconds(3)))
                            {
                                Log.Warn(bgThread.Name + " just wont die, so we're now aborting it...");
                                bgThread.Abort();
                            }
                        }
                    }
                }
                finally
                {
                    bgThread = null;
                    status = WorkerStatus.Stopped;
                }
            }

            public virtual void Dispose()
            {
                if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                    return;

                Stop();

                if (Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopped) != WorkerStatus.Stopped)
                    Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopping);

                try
                {
                    KillBgThreadIfExists();
                }
                catch (Exception ex)
                {
                    Log.Error("Error Disposing BackgroundWorker for: " + this.ThreadName, ex);
                }
            }

            /// <summary>
            /// Gets a string that describes the thread. 
            /// </summary>
            /// <remarks>
            /// It's recommended to include the thread Id in the ThreadName.
            /// </remarks>
            protected abstract string ThreadName { get; }

            /*
            public IMessageHandlerStats GetStats()
            {
                return messageHandler.GetStats();
            }
            */

            public virtual string GetStatus()
            {
                return "[Worker: {0}, Status: {1}, ThreadStatus: {2}]"
                    .Fmt(this.ThreadName, WorkerStatus.ToString(status), bgThread.ThreadState);
            }

            protected ThreadState BgThreadState
            {
                get { return this.bgThread.ThreadState; }
            }

            /// <summary>
            /// Creates a new object that is a copy of the current instance.
            /// </summary>
            /// <returns>
            /// A new object that is a copy of this instance.
            /// </returns>
            /// <filterpriority>2</filterpriority>
            public abstract object Clone();
        }

        public abstract class BackgroundWorker<TBackgroundWorker> : BackgroundWorker
            where TBackgroundWorker : IBackgroundWorker
        {
            protected BackgroundWorker(Action<TBackgroundWorker, Exception> errorHandler)
            {                
                this.ErrorHandler = errorHandler;
            }

            public Action<TBackgroundWorker, Exception> ErrorHandler { get; private set; }

            /// <summary>
            /// Creates a new object that is a copy of the current instance.
            /// </summary>
            /// <returns>
            /// A new object that is a copy of this instance.
            /// </returns>
            /// <filterpriority>2</filterpriority>
            public override sealed object Clone()
            {
                return this.CloneBackgroundWorker();
            }

            public abstract TBackgroundWorker CloneBackgroundWorker();
            /*
            {
                return new MessageHandlerWorker(clientsManager, messageHandler, QueueName, errorHandler);
            }
            */

            protected abstract void InvokeErrorHandler(Exception ex);

            protected sealed override void ExecuteErrorHandler(Exception ex)
            {
                if (this.ErrorHandler != null)
                {
                    this.InvokeErrorHandler(ex);
                }
            }
        }
    }
}
