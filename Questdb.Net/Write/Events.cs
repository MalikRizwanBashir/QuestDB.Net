using Serilog;
using System;
using System.Diagnostics;

namespace Questdb.Net.Write
{
    public abstract class QuestdbEventArgs : EventArgs
    {
        internal abstract void LogEvent();
    }

    public class WriteSuccessEvent : AbstractWriteEvent
    {
        public WriteSuccessEvent(WritePrecision precision, string lineProtocol) :
            base(precision, lineProtocol)
        {
        }

        internal override void LogEvent()
        {
            Log.Debug("The data was successfully written to QuestDB 2.0.");
        }
    }

    /// <summary>
    /// Published when occurs a runtime exception in background batch processing.
    /// </summary>
    public class WriteRuntimeExceptionEvent : QuestdbEventArgs
    {
        /// <summary>
        /// The Runtime Exception that was throw.
        /// </summary>
        public Exception Exception { get; }

        internal WriteRuntimeExceptionEvent(Exception exception)
        {
            Exception = exception;
        }

        internal override void LogEvent()
        {
            Log.Error($"The unhandled exception occurs: {Exception}");
        }
    }

    public class WriteErrorEvent : AbstractWriteEvent
    {
        /// <summary>
        /// The exception that was throw.
        /// </summary>
        public Exception Exception { get; }

        public WriteErrorEvent(WritePrecision precision, string lineProtocol, Exception exception) :
            base(precision, lineProtocol)
        {
            Exception = exception;
        }

        internal override void LogEvent()
        {
            Log.Error($"The error occurred during writing of data: {Exception.Message}");
        }
    }

    /// <summary>
    /// The event is published when occurs a retriable write exception.
    /// </summary>
    public class WriteRetriableErrorEvent : AbstractWriteEvent
    {
        /// <summary>
        /// The exception that was throw.
        /// </summary>
        public Exception Exception { get; }
        
        /// <summary>
        /// The time to wait before retry unsuccessful write (milliseconds)
        /// </summary>
        public long RetryInterval { get; }
        
        public WriteRetriableErrorEvent(WritePrecision precision, string lineProtocol, Exception exception, long retryInterval) : base(precision, lineProtocol)
        {
            Exception = exception;
            RetryInterval = retryInterval;
        }
        
        internal override void LogEvent()
        {
            var message = "The retriable error occurred during writing of data. " +
                          $"Reason: '{Exception.Message}'. " +
                          $"Retry in: {(double) RetryInterval / 1000}s.";
            
            Log.Warning(message);
        }
    }

    public abstract class AbstractWriteEvent : QuestdbEventArgs
    {

        /// <summary>
        /// The bucket that was used for write data.
        /// </summary>
        public string Database { get; }

        /// <summary>
        /// The Precision that was used for write data.
        /// </summary>
        public WritePrecision Precision { get; }

        /// <summary>
        /// The Data that was written.
        /// </summary>
        public string LineProtocol { get; }

        internal AbstractWriteEvent(WritePrecision precision, string lineProtocol)
        {
            Precision = precision;
            LineProtocol = lineProtocol;
        }
    }
}