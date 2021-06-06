using Questdb.Net.Config;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Questdb.Net.Write
{
    public abstract class BaseWriteApi : IDisposable
    {
        public event EventHandler EventHandler;
        private readonly Subject<IObservable<BatchWriteData>> _flush = new Subject<IObservable<BatchWriteData>>();

        /// <summary>
        /// Forces the client to flush all pending writes from the buffer to the QuestDB via HTTP.
        /// </summary>
        public void Flush()
        {
            if (!_flush.IsDisposed)
            {
                _flush.OnNext(Observable.Empty<BatchWriteData>());
            }
        }

        internal static void WaitToCondition(Func<bool> condition, int millis)
        {
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            while (!condition())
            {
                Thread.Sleep(25);
                if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - start > millis)
                {
                    Trace.TraceError($"The WriteApi can't be gracefully dispose! - {millis}ms elapsed.");
                    break;
                }
            }
        }

        protected void Publish(QuestdbEventArgs eventArgs)
        {
            eventArgs.LogEvent();

            EventHandler?.Invoke(this, eventArgs);
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        internal abstract class BatchWriteData
        {
            protected BatchWriteData()
            {
            }

            internal abstract string FormatData();
        }

        internal class BatchWriteRecord : BatchWriteData
        {
            private readonly string _records;

            internal BatchWriteRecord(string records)
            {
                Arguments.CheckNotNull(records, nameof(records));

                _records = records + "\n";
            }

            internal override string FormatData()
            {
                return _records;
            }
        }

        internal class BatchWritePoint : BatchWriteData
        {
            private readonly PointData _point;
            private readonly QuestdbClientOptions _clientOptions;

            internal BatchWritePoint(QuestdbClientOptions clientOptions, PointData point)
            {
                Arguments.CheckNotNull(point, nameof(point));

                _point = point;
                _clientOptions = clientOptions;
            }

            internal override string FormatData()
            {
                if (!_point.HasFields())
                {
                    Trace.WriteLine($"The point: ${_point} doesn't contains any fields, skipping");

                    return null;
                }

                return _point.ToLineProtocol(_clientOptions.PointSettings);
            }
        }

        internal class BatchWriteMeasurement<TM> : BatchWriteData
        {
            private readonly TM _measurement;
            private readonly Mapper _measurementMapper;
            private readonly QuestdbClientOptions _clientOptions;

            internal BatchWriteMeasurement(QuestdbClientOptions clientOptions, TM measurement,
                Mapper measurementMapper)
            {
                Arguments.CheckNotNull(measurement, nameof(measurement));

                _clientOptions = clientOptions;
                _measurement = measurement;
                _measurementMapper = measurementMapper;
            }

            internal override string FormatData()
            {
                var point = _measurementMapper.ToPoint(_measurement, WritePrecision.Nanoseconds);
                if (!point.HasFields())
                {
                    Trace.WriteLine($"The point: ${point} doesn't contains any fields, skipping");

                    return null;
                }

                return point.ToLineProtocol(_clientOptions.PointSettings);
            }
        }
    }
}
