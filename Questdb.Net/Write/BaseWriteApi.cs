using Questdb.Net.Config;
using Serilog;
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
                    Log.Error($"The WriteApi can't be gracefully dispose! - {millis}ms elapsed.");
                    break;
                }
            }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        internal abstract class BatchWriteData
        {
            internal readonly BatchWriteOptions Options;
            protected BatchWriteData(BatchWriteOptions options)
            {
                this.Options = options;
            }

            internal abstract string FormatData();
        }

        internal class BatchWriteRecord : BatchWriteData
        {
            private readonly string _records;

            internal BatchWriteRecord(BatchWriteOptions options, string record) : base(options)
            {
                Arguments.CheckNotNull(record, nameof(record));
                _records = record + "\n";
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

            internal BatchWritePoint(QuestdbClientOptions clientOptions, BatchWriteOptions options, PointData point) : base(options)
            {
                Arguments.CheckNotNull(point, nameof(point));

                _point = point;
                _clientOptions = clientOptions;
            }

            internal override string FormatData()
            {
                if (!_point.HasFields())
                {
                    Log.Debug($"The point: ${_point} doesn't contains any fields, skipping");

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

            internal BatchWriteMeasurement(QuestdbClientOptions clientOptions, BatchWriteOptions options, TM measurement,
                Mapper measurementMapper) : base(options)
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
                    Log.Debug($"The point: ${point} doesn't contains any fields, skipping");

                    return null;
                }

                return point.ToLineProtocol(_clientOptions.PointSettings);
            }
        }

        internal class BatchWriteOptions
        {
            internal readonly string Bucket = string.Empty;
            internal readonly WritePrecision Precision = WritePrecision.Nanoseconds;

            internal BatchWriteOptions()
            {
            }

            internal BatchWriteOptions(string bucket, WritePrecision precision)
            {
                Bucket = bucket;
                Precision = precision;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Bucket != null ? Bucket.GetHashCode() : 0;
                    hashCode = (hashCode * 397) ^ (int)Precision;
                    return hashCode;
                }
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((BatchWriteOptions)obj);
            }

            private bool Equals(BatchWriteOptions other)
            {
                return string.Equals(Bucket, other.Bucket) && Precision == other.Precision;
            }
        }
    }
}
