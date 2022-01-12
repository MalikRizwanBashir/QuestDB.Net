using Microsoft.Extensions.ObjectPool;
using Questdb.Net.Client;
using Questdb.Net.Config;
using Questdb.Net.Exceptions;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net.Write
{
    public class WriteLineApi : BaseWriteApi, IWriteLineApi
    {
        private readonly Subject<IObservable<BatchWriteData>> _flush = new Subject<IObservable<BatchWriteData>>();
        private readonly Subject<IObservable<string>> _lineFlush = new Subject<IObservable<string>>();

        private readonly QuestDBClient _questDbClient;
        private readonly Mapper _measurementMapper = new Mapper();
        private readonly QuestdbClientOptions _clientOptions;
        private readonly BatchWriteOptions _options;
        private readonly Subject<BatchWriteData> _subject = new Subject<BatchWriteData>();
        private readonly Subject<string> _subjectLine = new Subject<string>();
        private static readonly ObjectPoolProvider _objectPoolProvider = new DefaultObjectPoolProvider();
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool = _objectPoolProvider.CreateStringBuilderPool();
        private readonly IDisposable _unsubscribeDisposeCommand;
        private readonly TcpService _tcpService;
        private readonly Int16 MaxPointsToProcess = 10000;

        public bool Disposed => _disposed;


        private bool _disposed;
        protected internal WriteLineApi(
            QuestdbClientOptions options,
            WriteOptions writeOptions,
            QuestDBClient QuestDbClient,
            IObservable<TCPResponse> disposeCommand)
        {
            Arguments.CheckNotNull(writeOptions, nameof(writeOptions));
            Arguments.CheckNotNull(QuestDbClient, nameof(_questDbClient));
            Arguments.CheckNotNull(disposeCommand, nameof(disposeCommand));

            _clientOptions = options;
            _options = new BatchWriteOptions();

            _tcpService = new TcpService(_clientOptions.Url, 9009);
            _questDbClient = QuestDbClient;

            _unsubscribeDisposeCommand = disposeCommand.Subscribe(_ => Dispose());

            // backpreasure - is not implemented in C#
            // 
            // => use unbound buffer
            // 
            // https://github.com/dotnet/reactive/issues/19


            #region single insert

            IObservable<IObservable<BatchWriteRecord>> joinedbatches = _subject
                //
                // Batching
                //
                .Publish(connectedSource =>
                {
                    var trigger = Observable.Merge(
                            // triggered by time & count
                            connectedSource.Window(TimeSpan.FromMilliseconds(
                                                writeOptions.FlushInterval),
                                                writeOptions.BatchSize,
                                                writeOptions.WriteScheduler),
                            // flush trigger
                            _flush
                        );
                    return connectedSource
                        .Window(trigger);
                })
                //
                // Group by key - same bucket, same org
                //
                .SelectMany(it => it.GroupBy(batchWrite => batchWrite.Options))
                //
                // Create Write Point = bucket, org, ... + data
                //
                .Select(grouped =>
                {
                    var aggregate = grouped
                        .Aggregate(_stringBuilderPool.Get(), (builder, batchWrite) =>
                        {
                            var data = batchWrite.FormatData();

                            if (string.IsNullOrEmpty(data)) return builder;

                            if (builder.Length > 0)
                            {
                                builder.Append("\n");
                            }

                            return builder.Append(data);
                        }).Select(builder =>
                        {
                            var result = builder.ToString();
                            builder.Clear();
                            _stringBuilderPool.Return(builder);
                            return result;
                        });

                    return aggregate.Select(records => new BatchWriteRecord(grouped.Key, records))
                                    .Where(batchWriteItem => !string.IsNullOrEmpty(batchWriteItem.FormatData()));
                });

            if (writeOptions.JitterInterval > 0)
            {
                joinedbatches = joinedbatches
                    //
                    // Jitter
                    //
                    .Select(source =>
                    {
                        return source.Delay(_ => Observable.Timer(TimeSpan.FromMilliseconds(RetryAttempt.JitterDelay(writeOptions)), writeOptions.WriteScheduler));
                    });
            }
            var query = joinedbatches
                .Concat()
                //
                // Map to Async request
                //
                .Select(batchWriteItem =>
                {
                    var bucket = batchWriteItem.Options.Bucket;
                    var lineProtocol = batchWriteItem.FormatData();
                    var precision = batchWriteItem.Options.Precision;

                    return Observable
                        .Defer(() =>
                            _tcpService.SendAsync(Encoding.UTF8.GetBytes(lineProtocol))
                                .ToObservable())
                        .RetryWhen(f => f
                            .Zip(Observable.Range(1, writeOptions.MaxRetries + 1), (exception, count)
                                => new RetryAttempt(exception, count, writeOptions))
                            .SelectMany(attempt =>
                            {
                                if (attempt.IsRetry())
                                {
                                    var retryInterval = attempt.GetRetryInterval();

                                    return Observable.Timer(TimeSpan.FromMilliseconds(retryInterval),
                                        writeOptions.WriteScheduler);
                                }

                                throw attempt.Error;
                            }))
                        .Select(result =>
                        {
                            if (result.isSuccess) return Notification.CreateOnNext(result);

                            return Notification.CreateOnError<TCPResponse>(QuestdbException.Create(result));
                        })
                        .Catch<Notification<TCPResponse>, Exception>(ex =>
                        {
                            return Observable.Return(Notification.CreateOnError<TCPResponse>(ex));
                        }).Do(res =>
                        {
                            if (res.Kind == NotificationKind.OnNext)
                            {
                                //successful
                            }
                        });
                })
                .Concat()
                .Subscribe(
                    notification =>
                    {
                        switch (notification.Kind)
                        {
                            case NotificationKind.OnNext:
                                Log.Debug($"The batch item: {notification} was processed successfully.");
                                break;
                            case NotificationKind.OnError:
                                Log.Debug($"The batch item wasn't processed successfully because: {notification.Exception}");
                                break;
                            default:
                                Log.Debug($"The batch item: {notification} was processed");
                                break;
                        }
                    },
                    exception =>
                    {
                        _disposed = true;
                        Log.Warning($"The unhandled exception occurs: {exception}");
                    },
                    () =>
                    {
                        _disposed = true;
                        Log.Debug("The WriteApi was disposed.");
                    });

            #endregion


            #region batch insert
            IObservable<IObservable<string>> batches = _subjectLine
                //
                // Batching
                //
                .Publish(connectedSource =>
                {
                    var trigger = Observable.Merge(
                            // triggered by time & count
                            connectedSource.Window(TimeSpan.FromMilliseconds(
                                                writeOptions.FlushInterval),
                                                writeOptions.BatchSize,
                                                writeOptions.WriteScheduler),
                            // flush trigger
                            _lineFlush
                        );
                    return connectedSource
                        .Window(trigger);
                });

            if (writeOptions.JitterInterval > 0)
            {
                batches = batches
                    //
                    // Jitter
                    //
                    .Select(source =>
                    {
                        return source.Delay(_ => Observable.Timer(TimeSpan.FromMilliseconds(RetryAttempt.JitterDelay(writeOptions)), writeOptions.WriteScheduler));
                    });
            }
            var lineQuery = batches
                .Concat()
                //
                // Map to Async request
                //
                .Select(batchWriteItem =>
                {
                    //Observable.Start(() => _tcpService.SendAsync(Encoding.UTF8.GetBytes(batchWriteItem))
                    //            .ToObservable(), Scheduler.Default))
                    //        .Merge(3 /* at a time */)
                    return Observable
                        .Defer(() =>
                            _tcpService.SendAsync(Encoding.UTF8.GetBytes(batchWriteItem))
                                .ToObservable())
                        .RetryWhen(f => f
                            .Zip(Observable.Range(1, writeOptions.MaxRetries + 1), (exception, count)
                                => new RetryAttempt(exception, count, writeOptions))
                            .SelectMany(attempt =>
                            {
                                if (attempt.IsRetry())
                                {
                                    var retryInterval = attempt.GetRetryInterval();

                                    return Observable.Timer(TimeSpan.FromMilliseconds(retryInterval),
                                        writeOptions.WriteScheduler);
                                }

                                throw attempt.Error;
                            }))
                        .Select(result =>
                        {
                            if (result.isSuccess) return Notification.CreateOnNext(result);

                            return Notification.CreateOnError<TCPResponse>(QuestdbException.Create(result));
                        })
                        .Catch<Notification<TCPResponse>, Exception>(ex =>
                        {
                            return Observable.Return(Notification.CreateOnError<TCPResponse>(ex));
                        }).Do(res =>
                        {
                            if (res.Kind == NotificationKind.OnNext)
                            {
                                //successful
                            }
                        });
                })
                .Concat()
                .Subscribe(
                    notification =>
                    {
                        switch (notification.Kind)
                        {
                            case NotificationKind.OnNext:
                                Log.Debug($"The batch item: {notification} was processed successfully.");
                                break;
                            case NotificationKind.OnError:
                                Log.Error(
                                    $"The batch item wasn't processed successfully because: {notification.Exception}");
                                break;
                            default:
                                Log.Debug($"The batch item: {notification} was processed");
                                break;
                        }
                    },
                    exception =>
                    {
                        _disposed = true;
                        Log.Warning($"The unhandled exception occurs: {exception}");
                    },
                    () =>
                    {
                        _disposed = true;
                        Log.Debug("The WriteApi was disposed.");
                    });

            #endregion
        }

        #region write points, line protocol
        /// <summary>
        /// Write a Data point into specified database.
        /// </summary>
        /// <param name="point">specifies the Data point to write into database</param>
        public void WritePoint(PointData point)
        {
            if (point == null) return;

            _subject.OnNext(new BatchWritePoint(_clientOptions, _options, point));
        }

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        public void WritePoints(List<PointData> points)
        {
            Int16 loop = 1;
            Int32 processed = 0;
            if (points.Count > MaxPointsToProcess)
            {
                loop = (Int16)Math.Ceiling((decimal)points.Count / MaxPointsToProcess);
            }
            Parallel.For(0, loop, new ParallelOptions { MaxDegreeOfParallelism = 3 }, (i, state) =>
            {
                StringBuilder lines = new StringBuilder();
                int j = i * MaxPointsToProcess;
                for (int k = 0; k < MaxPointsToProcess; k++)
                {
                    if (j >= points.Count)
                        break;
                    var p = points[j];
                    lines.Append(p.ToLineProtocol(_clientOptions.PointSettings));
                    lines.Append("\n");
                    j++;
                    processed++;
                    if (points.Count == processed)
                        break;
                }
                _subjectLine.OnNext(lines.ToString());
                lines.Clear();
            });
        }

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        public void WritePoints(params PointData[] points)
        {
            Int16 loop = 1;
            Int32 processed = 0;
            if (points.Length > MaxPointsToProcess)
            {
                loop = (Int16)Math.Ceiling((decimal)points.Length / MaxPointsToProcess);
            }
            Parallel.For(0, loop, new ParallelOptions { MaxDegreeOfParallelism = 3 }, (i, state) =>
            {
                StringBuilder lines = new StringBuilder();
                int j = i * MaxPointsToProcess;
                for (int k = 0; k < MaxPointsToProcess; k++)
                {
                    if (j >= points.Length)
                        break;
                    var p = points[j];
                    lines.Append(p.ToLineProtocol(_clientOptions.PointSettings));
                    lines.Append("\n");
                    j++;
                    processed++;
                    if (points.Length == processed)
                        break;
                }
                _subjectLine.OnNext(lines.ToString());
                lines.Clear();
            });
        }

        #endregion

        #region write T type as point, Line protocol
        /// <summary>
        /// Write a Measurement into specified database.
        /// </summary>
        /// <param name="measurement">specifies the Measurement to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        public void WriteMeasurement<TM>(TM measurement)
        {
            if (measurement == null) return;

            _subject.OnNext(new BatchWriteMeasurement<TM>(_clientOptions, _options, measurement, _measurementMapper));
        }

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        public void WriteMeasurements<TM>(List<TM> measurements)
        {
            Int16 loop = 1;
            Int32 processed = 0;
            if (measurements.Count > MaxPointsToProcess)
            {
                loop = (Int16)Math.Ceiling((decimal)measurements.Count / MaxPointsToProcess);
            }
            Parallel.For(0, loop, new ParallelOptions { MaxDegreeOfParallelism = 3 }, (i, state) =>
            {
                StringBuilder lines = new StringBuilder();
                int j = i * MaxPointsToProcess;
                for (int k = 0; k < MaxPointsToProcess; k++)
                {
                    if (j >= measurements.Count)
                        break;
                    var p = measurements[j];
                    lines.Append(_measurementMapper.ToPoint(p, WritePrecision.Nanoseconds).ToLineProtocol(_clientOptions.PointSettings));
                    lines.Append("\n");
                    j++;
                    processed++;
                    if (measurements.Count == processed)
                        break;
                }
                _subjectLine.OnNext(lines.ToString());
                lines.Clear();
            });
        }

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        public void WriteMeasurements<TM>(params TM[] measurements)
        {
            Int16 loop = 1;
            Int32 processed = 0;
            if (measurements.Length > MaxPointsToProcess)
            {
                loop = (Int16)Math.Ceiling((decimal)measurements.Length / MaxPointsToProcess);
            }
            Parallel.For(0, loop, new ParallelOptions { MaxDegreeOfParallelism = 3 }, (i, state) =>
            {
                StringBuilder lines = new StringBuilder();
                int j = i * MaxPointsToProcess;
                for (int k = 0; k < MaxPointsToProcess; k++)
                {
                    if (j >= measurements.Length)
                        break;
                    var p = measurements[j];
                    lines.Append(_measurementMapper.ToPoint(p, WritePrecision.Nanoseconds).ToLineProtocol(_clientOptions.PointSettings));
                    lines.Append("\n");
                    j++;
                    processed++;
                    if (measurements.Length == processed)
                        break;
                }
                _subjectLine.OnNext(lines.ToString());
                lines.Clear();
            });
        }

        #endregion

        public new void Dispose()
        {
            _unsubscribeDisposeCommand.Dispose(); // avoid duplicate call to dispose

            Log.Debug("Flushing batches before shutdown.");

            if (!_subject.IsDisposed) _subject.OnCompleted();

            if (!_flush.IsDisposed) _flush.OnCompleted();

            _subject.Dispose();
            _flush.Dispose();

            WaitToCondition(() => _disposed, 30000);

            _tcpService?.Dispose();
        }
    }
}
