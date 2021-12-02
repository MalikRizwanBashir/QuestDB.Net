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

        private readonly QuestDBClient _questDbClient;
        private readonly Mapper _measurementMapper = new Mapper();
        private readonly QuestdbClientOptions _options;
        private readonly Subject<BatchWriteData> _subject = new Subject<BatchWriteData>();
        private static readonly ObjectPoolProvider _objectPoolProvider = new DefaultObjectPoolProvider();
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool = _objectPoolProvider.CreateStringBuilderPool();
        private readonly IDisposable _unsubscribeDisposeCommand;
        private readonly TcpService _tcpService;

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

            _options = options;

            _tcpService = new TcpService(_options.Url, 9009);
            _questDbClient = QuestDbClient;

            _unsubscribeDisposeCommand = disposeCommand.Subscribe(_ => Dispose());

            // backpreasure - is not implemented in C#
            // 
            // => use unbound buffer
            // 
            // https://github.com/dotnet/reactive/issues/19



            IObservable<IObservable<BatchWriteRecord>> batches = _subject
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
                //.SelectMany(it => it.GroupBy(batchWrite => batchWrite.Options))
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

                    return aggregate.Select(records => new BatchWriteRecord(records))
                                    .Where(batchWriteItem => !string.IsNullOrEmpty(batchWriteItem.FormatData()));
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
            var query = batches
                .Concat()
                //
                // Map to Async request
                //
                .Select(batchWriteItem =>
                {
                    var lineProtocol = batchWriteItem.FormatData();

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

                                    var retryable = new WriteRetriableErrorEvent(WritePrecision.Nanoseconds, lineProtocol,
                                        attempt.Error, retryInterval);

                                    Publish(retryable);

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
                            var error = new WriteErrorEvent(WritePrecision.Nanoseconds, lineProtocol, ex);
                            Publish(error);

                            return Observable.Return(Notification.CreateOnError<TCPResponse>(ex));
                        }).Do(res =>
                        {
                            if (res.Kind == NotificationKind.OnNext)
                            {
                                var success = new WriteSuccessEvent(WritePrecision.Nanoseconds, lineProtocol);
                                Publish(success);
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
                                Log.Debug(
                                    $"The batch item wasn't processed successfully because: {notification.Exception}");
                                break;
                            default:
                                Log.Debug($"The batch item: {notification} was processed");
                                break;
                        }
                    },
                    exception =>
                    {
                        Publish(new WriteRuntimeExceptionEvent(exception));
                        _disposed = true;
                        Log.Error($"The unhandled exception occurs: {exception}");
                    },
                    () =>
                    {
                        _disposed = true;
                        Log.Debug("The WriteApi was disposed.");
                    });
        }

        #region write points, line protocol
        /// <summary>
        /// Write a Data point into specified database.
        /// </summary>
        /// <param name="point">specifies the Data point to write into database</param>
        public void WritePoint(PointData point)
        {
            if (point == null) return;

            _subject.OnNext(new BatchWritePoint(_options, point));
        }

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        public void WritePoints(List<PointData> points)
        {
            foreach (var point in points) WritePoint(point);
        }

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        public void WritePoints(params PointData[] points)
        {
            foreach (var point in points) WritePoint(point);
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

            _subject.OnNext(new BatchWriteMeasurement<TM>(_options, measurement, _measurementMapper));
        }

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        public void WriteMeasurements<TM>(List<TM> measurements)
        {
            foreach (var measurement in measurements) WriteMeasurement(measurement);
        }

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        public void WriteMeasurements<TM>(params TM[] measurements)
        {
            WriteMeasurements(measurements.ToList());
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
