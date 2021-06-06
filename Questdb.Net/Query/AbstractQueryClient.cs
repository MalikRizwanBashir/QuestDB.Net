using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Questdb.Net.Exceptions;
using Questdb.Net.Query;
using RestSharp;

namespace Questdb.Net.Query
{
    public abstract class AbstractQueryClient
    {
        protected static readonly Action EmptyAction = () => { };

        protected static readonly Action<Exception> ErrorConsumer = e => throw e;

        private readonly QuestCsvParser _csvParser = new QuestCsvParser();

        protected readonly RestClient RestClient;

        protected AbstractQueryClient(RestClient restClient)
        {
            Arguments.CheckNotNull(restClient, nameof(restClient));

            RestClient = restClient;
        }

        protected async Task<IRestResponse> QueryAsync(RestRequest query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            BeforeIntercept(query);

            return await RestClient.ExecuteAsync(query);
        }

        protected IRestResponse Query(RestRequest query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            BeforeIntercept(query);

            return RestClient.Execute(query);
        }

        protected QuestdbResponse QueryTable(RestRequest query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            BeforeIntercept(query);

            var response = Query(query);

            //response.Content = AfterIntercept((int)response.StatusCode, () => ToHeaders(response.Headers), response.Content);

            RaiseForIfError(response, response.Content);

            return response.Content.AsQuestdbResponse();
        }

        protected async Task<QuestdbResponse> QueryTableAsync(RestRequest query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            BeforeIntercept(query);

            var response = await QueryAsync(query);

            //response.Content = AfterIntercept((int)response.StatusCode, () => ToHeaders(response.Headers), response.Content);

            RaiseForIfError(response, response.Content);

            return response.Content.AsQuestdbResponse();
        }

        protected async Task<IEnumerable<T>> QueryEnumerableAsync<T>(RestRequest query)
        {
            var response = await QueryTableAsync(query);
            return response.ToPoco<T>();
        }

        protected IEnumerable<T> QueryEnumerable<T>(RestRequest query)
        {
            var response = QueryTable(query);
            return response.ToPoco<T>();
        }

        protected async Task QueryAsync(RestRequest query, QuestCsvParser.ICSVResponseConsumer responseConsumer,
            Action<Exception> onError,
            Action onComplete)
        {
            void Consumer(ICancellable cancellable, Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseCSVResponse(bufferedStream, cancellable, responseConsumer);
                }
                catch (IOException e)
                {
                    onError(e);
                }
            }

            await QueryAsync(query, Consumer, onError, onComplete);
        }

        protected async Task QueryAsync(RestRequest query, Action<ICancellable, Stream> consumer,
            Action<Exception> onError, Action onComplete)
        {
            Arguments.CheckNotNull(query, "query");
            Arguments.CheckNotNull(consumer, "consumer");
            Arguments.CheckNotNull(onError, "onError");
            Arguments.CheckNotNull(onComplete, "onComplete");

            try
            {
                var cancellable = new DefaultCancellable();

                BeforeIntercept(query);

                query.AdvancedResponseWriter = (responseStream, response) =>
                {
                    //responseStream = AfterIntercept((int)response.StatusCode, () => response.Headers, responseStream);

                    RaiseForIfError(response, responseStream);
                    consumer(cancellable, responseStream);
                };

                await Task.Run(() => { RestClient.DownloadData(query, true); });
                if (!cancellable.IsCancelled())
                {
                    onComplete();
                }
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        protected async Task QueryAsync(RestRequest query, QuestCsvParser.ICSVResponseConsumer responseConsumer,
            Action onComplete)
        {
            void Consumer(ICancellable cancellable, Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseCSVResponse(bufferedStream, cancellable, responseConsumer);
                }
                catch (IOException e)
                {
                    throw new QuestdbException(e);
                }
            }

            await QueryAsync(query, Consumer, onComplete);
        }

        protected async Task QueryAsync(RestRequest query, Action<ICancellable, Stream> consumer,
            Action onComplete)
        {
            Arguments.CheckNotNull(query, "query");
            Arguments.CheckNotNull(consumer, "consumer");
            Arguments.CheckNotNull(onComplete, "onComplete");

            try
            {
                var cancellable = new DefaultCancellable();

                BeforeIntercept(query);

                query.AdvancedResponseWriter = (responseStream, response) =>
                {
                    //responseStream = AfterIntercept((int)response.StatusCode, () => response.Headers, responseStream);

                    RaiseForIfError(response, responseStream);
                    consumer(cancellable, responseStream);
                };

                await Task.Run(() => { RestClient.DownloadData(query, true); });
                if (!cancellable.IsCancelled())
                {
                    onComplete();
                }
            }
            catch (Exception e)
            {
                throw new QuestdbException(e);
            }
        }


        protected async Task QueryAsync(RestRequest query, QuestCsvParser.ICSVResponseConsumer responseConsumer,
            Action<Exception> onError)
        {
            void Consumer(ICancellable cancellable, Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseCSVResponse(bufferedStream, cancellable, responseConsumer);
                }
                catch (IOException e)
                {
                    onError(e);
                }
            }

            await QueryAsync(query, Consumer, onError);
        }

        protected async Task QueryAsync(RestRequest query, Action<ICancellable, Stream> consumer,
            Action<Exception> onError)
        {
            Arguments.CheckNotNull(query, "query");
            Arguments.CheckNotNull(consumer, "consumer");
            Arguments.CheckNotNull(onError, "onError");

            try
            {
                var cancellable = new DefaultCancellable();

                BeforeIntercept(query);

                query.AdvancedResponseWriter = (responseStream, response) =>
                {
                    //responseStream = AfterIntercept((int)response.StatusCode, () => response.Headers, responseStream);

                    RaiseForIfError(response, responseStream);
                    consumer(cancellable, responseStream);
                };

                await Task.Run(() => { RestClient.DownloadData(query, true); });
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        protected async Task QueryAsync(RestRequest query, QuestCsvParser.ICSVResponseConsumer responseConsumer)
        {
            void Consumer(ICancellable cancellable, Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseCSVResponse(bufferedStream, cancellable, responseConsumer);
                }
                catch (IOException e)
                {
                    throw new QuestdbException(e);
                }
            }

            await QueryAsync(query, Consumer);
        }

        protected async Task QueryAsync(RestRequest query, Action<ICancellable, Stream> consumer)
        {
            Arguments.CheckNotNull(query, "query");
            Arguments.CheckNotNull(consumer, "consumer");

            try
            {
                var cancellable = new DefaultCancellable();

                BeforeIntercept(query);

                query.AdvancedResponseWriter = (responseStream, response) =>
                {
                    //responseStream = AfterIntercept((int)response.StatusCode, () => response.Headers, responseStream);

                    RaiseForIfError(response, responseStream);
                    consumer(cancellable, responseStream);
                };

                await Task.Run(() => { RestClient.DownloadData(query, true); });
            }
            catch (Exception e)
            {
                throw new QuestdbException(e);
            }
        }


        protected abstract void BeforeIntercept(RestRequest query);

        protected class ResponseConsumerPoco<T> : QuestCsvParser.ICSVResponseConsumer
        {
            private readonly Action<ICancellable, T> _onNext;

            public ResponseConsumerPoco(Action<ICancellable, T> onNext)
            {
                _onNext = onNext;
            }

            public void Accept(ICancellable cancellable, QuestdbResponse table)
            {
            }

            public void Accept(ICancellable cancellable, List<TableColumn> columns, List<object> record)
            {
                _onNext(cancellable, ResponseMapper.ToPoco<T>(record, columns));
            }
        }

        protected void RaiseForIfError(object result, object body)
        {

            if (result is QuestdbResponse questResponse)
            {
                if (questResponse.IsSuccessResponse) return;

                if (!string.IsNullOrEmpty(questResponse.Error))
                {
                    throw new QuestdbException(questResponse.Error);
                }
            }

            if (result is IRestResponse restResponse)
            {
                if (restResponse.IsSuccessful) return;

                if (restResponse.ErrorException is QuestdbException)
                {
                    throw restResponse.ErrorException;
                }

                throw HttpException.Create(restResponse, body);
            }

            var httpResponse = (IHttpResponse)result;
            if ((int)httpResponse.StatusCode >= 200 && (int)httpResponse.StatusCode < 300)
            {
                return;
            }

            if (httpResponse.ErrorException is QuestdbException)
            {
                throw httpResponse.ErrorException;
            }

            throw HttpException.Create(httpResponse, body);
        }
    }
}