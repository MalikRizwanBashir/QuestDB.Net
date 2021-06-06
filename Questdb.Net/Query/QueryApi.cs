using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Questdb.Net.Config;
using RestSharp;
using Task = System.Threading.Tasks.Task;

namespace Questdb.Net.Query
{
    public class QueryApi : AbstractQueryClient
    {
        private readonly QuestdbClientOptions _options;
        private readonly ApiClient _apiClient;

        protected internal QueryApi(QuestdbClientOptions options, ApiClient apiClient) : base(apiClient.RestClient)
        {
            Arguments.CheckNotNull(options, nameof(options));
            Arguments.CheckNotNull(apiClient, nameof(apiClient));

            _options = options;
            _apiClient = apiClient;
        }


        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public async Task<string> QueryRawAsync(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateRequest(query);

            var response = await QueryAsync(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content;
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public string QueryRaw(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateRequest(query);

            var response = Query(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content;
        }


        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public async Task<QuestdbResponse> QueryAsync(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateRequest(query);

            var response = await QueryAsync(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content.AsQuestdbResponse();
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public QuestdbResponse Query(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateRequest(query);

            var response = Query(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content.AsQuestdbResponse();
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public async Task<string> QueryCSVAsync(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateCSVRequest(query);

            var response = await QueryAsync(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content;
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public string QueryCSV(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));

            var requestMessage = CreateCSVRequest(query);

            var response = Query(requestMessage);

            RaiseForIfError(response, response.Content);

            return response.Content;
        }


        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public async Task<IEnumerable<T>> QueryEnumerableAsync<T>(string query)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));

            var requestMessage = CreateRequest(query);

            return await QueryEnumerableAsync<T>(requestMessage);
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        public IEnumerable<T> QueryEnumerable<T>(string query)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));

            var requestMessage = CreateRequest(query);

            return QueryEnumerable<T>(requestMessage);
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously stream <see cref="FluxRecord"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="org">specifies the source organization</param>
        /// <param name="onNext">the callback to consume the FluxRecord result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        public async Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action<Exception> onError,
            Action onComplete)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));
            Arguments.CheckNotNull(onNext, nameof(onNext));
            Arguments.CheckNotNull(onError, nameof(onError));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            var consumer = new ResponseConsumerPoco<T>(onNext);

            await QueryAsync(query, consumer, onError, onComplete);
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously stream <see cref="T"/>
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="onNext">the callback to consume the FluxRecord result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        public async Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action<Exception> onError)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));
            Arguments.CheckNotNull(onNext, nameof(onNext));
            Arguments.CheckNotNull(onError, nameof(onError));

            var consumer = new ResponseConsumerPoco<T>(onNext);

            await QueryAsync(query, consumer, onError);
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously stream <see cref="FluxRecord"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="org">specifies the source organization</param>
        /// <param name="onNext">the callback to consume the FluxRecord result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        public async Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action onComplete)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));
            Arguments.CheckNotNull(onNext, nameof(onNext));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            var consumer = new ResponseConsumerPoco<T>(onNext);

            await QueryAsync(query, consumer, onComplete);
        }

        /// <summary>
        /// Executes the Flux query against the QuestDB 2.0 and asynchronously stream <see cref="FluxRecord"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the flux query to execute</param>
        /// <param name="org">specifies the source organization</param>
        /// <param name="onNext">the callback to consume the FluxRecord result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        public async Task QueryAsync<T>(string query, Action<ICancellable, T> onNext)
        {
            Arguments.CheckNonEmptyString(query, nameof(query));
            Arguments.CheckNotNull(onNext, nameof(onNext));

            var consumer = new ResponseConsumerPoco<T>(onNext);

            await QueryAsync(query, consumer);
        }

        #region privte methods
        private async Task QueryAsync(string query, QuestCsvParser.ICSVResponseConsumer consumer,
            Action<Exception> onError,
            Action onComplete)

        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));
            Arguments.CheckNotNull(onError, nameof(onError));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            var requestMessage = CreateCSVRequest(query);

            await QueryAsync(requestMessage, consumer, onError, onComplete);
        }

        private async Task QueryAsync(string query, QuestCsvParser.ICSVResponseConsumer consumer,
            Action<Exception> onError)

        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));
            Arguments.CheckNotNull(onError, nameof(onError));

            var requestMessage = CreateCSVRequest(query);

            await QueryAsync(requestMessage, consumer, onError);
        }


        private async Task QueryAsync(string query, QuestCsvParser.ICSVResponseConsumer consumer,
            Action onComplete)

        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            var requestMessage = CreateCSVRequest(query);

            await QueryAsync(requestMessage, consumer, onComplete);
        }

        private async Task QueryAsync(string query, QuestCsvParser.ICSVResponseConsumer consumer)

        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));

            var requestMessage = CreateCSVRequest(query);

            await QueryAsync(requestMessage, consumer);
        }


        protected override void BeforeIntercept(RestRequest request)
        {
            _apiClient.BeforeIntercept(request);
        }

        private RestRequest CreateRequest(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));
            var request = _apiClient.GetQueryWithRestRequest("/exec", "gzip, deflate, br", "text/csv; charset=utf-8", query);
            return request;
        }

        private RestRequest CreateCSVRequest(string query)
        {
            Arguments.CheckNotNull(query, nameof(query));
            var request = _apiClient.GetQueryWithRestRequest("/exp", "gzip, deflate, br", "application/json", query);
            return request;
        }
        #endregion
    }
}