using Questdb.Net.Client;
using Questdb.Net.Config;
using Questdb.Net.Exceptions;
using Questdb.Net.Query;
using Questdb.Net.Write;
using System;
using System.Reactive.Subjects;
using System.Text;

namespace Questdb.Net
{
    public class QuestDBClient : IQuestDBClient, IDisposable
    {
        private readonly ApiClient _apiClient;
        private readonly ExceptionFactory _exceptionFactory;
        private readonly GzipHandler _gzipHandler;

        private readonly QuestdbClientOptions _options;

        private readonly Subject<TcpService.TCPResponse> _disposeNotification = new Subject<TcpService.TCPResponse>();

        #region ctor

        public QuestDBClient()
        {
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .LoadConfig()
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        }

        public QuestDBClient(string host)
        {
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(host)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        }

        public QuestDBClient(string url, string username, char[] password)
        {
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(url)
                .Authenticate(username, password)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        }

        public QuestDBClient(string host, char[] token)
        {
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(host)
                .AuthenticateToken(token)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        }

        public QuestDBClient(string url, string token)
        {
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(url)
                .AuthenticateToken(token)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        }

        #endregion


        /// <summary>
        /// Get the Query client.
        /// </summary>
        /// <returns>the new client instance for the Query API</returns>
        public IQueryApi GetQueryApi()
        {
            return new QueryApi(_options, _apiClient);
        }

        /// <summary>
        /// Get the Write client.
        /// </summary>
        /// <param name="writeOptions">the configuration for a write client</param>
        /// <returns>the new client instance for the Write API</returns>
        public IWriteLineApi GetWriteApi(WriteOptions writeOptions)
        {
            var writeApi = new WriteLineApi(_options, writeOptions, this, _disposeNotification);

            return writeApi;
        }

        /// <summary>
        /// Get the Write client.
        /// </summary>
        /// <returns>the new client instance for the Write API</returns>
        public IWriteLineApi GetWriteApi()
        {
            return GetWriteApi(WriteOptions.CreateNew().Build());
        }

        /// <summary>
        /// Enable Gzip compress for http requests.
        ///
        /// <para>Currently only the "Write" and "Query" endpoints supports the Gzip compression.</para>
        /// </summary>
        /// <returns></returns>
        public QuestDBClient EnableGzip()
        {
            _gzipHandler.EnableGzip();

            return this;
        }

        /// <summary>
        /// Disable Gzip compress for http request body.
        /// </summary>
        /// <returns>this</returns>
        public QuestDBClient DisableGzip()
        {
            _gzipHandler.DisableGzip();

            return this;
        }

        /// <summary>
        /// Returns whether Gzip compress for http request body is enabled.
        /// </summary>
        /// <returns>true if gzip is enabled.</returns>
        public bool IsGzipEnabled()
        {
            return _gzipHandler.IsEnabledGzip();
        }

        internal static string AuthorizationHeader(string username, string password)
        {
            return "Basic " + Convert.ToBase64String(Encoding.Default.GetBytes(username + ":" + password));
        }

        public void Dispose()
        {
            //
            // Dispose child APIs
            //
            _disposeNotification.OnNext(default);
        }
    }
}