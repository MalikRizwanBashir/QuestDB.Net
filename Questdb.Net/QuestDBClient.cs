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
        private readonly WriteOptions _writeOptions;
        private readonly IQueryApi _queryAPI;
        private readonly IWriteLineApi _writeAPI;

        private readonly QuestdbClientOptions _options;

        private readonly Subject<TCPResponse> _disposeNotification = new Subject<TCPResponse>();

        #region ctor

        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// </summary>
        public QuestDBClient()
        {
            _writeOptions = WriteOptions.CreateNew().Build();
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .LoadConfig()
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);

            _queryAPI = new QueryApi(_options, _apiClient);
            _writeAPI = new WriteLineApi(_options, _writeOptions, this, _disposeNotification);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="host">Server URL(IP)</param>
        public QuestDBClient(string host)
        {
            _writeOptions = WriteOptions.CreateNew().Build();
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(host)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);

            _queryAPI = new QueryApi(_options, _apiClient);
            _writeAPI = new WriteLineApi(_options, _writeOptions, this, _disposeNotification);
        }

        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// <param name="writeOptions">the configuration for a write client</param>
        /// </summary>
        public QuestDBClient(WriteOptions writeOptions)
        {
            _writeOptions = writeOptions;
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .LoadConfig()
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);

            _queryAPI = new QueryApi(_options, _apiClient);
            _writeAPI = new WriteLineApi(_options, _writeOptions, this, _disposeNotification);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="writeOptions">the configuration for a write client</param>
        /// <param name="host">Server URL(IP)</param>
        public QuestDBClient(string host, WriteOptions writeOptions)
        {
            _writeOptions = writeOptions;
            _options = QuestdbClientOptions.Builder
                .CreateNew()
                .Url(host)
                .Build();

            _gzipHandler = new GzipHandler();
            _exceptionFactory = (methodName, response) =>
                !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
            _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);

            _queryAPI = new QueryApi(_options, _apiClient);
            _writeAPI = new WriteLineApi(_options, _writeOptions, this, _disposeNotification);
        }

        #region authentication not implemented yet
        //public QuestDBClient(string url, string username, char[] password)
        //{
        //    _options = QuestdbClientOptions.Builder
        //        .CreateNew()
        //        .Url(url)
        //        .Authenticate(username, password)
        //        .Build();

        //    _gzipHandler = new GzipHandler();
        //    _exceptionFactory = (methodName, response) =>
        //        !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
        //    _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        //}

        //public QuestDBClient(string host, char[] token)
        //{
        //    _options = QuestdbClientOptions.Builder
        //        .CreateNew()
        //        .Url(host)
        //        .AuthenticateToken(token)
        //        .Build();

        //    _gzipHandler = new GzipHandler();
        //    _exceptionFactory = (methodName, response) =>
        //        !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
        //    _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        //}

        //public QuestDBClient(string url, string token)
        //{
        //    _options = QuestdbClientOptions.Builder
        //        .CreateNew()
        //        .Url(url)
        //        .AuthenticateToken(token)
        //        .Build();

        //    _gzipHandler = new GzipHandler();
        //    _exceptionFactory = (methodName, response) =>
        //        !response.IsSuccessful ? HttpException.Create(response, response.Content) : null;
        //    _apiClient = new ApiClient(_options, _exceptionFactory, _gzipHandler);
        //}
        #endregion

        #endregion


        /// <summary>
        /// Get the Query client.
        /// </summary>
        /// <returns>the new client instance for the Query API</returns>
        public IQueryApi GetQueryApi()
        {
            return _queryAPI;
        }

        /// <summary>
        /// Get the Write client.
        /// </summary>
        /// <returns>the new client instance for the Write API</returns>
        public IWriteLineApi GetWriteApi()
        {
            return _writeAPI;
        }

        /// <summary>
        /// Enable Gzip compress for http requests.
        ///
        /// <para>Currently only the "Write" and "Query" endpoints supports the Gzip compression.</para>
        /// </summary>
        /// <returns></returns>
        public IQuestDBClient EnableGzip()
        {
            _gzipHandler.EnableGzip();

            return this;
        }

        /// <summary>
        /// Disable Gzip compress for http request body.
        /// </summary>
        /// <returns>this</returns>
        public IQuestDBClient DisableGzip()
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