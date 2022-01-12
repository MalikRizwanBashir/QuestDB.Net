using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using System.IO;
using System.Web;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using RestSharp;
using System.Diagnostics;
using Questdb.Net.Config;
using Questdb.Net.Exceptions;

namespace Questdb.Net
{
    /// <summary>
    /// API client is mainly responsible for making the HTTP call to the API backend.
    /// </summary>
    public partial class ApiClient
    {
        private readonly QuestdbClientOptions _options;
        private readonly GzipHandler _gzipHandler;

        private ExceptionFactory ExceptionFactory = (name, response) => null;

        private JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
        };

        /// <summary>
        /// Allows for extending request processing for <see cref="ApiClient"/> generated code.
        /// </summary>
        /// <param name="request">The RestSharp request object</param>
        partial void InterceptRequest(IRestRequest request);

        /// <summary>
        /// Initializes a new instance of the <see cref="ApiClient" /> class
        /// with default configuration.
        /// </summary>
        public ApiClient()
        {
            RestClient = new RestClient("http://localhost/exec");
        }

        public ApiClient(QuestdbClientOptions options, ExceptionFactory exceptionFactory, GzipHandler gzipHandler)
        {
            _options = options;
            _gzipHandler = gzipHandler;
            ExceptionFactory = exceptionFactory;

            var timeoutTotalMilliseconds = (int)options.Timeout.TotalMilliseconds;
            var totalMilliseconds = (int)options.ReadWriteTimeout.TotalMilliseconds;
            var serverBaseAddress = new Uri(options.Url);
            var host = serverBaseAddress.Host;
            IPAddress ipAddress = Dns.GetHostEntry(host).AddressList[0];
            var url = options.Url;
            url = url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url;
            RestClient = new RestClient(url + ":9000");
            RestClient.AutomaticDecompression = false;
        }

        /// <summary>
        /// Allows for extending response processing for <see cref="ApiClient"/> generated code.
        /// </summary>
        /// <param name="request">The RestSharp request object</param>
        /// <param name="response">The RestSharp response object</param>
        internal IRestResponse InterceptResponse(IRestRequest request, IRestResponse response)
        {
            return response;
        }

        partial void InterceptRequest(IRestRequest request)
        {
            BeforeIntercept(request);
        }

        internal void BeforeIntercept(IRestRequest request)
        {
            _gzipHandler.BeforeIntercept(request);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ApiClient" /> class
        /// with default configuration.
        /// </summary>
        /// <param name="basePath">The base path.</param>
        public ApiClient(String basePath)
        {
            if (String.IsNullOrEmpty(basePath))
                throw new ArgumentException("basePath cannot be empty");

            RestClient = new RestClient(basePath);
        }

        /// <summary>
        /// Gets or sets the default API client for making HTTP calls.
        /// </summary>
        /// <value>The default API client.</value>
        [Obsolete("ApiClient.Default is deprecated, please use 'Configuration.Default.ApiClient' instead.")]
        public static ApiClient Default;

        /// <summary>
        /// Gets or sets the RestClient.
        /// </summary>
        /// <value>An instance of the RestClient</value>
        public RestClient RestClient { get; set; }


        #region api calls

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">Query or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public async System.Threading.Tasks.Task<IRestResponse> PostQueryWithIRestResponseAsync(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>();
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter
            if (query != null && query.GetType() != typeof(byte[]))
            {
                localVarPostBody = this.Serialize(query); // http body (model) parameter
            }
            else
            {
                localVarPostBody = query; // byte array
            }

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);


            // make the HTTP request
            IRestResponse localVarResponse = (IRestResponse)await this.CallApiAsync(endpoint,
                Method.POST, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);

            int localVarStatusCode = (int)localVarResponse.StatusCode;

            if (ExceptionFactory != null)
            {
                Exception exception = ExceptionFactory("PostQuery", localVarResponse);
                if (exception != null) throw exception;
            }

            return localVarResponse;
        }

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">Query or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public IRestResponse PostQueryWithIRestResponse(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>();
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter
            if (query != null && query.GetType() != typeof(byte[]))
            {
                localVarPostBody = this.Serialize(query); // http body (model) parameter
            }
            else
            {
                localVarPostBody = query; // byte array
            }

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);


            // make the HTTP request
            IRestResponse localVarResponse = (IRestResponse)this.CallApi(endpoint,
                Method.POST, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);

            int localVarStatusCode = (int)localVarResponse.StatusCode;

            if (ExceptionFactory != null)
            {
                Exception exception = ExceptionFactory("PostQuery", localVarResponse);
                if (exception != null) throw exception;
            }

            return localVarResponse;
        }

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">Query or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public RestRequest PostQueryWithRestRequest(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>(_options.DefaultHeaders);
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter
            if (query != null && query.GetType() != typeof(byte[]))
            {
                localVarPostBody = this.Serialize(query); // http body (model) parameter
            }
            else
            {
                localVarPostBody = query; // byte array
            }

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);


            return this.PrepareRequest(endpoint,
                Method.POST, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);
        }

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">Query or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public async System.Threading.Tasks.Task<IRestResponse> GetQueryWithIRestResponseAsync(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>(_options.DefaultHeaders);
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);

            localVarQueryParams.Add(new KeyValuePair<string, string>("query", query));
            localVarQueryParams.Add(new KeyValuePair<string, string>("count", "true"));
            localVarQueryParams.Add(new KeyValuePair<string, string>("limit", "0, 10000"));

            // make the HTTP request
            IRestResponse localVarResponse = (IRestResponse)await this.CallApiAsync(endpoint,
                Method.GET, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);

            int localVarStatusCode = (int)localVarResponse.StatusCode;

            if (ExceptionFactory != null)
            {
                Exception exception = ExceptionFactory("PostQuery", localVarResponse);
                if (exception != null) throw exception;
            }

            return localVarResponse;
        }

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">Query or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public IRestResponse GetQueryWithIRestResponse(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>(_options.DefaultHeaders);
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);

            localVarQueryParams.Add(new KeyValuePair<string, string>("query", query));
            localVarQueryParams.Add(new KeyValuePair<string, string>("count", "true"));
            localVarQueryParams.Add(new KeyValuePair<string, string>("limit", "0, 10000"));

            // make the HTTP request
            IRestResponse localVarResponse = (IRestResponse)this.CallApi(endpoint,
                Method.GET, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);

            int localVarStatusCode = (int)localVarResponse.StatusCode;

            if (ExceptionFactory != null)
            {
                Exception exception = ExceptionFactory("PostQuery", localVarResponse);
                if (exception != null) throw exception;
            }

            return localVarResponse;
        }

        /// <summary>
        /// Query QuestDB 
        /// </summary>
        /// <param name="acceptEncoding">The Accept-Encoding request HTTP header advertises which content encoding, usually a compression algorithm, the client is able to understand. (optional, default to identity)</param>
        /// <param name="contentType"> (optional)</param>
        /// <param name="query">@uery or specification to execute (optional)</param>
        /// <returns>ApiResponse of string</returns>
        public RestRequest GetQueryWithRestRequest(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>(_options.DefaultHeaders);
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            // to determine the Content-Type header
            String[] localVarHttpContentTypes = new String[] {
                "application/json"
            };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding)); // header parameter
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType)); // header parameter

            // to determine the Accept header
            String[] localVarHttpHeaderAccepts = new String[] {
                "text/csv",
                "application/json"
            };

            localVarQueryParams.Add(new KeyValuePair<string, string>("query", query));
            localVarQueryParams.Add(new KeyValuePair<string, string>("count", "true"));
            localVarQueryParams.Add(new KeyValuePair<string, string>("limit", "0, 10000"));

            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);


            return this.PrepareRequest(endpoint,
                Method.GET, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);
        }


        // Creates and sets up a RestRequest prior to a call.
        internal RestRequest PrepareRequest(
            String path, RestSharp.Method method, List<KeyValuePair<String, String>> queryParams, Object postBody,
            Dictionary<String, String> headerParams, Dictionary<String, String> formParams,
            Dictionary<String, FileParameter> fileParams, Dictionary<String, String> pathParams,
            String contentType)
        {
            var request = new RestRequest(path, method);

            // add path parameter, if any
            foreach (var param in pathParams)
                request.AddParameter(param.Key, param.Value, ParameterType.UrlSegment);

            // add header parameter, if any
            foreach (var param in headerParams)
                request.AddHeader(param.Key, param.Value);

            // add query parameter, if any
            foreach (var param in queryParams)
                request.AddQueryParameter(param.Key, param.Value);

            // add form parameter, if any
            foreach (var param in formParams)
                request.AddParameter(param.Key, param.Value);

            // add file parameter, if any
            foreach (var param in fileParams)
            {
                request.AddFile(param.Value.Name, param.Value.Writer, param.Value.FileName, param.Value.ContentLength, param.Value.ContentType);
            }

            if (postBody != null) // http body (model or byte[]) parameter
            {
                request.AddParameter(contentType, postBody, ParameterType.RequestBody);
            }

            return request;
        }

        /// <summary>
        /// Makes the HTTP request (Sync).
        /// </summary>
        /// <param name="path">URL path.</param>
        /// <param name="method">HTTP method.</param>
        /// <param name="queryParams">Query parameters.</param>
        /// <param name="postBody">HTTP body (POST request).</param>
        /// <param name="headerParams">Header parameters.</param>
        /// <param name="formParams">Form parameters.</param>
        /// <param name="fileParams">File parameters.</param>
        /// <param name="pathParams">Path parameters.</param>
        /// <param name="contentType">Content Type of the request</param>
        /// <returns>Object</returns>
        public Object CallApi(
            String path, RestSharp.Method method, List<KeyValuePair<String, String>> queryParams, Object postBody,
            Dictionary<String, String> headerParams, Dictionary<String, String> formParams,
            Dictionary<String, FileParameter> fileParams, Dictionary<String, String> pathParams,
            String contentType)
        {
            var request = PrepareRequest(
                path, method, queryParams, postBody, headerParams, formParams, fileParams,
                pathParams, contentType);

            // set timeout

            RestClient.Timeout = (int)_options.Timeout.TotalMilliseconds;
            RestClient.ReadWriteTimeout = (int)_options.ReadWriteTimeout.TotalMilliseconds;
            // set user agent
            RestClient.UserAgent = _options.UserAgent;

            InterceptRequest(request);
            var response = RestClient.Execute(request);
            InterceptResponse(request, response);

            return (Object)response;
        }
        /// <summary>
        /// Makes the asynchronous HTTP request.
        /// </summary>
        /// <param name="path">URL path.</param>
        /// <param name="method">HTTP method.</param>
        /// <param name="queryParams">Query parameters.</param>
        /// <param name="postBody">HTTP body (POST request).</param>
        /// <param name="headerParams">Header parameters.</param>
        /// <param name="formParams">Form parameters.</param>
        /// <param name="fileParams">File parameters.</param>
        /// <param name="pathParams">Path parameters.</param>
        /// <param name="contentType">Content type.</param>
        /// <returns>The Task instance.</returns>
        public async System.Threading.Tasks.Task<Object> CallApiAsync(
            String path, RestSharp.Method method, List<KeyValuePair<String, String>> queryParams, Object postBody,
            Dictionary<String, String> headerParams, Dictionary<String, String> formParams,
            Dictionary<String, FileParameter> fileParams, Dictionary<String, String> pathParams,
            String contentType)
        {
            var request = PrepareRequest(
                path, method, queryParams, postBody, headerParams, formParams, fileParams,
                pathParams, contentType);
            InterceptRequest(request);
            var response = await RestClient.ExecuteAsync(request).ContinueWith(task => InterceptResponse(request, task.Result));
            //var response = await RestClient.ExecuteAsync(request);
            //InterceptResponse(request, response);
            return (Object)response;
        }


        #endregion

        #region helpers
        /// <summary>
        /// Escape string (url-encoded).
        /// </summary>
        /// <param name="str">String to be escaped.</param>
        /// <returns>Escaped string.</returns>
        public string EscapeString(string str)
        {
            return UrlEncode(str);
        }

        /// <summary>
        /// Create FileParameter based on Stream.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="stream">Input stream.</param>
        /// <returns>FileParameter.</returns>
        public FileParameter ParameterToFile(string name, Stream stream)
        {
            if (stream is FileStream)
                return FileParameter.Create(name, ReadAsBytes(stream), Path.GetFileName(((FileStream)stream).Name));
            else
                return FileParameter.Create(name, ReadAsBytes(stream), "no_file_name_provided");
        }

        /// <summary>
        /// If parameter is DateTime, output in a formatted string (default ISO 8601), customizable with Configuration.DateTime.
        /// If parameter is a list, join the list with ",".
        /// Otherwise just return the string.
        /// </summary>
        /// <param name="obj">The parameter (header, path, query, form).</param>
        /// <returns>Formatted string.</returns>
        public string ParameterToString(object obj)
        {
            if (obj is DateTime)
                // Return a formatted date string - Can be customized with Configuration.DateTimeFormat
                // Defaults to an ISO 8601, using the known as a Round-trip date/time pattern ("o")
                // https://msdn.microsoft.com/en-us/library/az4se3k1(v=vs.110).aspx#Anchor_8
                // For example: 2009-06-15T13:45:30.0000000
                return ((DateTime)obj).ToString("o");
            else if (obj is DateTimeOffset)
                // Return a formatted date string - Can be customized with Configuration.DateTimeFormat
                // Defaults to an ISO 8601, using the known as a Round-trip date/time pattern ("o")
                // https://msdn.microsoft.com/en-us/library/az4se3k1(v=vs.110).aspx#Anchor_8
                // For example: 2009-06-15T13:45:30.0000000
                return ((DateTimeOffset)obj).ToString("o");
            else if (obj is IList)
            {
                var flattenedString = new StringBuilder();
                foreach (var param in (IList)obj)
                {
                    if (flattenedString.Length > 0)
                        flattenedString.Append(",");
                    flattenedString.Append(param);
                }
                return flattenedString.ToString();
            }
            else if (obj is Enum)
            {
                var name = Convert.ToString(obj);
                name = obj.GetType().GetField(name)
                .GetCustomAttributes(typeof(EnumMemberAttribute), true)
                .Cast<EnumMemberAttribute>()
                .Select(a => a.Value)
                .SingleOrDefault() ?? name;

                return name;
            }
            else
                return Convert.ToString(obj);
        }

        /// <summary>
        /// Deserialize the JSON string into a proper object.
        /// </summary>
        /// <param name="response">The HTTP response.</param>
        /// <param name="type">Object type.</param>
        /// <returns>Object representation of the JSON string.</returns>
        public object Deserialize(IRestResponse response, Type type)
        {
            var headers = response.Headers;
            if (type == typeof(byte[])) // return byte array
            {
                return response.RawBytes;
            }

            // TODO: ? if (type.IsAssignableFrom(typeof(Stream)))
            if (type == typeof(Stream))
            {
                if (headers != null)
                {
                    var filePath = Path.GetTempPath();
                    var regex = new Regex(@"Content-Disposition=.*filename=['""]?([^'""\s]+)['""]?$");
                    foreach (var header in headers)
                    {
                        var match = regex.Match(header.ToString());
                        if (match.Success)
                        {
                            string fileName = filePath + SanitizeFilename(match.Groups[1].Value.Replace("\"", "").Replace("'", ""));
                            File.WriteAllBytes(fileName, response.RawBytes);
                            return new FileStream(fileName, FileMode.Open);
                        }
                    }
                }
                var stream = new MemoryStream(response.RawBytes);
                return stream;
            }

            if (type.Name.StartsWith("System.Nullable`1[[System.DateTime")) // return a datetime object
            {
                return DateTime.Parse(response.Content, null, System.Globalization.DateTimeStyles.RoundtripKind);
            }

            if (type == typeof(String) || type.Name.StartsWith("System.Nullable")) // return primitive type
            {
                return ConvertType(response.Content, type);
            }

            // at this point, it must be a model (json)
            try
            {
                return JsonConvert.DeserializeObject(response.Content, type, serializerSettings);
            }
            catch (Exception e)
            {
                throw new QuestdbException(e.Message, e);
            }
        }

        /// <summary>
        /// Serialize an input (model) into JSON string
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>JSON string.</returns>
        public String Serialize(object obj)
        {
            try
            {
                return obj != null ? JsonConvert.SerializeObject(obj) : null;
            }
            catch (Exception e)
            {
                throw new QuestdbException(e.Message, e);
            }
        }

        /// <summary>
        ///Check if the given MIME is a JSON MIME.
        ///JSON MIME examples:
        ///    application/json
        ///    application/json; charset=UTF8
        ///    APPLICATION/JSON
        ///    application/vnd.company+json
        /// </summary>
        /// <param name="mime">MIME</param>
        /// <returns>Returns True if MIME type is json.</returns>
        public bool IsJsonMime(String mime)
        {
            var jsonRegex = new Regex("(?i)^(application/json|[^;/ \t]+/[^;/ \t]+[+]json)[ \t]*(;.*)?$");
            return mime != null && (jsonRegex.IsMatch(mime) || mime.Equals("application/json-patch+json"));
        }

        /// <summary>
        /// Select the Content-Type header's value from the given content-type array:
        /// if JSON type exists in the given array, use it;
        /// otherwise use the first one defined in 'consumes'
        /// </summary>
        /// <param name="contentTypes">The Content-Type array to select from.</param>
        /// <returns>The Content-Type header to use.</returns>
        public String SelectHeaderContentType(String[] contentTypes)
        {
            if (contentTypes.Length == 0)
                return "application/json";

            foreach (var contentType in contentTypes)
            {
                if (IsJsonMime(contentType.ToLower()))
                    return contentType;
            }

            return contentTypes[0]; // use the first content type specified in 'consumes'
        }

        /// <summary>
        /// Select the Accept header's value from the given accepts array:
        /// if JSON exists in the given array, use it;
        /// otherwise use all of them (joining into a string)
        /// </summary>
        /// <param name="accepts">The accepts array to select from.</param>
        /// <returns>The Accept header to use.</returns>
        public String SelectHeaderAccept(String[] accepts)
        {
            if (accepts.Length == 0)
                return null;

            if (accepts.Contains("application/json", StringComparer.OrdinalIgnoreCase))
                return "application/json";

            return String.Join(",", accepts);
        }

        /// <summary>
        /// Encode string in base64 format.
        /// </summary>
        /// <param name="text">String to be encoded.</param>
        /// <returns>Encoded string.</returns>
        public static string Base64Encode(string text)
        {
            return System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(text));
        }

        /// <summary>
        /// Dynamically cast the object into target type.
        /// </summary>
        /// <param name="fromObject">Object to be casted</param>
        /// <param name="toObject">Target type</param>
        /// <returns>Casted object</returns>
        public static dynamic ConvertType(dynamic fromObject, Type toObject)
        {
            return Convert.ChangeType(fromObject, toObject);
        }

        /// <summary>
        /// Convert stream to byte array
        /// </summary>
        /// <param name="inputStream">Input stream to be converted</param>
        /// <returns>Byte array</returns>
        public static byte[] ReadAsBytes(Stream inputStream)
        {
            byte[] buf = new byte[16 * 1024];
            using (MemoryStream ms = new MemoryStream())
            {
                int count;
                while ((count = inputStream.Read(buf, 0, buf.Length)) > 0)
                {
                    ms.Write(buf, 0, count);
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// URL encode a string
        /// Credit/Ref: https://github.com/restsharp/RestSharp/blob/master/RestSharp/Extensions/StringExtensions.cs#L50
        /// </summary>
        /// <param name="input">String to be URL encoded</param>
        /// <returns>Byte array</returns>
        public static string UrlEncode(string input)
        {
            const int maxLength = 32766;

            if (input == null)
            {
                throw new ArgumentNullException("input");
            }

            if (input.Length <= maxLength)
            {
                return Uri.EscapeDataString(input);
            }

            StringBuilder sb = new StringBuilder(input.Length * 2);
            int index = 0;

            while (index < input.Length)
            {
                int length = Math.Min(input.Length - index, maxLength);
                string subString = input.Substring(index, length);

                sb.Append(Uri.EscapeDataString(subString));
                index += subString.Length;
            }

            return sb.ToString();
        }

        /// <summary>
        /// Sanitize filename by removing the path
        /// </summary>
        /// <param name="filename">Filename</param>
        /// <returns>Filename</returns>
        public static string SanitizeFilename(string filename)
        {
            Match match = Regex.Match(filename, @".*[/\\](.*)$");

            if (match.Success)
            {
                return match.Groups[1].Value;
            }
            else
            {
                return filename;
            }
        }

        /// <summary>
        /// Convert params to key/value pairs. 
        /// Use collectionFormat to properly format lists and collections.
        /// </summary>
        /// <param name="name">Key name.</param>
        /// <param name="value">Value object.</param>
        /// <returns>A list of KeyValuePairs</returns>
        public IEnumerable<KeyValuePair<string, string>> ParameterToKeyValuePairs(string collectionFormat, string name, object value)
        {
            var parameters = new List<KeyValuePair<string, string>>();

            if (IsCollection(value) && collectionFormat == "multi")
            {
                var valueCollection = value as IEnumerable;
                parameters.AddRange(from object item in valueCollection select new KeyValuePair<string, string>(name, ParameterToString(item)));
            }
            else
            {
                parameters.Add(new KeyValuePair<string, string>(name, ParameterToString(value)));
            }

            return parameters;
        }

        /// <summary>
        /// Check if generic object is a collection.
        /// </summary>
        /// <param name="value"></param>
        /// <returns>True if object is a collection type</returns>
        private static bool IsCollection(object value)
        {
            return value is IList || value is ICollection;
        }

        #endregion
    }
}
