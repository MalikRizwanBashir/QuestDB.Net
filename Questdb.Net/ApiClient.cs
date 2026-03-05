using Newtonsoft.Json;
using Questdb.Net.Config;
using Questdb.Net.Exceptions;
using RestSharp;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Questdb.Net
{
    /// <summary>
    /// API client is mainly responsible for making the HTTP call to the API backend.
    /// </summary>
    public partial class ApiClient
    {
        private readonly QuestdbClientOptions _options;
        private readonly GzipHandler _gzipHandler;
        private JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
        };

        private ExceptionFactory ExceptionFactory = (name, response) => null;

        public RestClient RestClient { get; set; }

        public ApiClient()
        {
            RestClient = new RestClient(new RestClientOptions("http://localhost/exec"));
        }

        public ApiClient(QuestdbClientOptions options, ExceptionFactory exceptionFactory, GzipHandler gzipHandler)
        {
            _options = options;
            _gzipHandler = gzipHandler;
            ExceptionFactory = exceptionFactory;

            var url = options.Url.TrimEnd('/');
            RestClient = new RestClient(new RestClientOptions($"{url}:9000"));
        }

        public void InterceptRequest(RestRequest request)
        {
            _gzipHandler.BeforeIntercept(request);
        }

        public async Task<RestResponse> PostQueryWithIRestResponseAsync(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var request = PrepareRequest(endpoint, Method.Post, acceptEncoding, contentType, query);
            var response = await RestClient.ExecuteAsync(request);
            if (ExceptionFactory != null)
            {
                var ex = ExceptionFactory("PostQuery", response);
                if (ex != null) throw ex;
            }
            return response;
        }

        public RestResponse PostQueryWithIRestResponse(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var request = PrepareRequest(endpoint, Method.Post, acceptEncoding, contentType, query);
            var response = RestClient.Execute(request);
            if (ExceptionFactory != null)
            {
                var ex = ExceptionFactory("PostQuery", response);
                if (ex != null) throw ex;
            }
            return response;
        }

        public async Task<RestResponse> GetQueryWithIRestResponseAsync(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var request = PrepareRequest(endpoint, Method.Get, acceptEncoding, contentType, null);
            request.AddQueryParameter("query", query);
            request.AddQueryParameter("count", "true");
            request.AddQueryParameter("limit", "0, 10000");

            var response = await RestClient.ExecuteAsync(request);
            if (ExceptionFactory != null)
            {
                var ex = ExceptionFactory("GetQuery", response);
                if (ex != null) throw ex;
            }
            return response;
        }

        public RestResponse GetQueryWithIRestResponse(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var request = PrepareRequest(endpoint, Method.Get, acceptEncoding, contentType, null);
            request.AddQueryParameter("query", query);
            request.AddQueryParameter("count", "true");
            request.AddQueryParameter("limit", "0, 10000");

            var response = RestClient.Execute(request);
            if (ExceptionFactory != null)
            {
                var ex = ExceptionFactory("GetQuery", response);
                if (ex != null) throw ex;
            }
            return response;
        }

        private RestRequest PrepareRequest(string path, Method method, string acceptEncoding, string contentType, object body)
        {
            var request = new RestRequest(path, method);

            if (!string.IsNullOrEmpty(acceptEncoding))
                request.AddHeader("Accept-Encoding", acceptEncoding);
            if (!string.IsNullOrEmpty(contentType))
                request.AddHeader("Content-Type", contentType);

            request.AddHeader("Accept", "application/json, text/csv");

            if (body != null)
                request.AddStringBody(Serialize(body), contentType ?? "application/json");

            InterceptRequest(request);
            return request;
        }

        internal void BeforeIntercept(RestRequest request)
        {
            _gzipHandler.BeforeIntercept(request);
        }


        public RestRequest GetQueryWithRestRequest(string endpoint = "/exec", string acceptEncoding = null, string contentType = null, string query = null)
        {
            var localVarPathParams = new Dictionary<String, String>();
            var localVarQueryParams = new List<KeyValuePair<String, String>>();
            var localVarHeaderParams = new Dictionary<String, String>(_options.DefaultHeaders);
            var localVarFormParams = new Dictionary<String, String>();
            var localVarFileParams = new Dictionary<String, FileParameter>();
            Object localVarPostBody = null;

            String[] localVarHttpContentTypes = new String[] { "application/json" };
            String localVarHttpContentType = this.SelectHeaderContentType(localVarHttpContentTypes);

            if (acceptEncoding != null) localVarHeaderParams.Add("Accept-Encoding", this.ParameterToString(acceptEncoding));
            if (contentType != null) localVarHeaderParams.Add("Content-Type", this.ParameterToString(contentType));

            String[] localVarHttpHeaderAccepts = new String[] { "text/csv", "application/json" };
            String localVarHttpHeaderAccept = this.SelectHeaderAccept(localVarHttpHeaderAccepts);
            if (localVarHttpHeaderAccept != null && !localVarHeaderParams.ContainsKey("Accept"))
                localVarHeaderParams.Add("Accept", localVarHttpHeaderAccept);

            localVarQueryParams.Add(new KeyValuePair<string, string>("query", query));
            localVarQueryParams.Add(new KeyValuePair<string, string>("count", "true"));
            localVarQueryParams.Add(new KeyValuePair<string, string>("limit", "0, 10000"));

            return this.PrepareRequest(endpoint,
                Method.Get, localVarQueryParams, localVarPostBody, localVarHeaderParams, localVarFormParams, localVarFileParams,
                localVarPathParams, localVarHttpContentType);
        }

        private RestRequest PrepareRequest(
    string path,
    Method method,
    List<KeyValuePair<string, string>> queryParams,
    object postBody,
    Dictionary<string, string> headerParams,
    Dictionary<string, string> formParams,
    Dictionary<string, FileParameter> fileParams,
    Dictionary<string, string> pathParams,
    string contentType)
        {
            var request = new RestRequest(path, method);

            // Apply path parameters
            foreach (var param in pathParams)
            {
                request.AddUrlSegment(param.Key, param.Value);
            }

            // Apply headers
            foreach (var header in headerParams)
            {
                request.AddOrUpdateHeader(header.Key, header.Value);
            }

            // Apply query parameters
            foreach (var queryParam in queryParams)
            {
                request.AddQueryParameter(queryParam.Key, queryParam.Value);
            }

            // Apply form parameters
            foreach (var formParam in formParams)
            {
                request.AddParameter(formParam.Key, formParam.Value, ParameterType.GetOrPost);
            }

            // Apply files
            foreach (var fileParam in fileParams)
            {
                request.AddFile(fileParam.Value.Name, fileParam.Value.GetFile, fileParam.Value.FileName, fileParam.Value.ContentType);
            }

            // Set Accept header if not already set
            if (!headerParams.ContainsKey("Accept"))
            {
                request.AddHeader("Accept", "application/json, text/csv");
            }

            // Add body if exists
            if (postBody != null)
            {
                if (postBody is byte[] bytes)
                {
                    request.AddParameter(contentType, bytes, ParameterType.RequestBody);
                }
                else
                {
                    request.AddStringBody(postBody.ToString(), contentType ?? "application/json");
                }
            }

            return request;
        }


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

        public String SelectHeaderAccept(String[] accepts)
        {
            if (accepts.Length == 0)
                return null;

            if (accepts.Contains("application/json", StringComparer.OrdinalIgnoreCase))
                return "application/json";

            return String.Join(",", accepts);
        }


        public bool IsJsonMime(String mime)
        {
            var jsonRegex = new Regex("(?i)^(application/json|[^;/ \\t]+/[^;/ \\t]+[+]json)[ \\t]*(;.*)?$");
            return mime != null && (jsonRegex.IsMatch(mime) || mime.Equals("application/json-patch+json"));
        }


        public string Serialize(object obj)
        {
            return obj != null ? JsonConvert.SerializeObject(obj) : null;
        }

        public string ParameterToString(object obj)
        {
            if (obj is DateTime dt)
                return dt.ToString("o");
            if (obj is DateTimeOffset dto)
                return dto.ToString("o");
            if (obj is IList list)
                return string.Join(",", list.Cast<object>());
            if (obj is Enum e)
            {
                var name = Convert.ToString(e);
                var attr = e.GetType().GetField(name).GetCustomAttributes(typeof(EnumMemberAttribute), true).FirstOrDefault() as EnumMemberAttribute;
                return attr?.Value ?? name;
            }
            return Convert.ToString(obj);
        }

        public object Deserialize(RestResponse response, Type type)
        {
            if (type == typeof(byte[]))
                return response.RawBytes;
            if (type == typeof(Stream))
                return new MemoryStream(response.RawBytes);
            if (type == typeof(string))
                return response.Content;
            try
            {
                return JsonConvert.DeserializeObject(response.Content, type, serializerSettings);
            }
            catch (Exception e)
            {
                throw new QuestdbException(e.Message, e);
            }
        }

        public static string Base64Encode(string text)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
        }
    }
}
