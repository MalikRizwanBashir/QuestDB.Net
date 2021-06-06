using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;

namespace Questdb.Net.Exceptions
{
    public class QuestdbException : Exception
    {
        public QuestdbException(string message, Exception exception = null) : this(message, 0, exception)
        {
        }

        public QuestdbException(Exception exception) : base(exception.Message, exception)
        {
            Code = 0;
        }

        public static Exception Create(object body)
        {
            return new Exception(Convert.ToString(body));
        }

        public QuestdbException(string message, int code, Exception exception = null) : base(message, exception)
        {
            Code = code;
            Status = 0;
        }

        /// <summary>
        ///     Gets the reference code unique to the error type. If the reference code is not present than return "0".
        /// </summary>
        public int Code { get; }

        /// <summary>
        ///     Gets the HTTP status code of the unsuccessful response. If the response is not present than return "0".
        /// </summary>
        public int Status { get; set; }
    }

    public class HttpException : QuestdbException
    {
        public HttpException(string message, int status, Exception exception = null) : base(message, 0, exception)
        {
            Status = status;
        }

        /// <summary>
        ///     The JSON unsuccessful response body.
        /// </summary>
        public JObject ErrorBody { get; set; }

        /// <summary>
        ///     The retry interval is used when the QuestDB server does not specify "Retry-After" header.
        /// </summary>
        public int? RetryAfter { get; set; }

        public static HttpException Create(IRestResponse requestResult, object body)
        {
            Arguments.CheckNotNull(requestResult, nameof(requestResult));

            var httpHeaders = ToHeaders(requestResult.Headers);

            return Create(body, httpHeaders, requestResult.ErrorMessage, requestResult.StatusCode,
                requestResult.ErrorException);
        }

        public static HttpException Create(IHttpResponse requestResult, object body)
        {
            Arguments.CheckNotNull(requestResult, nameof(requestResult));

            return Create(body, requestResult.Headers, requestResult.ErrorMessage, requestResult.StatusCode,
                requestResult.ErrorException);
        }

        public static HttpException Create(object content, IList<HttpHeader> headers, string ErrorMessage,
            HttpStatusCode statusCode, Exception exception = null)
        {
            string stringBody = null;
            var errorBody = new JObject();
            string errorMessage = null;

            int? retryAfter = null;
            {
                var retryHeader = headers.FirstOrDefault(header => header.Name.Equals("Retry-After"));
                if (retryHeader != null) retryAfter = Convert.ToInt32(retryHeader.Value);
            }

            if (content != null)
            {
                if (content is Stream)
                {
                    var stream = content as Stream;
                    var sr = new StreamReader(stream);
                    stringBody = sr.ReadToEnd();
                }
                else
                {
                    stringBody = content.ToString();
                }
            }

            if (!string.IsNullOrEmpty(stringBody))
            {
                try
                {
                    errorBody = JObject.Parse(stringBody);
                    if (errorBody.ContainsKey("message"))
                    {
                        errorMessage = errorBody.GetValue("message").ToString();
                    }
                }
                catch (JsonException)
                {
                    errorBody = new JObject();
                }
            }

            var keys = new[] { "X-Platform-Error-Code", "X-Quest-Error", "X-QuestDb-Error" };

            if (string.IsNullOrEmpty(errorMessage))
                errorMessage = headers
                    .Where(header => keys.Contains(header.Name, StringComparer.OrdinalIgnoreCase))
                    .Select(header => header.Value.ToString()).FirstOrDefault();

            if (string.IsNullOrEmpty(errorMessage)) errorMessage = ErrorMessage;
            if (string.IsNullOrEmpty(errorMessage)) errorMessage = stringBody;

            return new HttpException(errorMessage, (int)statusCode, exception)
            { ErrorBody = errorBody, RetryAfter = retryAfter };
        }

        public static List<HttpHeader> ToHeaders(IList<Parameter> parameters, ParameterType type = ParameterType.HttpHeader)
        {
            return parameters
                .Where(parameter => parameter.Type.Equals(type))
                .Select(h => new HttpHeader(h.Name, h.Value.ToString()))
                .ToList();
        }
    }
}