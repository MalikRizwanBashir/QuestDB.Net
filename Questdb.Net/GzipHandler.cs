using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using RestSharp;

namespace Questdb.Net
{
    public class GzipHandler
    {
        private bool _enabled;

        public void EnableGzip()
        {
            _enabled = true;
        }

        public void DisableGzip()
        {
            _enabled = false;
        }

        public bool IsEnabledGzip()
        {
            return _enabled;
        }

        public void BeforeIntercept(RestRequest request)
        {
            if (!_enabled)
            {
                request.AddOrUpdateHeader("Accept-Encoding", "identity");
            }
            else if (request.Method == Method.Post)
            {
                request.AddOrUpdateHeader("Content-Encoding", "gzip");
                request.AddOrUpdateHeader("Accept-Encoding", "identity");

                var bodyParam = request.Parameters.FirstOrDefault(p => p.Type == ParameterType.RequestBody);

                if (bodyParam != null && bodyParam.Value is string bodyValue)
                {
                    var compressedBytes = CompressString(bodyValue);

                    // Log warning: body cannot be replaced here directly
                    throw new InvalidOperationException("Body replacement is not supported after being added in RestSharp 107+. Compress the body before adding to the request.");
                }
            }
            else if (request.Method == Method.Get)
            {
                request.AddOrUpdateHeader("Accept-Encoding", "gzip");
            }
            else
            {
                request.AddOrUpdateHeader("Accept-Encoding", "identity");
            }
        }

        private static byte[] CompressString(string input)
        {
            var inputBytes = Encoding.UTF8.GetBytes(input);
            using var outputStream = new MemoryStream();
            using (var gzip = new GZipStream(outputStream, CompressionMode.Compress))
            {
                gzip.Write(inputBytes, 0, inputBytes.Length);
            }
            return outputStream.ToArray();
        }


        public object AfterIntercept(int statusCode, Func<IList<HeaderParameter>> headers, object body)
        {
            return body;
        }


    }
}