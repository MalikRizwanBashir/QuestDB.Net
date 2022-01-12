using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net.Client
{
    public class TcpService : IDisposable
    {
        private TcpClient _tcpClient;
        private readonly string _tcpHostName;
        private readonly int _tcpPort;

        public TcpService(string url, int port)
        {
            url = url.Replace("localhost", "127.0.0.1").Replace("Localhost", "127.0.0.1");
            var serverBaseAddress = new Uri(url);
            _tcpHostName = serverBaseAddress.Host;
            _tcpPort = port;
            GetClient();
        }

        private TcpClient GetClient()
        {
            if (_tcpClient == null || !_tcpClient.Connected)
            {
                _tcpClient = new TcpClient();
                IPAddress ipAddress = Dns.GetHostEntry(_tcpHostName).AddressList[0];
                IPEndPoint ipEndPoint = new IPEndPoint(ipAddress, _tcpPort);

                _tcpClient.Connect(ipEndPoint);
            }
            return _tcpClient;
        }

        public async Task<TCPResponse> SendAsync(byte[] buffer)
        {
            GetClient();
            TCPResponse response;
            NetworkStream stream = _tcpClient.GetStream();
            try
            {
                // Send the message to the connected TcpServer.
                await stream.WriteAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                response = new TCPResponse(true, "Data sent");
                return response;
            }
            catch (Exception ex)
            {
                stream.Close();
                if (_tcpClient.Connected)
                    _tcpClient.Close();
                _tcpClient.Dispose();
                response = new TCPResponse(false, ex.Message);
                return response;
            }
        }
        public void Dispose()
        {
            if (_tcpClient.Connected)
                _tcpClient.Close();
            _tcpClient.Dispose();
        }
    }

    public class TCPResponse
    {
        public TCPResponse(bool isSuccess, string message)
        {

        }
        public bool isSuccess { get; set; }
        public string message { get; set; }
    }
}
