using Questdb.Net.Write;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net
{
    public interface IQuestDBClient
    {
        /// <summary>
        /// Get the Query client.
        /// </summary>
        /// <returns>the new client instance for the Query API</returns>
        IQueryApi GetQueryApi();

        /// <summary>
        /// Get the Write client.
        /// </summary>
        /// <returns>the new client instance for the Write API</returns>
        IWriteLineApi GetWriteApi();

        /// <summary>
        /// Enable Gzip compress for http requests.
        ///
        /// <para>Currently only the "Write" and "Query" endpoints supports the Gzip compression.</para>
        /// </summary>
        /// <returns></returns>
        IQuestDBClient EnableGzip();

        /// <summary>
        /// Disable Gzip compress for http request body.
        /// </summary>
        /// <returns>this</returns>
        IQuestDBClient DisableGzip();

        /// <summary>
        /// Returns whether Gzip compress for http request body is enabled.
        /// </summary>
        /// <returns>true if gzip is enabled.</returns>
        bool IsGzipEnabled();
    }
}
