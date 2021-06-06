using Questdb.Net.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net
{
    public interface IQueryApi
    {
        /// <summary>
        /// Executes the query against the QuestDB
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        Task<string> QueryRawAsync(string query);

        /// <summary>
        /// Executes the query against the QuestDB
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        string QueryRaw(string query);

        /// <summary>
        /// Executes the query against the QuestDB and maps to QuestdbResponse
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        Task<QuestdbResponse> QueryAsync(string query);

        /// <summary>
        /// Executes the query against the QuestDB and maps to QuestdbResponse
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        QuestdbResponse Query(string query);

        /// <summary>
        /// Executes the query against the QuestDB
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        Task<string> QueryCSVAsync(string query);

        /// <summary>
        /// Executes the query against the QuestDB
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <returns>Measurements which are matched the query</returns>
        string QueryCSV(string query);

        /// <summary>
        /// Executes the query against the QuestDB and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        Task<IEnumerable<T>> QueryEnumerableAsync<T>(string query);

        /// <summary>
        /// Executes the query against the QuestDB and asynchronously maps
        /// response to enumerable of objects of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <typeparam name="T">the type of measurement</typeparam>
        /// <returns>Measurements which are matched the query</returns>
        IEnumerable<T> QueryEnumerable<T>(string query);

        /// <summary>
        /// Executes the query against the QuestDB and asynchronously stream <see cref="T"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <param name="onNext">the callback to consume the T result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action<Exception> onError,
            Action onComplete);

        /// <summary>
        /// Executes the query against the QuestDB and asynchronously stream <see cref="T"/>
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <param name="onNext">the callback to consume the T result with capability</param>
        /// <param name="onError">the callback to consume any error notification</param>
        /// <returns>async task</returns>
        Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action<Exception> onError);

        /// <summary>
        /// Executes the query against the QuestDB and asynchronously stream <see cref="T"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <param name="onNext">the callback to consume the T result with capability</param>
        /// <param name="onComplete">the callback to consume a notification about successfully end of stream</param>
        /// <returns>async task</returns>
        Task QueryAsync<T>(string query, Action<ICancellable, T> onNext,
            Action onComplete);

        /// <summary>
        /// Executes the query against the QuestDB 2.0 and asynchronously stream <see cref="T"/>
        /// to <see cref="onNext"/> consumer.
        /// </summary>
        /// <param name="query">the query to execute</param>
        /// <param name="onNext">the callback to consume the T result with capability</param>
        /// <returns>async task</returns>
        Task QueryAsync<T>(string query, Action<ICancellable, T> onNext);
    }
}
