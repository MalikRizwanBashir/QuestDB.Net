using Microsoft.Extensions.DependencyInjection;
using Questdb.Net.Write;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net
{
    public static class IServiceCollectionExtentions
    {
        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// </summary>
        public static void AddQuestDb(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddTransient<IQuestDBClient>(opt => new QuestDBClient());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="host">Server URL(IP)</param>
        public static void AddQuestDb(this IServiceCollection serviceCollection, string host)
        {
            serviceCollection.AddTransient<IQuestDBClient>(opt => new QuestDBClient(host));
        }

        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// </summary>
        public static void AddQuestDb(this IServiceCollection serviceCollection, WriteOptions writeOptions)
        {
            serviceCollection.AddTransient<IQuestDBClient>(opt => new QuestDBClient(writeOptions));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="host">Server URL(IP)</param>
        public static void AddQuestDb(this IServiceCollection serviceCollection, string host, WriteOptions writeOptions)
        {
            serviceCollection.AddTransient<IQuestDBClient>(opt => new QuestDBClient(host, writeOptions));
        }
    }
}
