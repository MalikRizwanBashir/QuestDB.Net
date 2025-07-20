using Microsoft.Extensions.DependencyInjection;
using Questdb.Net.Write;

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


        #region Keyed Repositories

        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// </summary>
        public static void AddKeyedQuestDb(this IServiceCollection serviceCollection, string key)
        {
            serviceCollection.AddKeyedTransient<IQuestDBClient>(key, (sp, _) =>
            {
                var client = new QuestDBClient();
                return client;
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="host">Server URL(IP)</param>
        public static void AddKeyedQuestDb(this IServiceCollection serviceCollection, string key, string host)
        {
            serviceCollection.AddKeyedTransient<IQuestDBClient>(key, (sp, _) =>
            {
                var client = new QuestDBClient(host);
                return client;
            });
        }

        /// <summary>
        /// Load configurations from configuration file using key "questdb"
        /// </summary>
        public static void AddKeyedQuestDb(this IServiceCollection serviceCollection, string key, WriteOptions writeOptions)
        {
            serviceCollection.AddKeyedTransient<IQuestDBClient>(key, (sp, _) =>
            {
                var client = new QuestDBClient(writeOptions);
                return client;
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="host">Server URL(IP)</param>
        public static void AddKeyedQuestDb(this IServiceCollection serviceCollection, string key, string host, WriteOptions writeOptions)
        {
            serviceCollection.AddKeyedTransient<IQuestDBClient>(key, (sp, _) =>
            {
                var client = new QuestDBClient(host, writeOptions);
                return client;
            });
        }

        #endregion Keyed Repositories
    }
}
