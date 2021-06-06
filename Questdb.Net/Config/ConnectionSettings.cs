using System.Configuration;

namespace Questdb.Net.Config

{
    public class ConnectionSettings : ConfigurationSection
    {
        /// <summary>
        /// The url to connect the QuestDB.
        /// </summary>
        [ConfigurationProperty("url", IsKey = true, IsRequired = true)]
        public string Url
        {
            get => (string) base["url"];
            set => base["url"] = value;
        }

        /// <summary>
        /// The Line port to connect the QuestDB.
        /// </summary>
        [ConfigurationProperty("linePort", IsKey = true, IsRequired = true)]
        public string LinePort
        {
            get => (string)base["linePort"];
            set => base["linePort"] = value;
        }

        /// <summary>
        /// The wire port to connect the QuestDB.
        /// </summary>
        [ConfigurationProperty("wirePort", IsKey = true, IsRequired = true)]
        public string WirePort
        {
            get => (string)base["wirePort"];
            set => base["wirePort"] = value;
        }

        /// <summary>
        /// Specify the default destination bucket for writes.
        /// </summary>
        [ConfigurationProperty("database", IsKey = true, IsRequired = false)]
        public string Database
        {
            get => (string) base["database"];
            set => base["database"] = value;
        }

        /// <summary>
        /// The token to use for the authorization.
        /// </summary>
        [ConfigurationProperty("token", IsKey = true, IsRequired = false)]
        public string Token
        {
            get => (string) base["token"];
            set => base["token"] = value;
        }

        /// <summary>
        /// The timeout to read and write from the QuestDB.
        /// </summary>
        [ConfigurationProperty("readWriteTimeout", IsKey = true, IsRequired = false)]
        public string ReadWriteTimeout
        {
            get => (string) base["readWriteTimeout"];
            set => base["readWriteTimeout"] = value;
        }

        /// <summary>
        /// The timeout to connect the QuestDB.
        /// </summary>
        [ConfigurationProperty("timeout", IsKey = true, IsRequired = false)]
        public string Timeout
        {
            get => (string) base["timeout"];
            set => base["timeout"] = value;
        }
    }
}