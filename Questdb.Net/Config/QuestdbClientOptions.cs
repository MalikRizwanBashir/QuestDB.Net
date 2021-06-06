using Questdb.Net.Exceptions;
using Questdb.Net.Write;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text.RegularExpressions;
using System.Web;

namespace Questdb.Net.Config
{
    /// <summary>
    /// QuestDBClientOptions are used to configure the QuestDB 2.0 connections.
    /// </summary>
    public class QuestdbClientOptions
    {
        private static readonly Regex DurationRegex = new Regex(@"^(?<Amount>\d+)(?<Unit>[a-zA-Z]{0,2})$",
            RegexOptions.ExplicitCapture |
            RegexOptions.Compiled |
            RegexOptions.CultureInvariant |
            RegexOptions.RightToLeft);

        public string Url { get; }

        public AuthenticationScheme AuthScheme { get; }
        public char[] Token { get; }
        public string Username { get; }
        public char[] Password { get; }

        public string Database { get; }

        public TimeSpan Timeout { get; }
        public TimeSpan ReadWriteTimeout { get; }

        public PointSettings PointSettings { get; }

        public string UserAgent { get; }

        public Dictionary<String, String> DefaultHeaders { get; } = new Dictionary<string, string>();

        private QuestdbClientOptions(Builder builder)
        {
            Arguments.CheckNotNull(builder, nameof(builder));

            Url = builder.UrlString;
            AuthScheme = builder.AuthScheme;
            Token = builder.Token;
            Username = builder.Username;
            Password = builder.Password;

            Database = builder.DatabaseString;

            Timeout = builder.Timeout;
            ReadWriteTimeout = builder.ReadWriteTimeout;

            PointSettings = builder.PointSettings;
        }

        /// <summary>
        /// The scheme uses to Authentication.
        /// </summary>
        public enum AuthenticationScheme
        {
            /// <summary>
            /// Basic auth.
            /// </summary>
            Session,

            /// <summary>
            /// Authentication token.
            /// </summary>
            Token
        }

        /// <summary>
        /// A builder for <see cref="QuestdbClientOptions"/>.
        /// </summary>
        public sealed class Builder
        {
            internal string UrlString;

            internal AuthenticationScheme AuthScheme;
            internal char[] Token;
            internal string Username;
            internal char[] Password;
            internal TimeSpan Timeout;
            internal TimeSpan ReadWriteTimeout;

            internal string DatabaseString;

            internal PointSettings PointSettings = new PointSettings();

            public static Builder CreateNew()
            {
                return new Builder();
            }

            /// <summary>
            /// Set the url to connect the QuestDB.
            /// </summary>
            /// <param name="url">the url to connect the QuestDB. It must be defined.</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder Url(string url)
            {
                Arguments.CheckNonEmptyString(url, nameof(url));

                UrlString = url;

                return this;
            }

            /// <summary>
            /// Set the Timeout to connect the QuestDB.
            /// </summary>
            /// <param name="timeout">the timeout to connect the QuestDB. It must be defined.</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder TimeOut(TimeSpan timeout)
            {
                Arguments.CheckNotNull(timeout, nameof(timeout));

                Timeout = timeout;

                return this;
            }

            /// <summary>
            /// Set the read and write timeout from the QuestDB.
            /// </summary>
            /// <param name="readWriteTimeout">the timeout to read and write from the QuestDB. It must be defined.</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder ReadWriteTimeOut(TimeSpan readWriteTimeout)
            {
                Arguments.CheckNotNull(readWriteTimeout, nameof(readWriteTimeout));

                ReadWriteTimeout = readWriteTimeout;

                return this;
            }

            /// <summary>
            /// Setup authorization by <see cref="AuthenticationScheme.Session"/>.
            /// </summary>
            /// <param name="username">the username to use in the basic auth</param>
            /// <param name="password">the password to use in the basic auth</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder Authenticate(string username,
                char[] password)
            {
                Arguments.CheckNonEmptyString(username, "username");
                Arguments.CheckNotNull(password, "password");

                AuthScheme = AuthenticationScheme.Session;
                Username = username;
                Password = password;

                return this;
            }

            /// <summary>
            /// Setup authorization by <see cref="AuthenticationScheme.Token"/>.
            /// </summary>
            /// <param name="token">the token to use for the authorization</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder AuthenticateToken(char[] token)
            {
                Arguments.CheckNotNull(token, "token");

                AuthScheme = AuthenticationScheme.Token;
                Token = token;

                return this;
            }

            /// <summary>
            /// Setup authorization by <see cref="AuthenticationScheme.Token"/>.
            /// </summary>
            /// <param name="token">the token to use for the authorization</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder AuthenticateToken(string token)
            {
                return AuthenticateToken(token.ToCharArray());
            }

            /// <summary>
            /// Specify the default destination database for writes.
            /// </summary>
            /// <param name="database">default destination database for writes</param>
            /// <returns><see cref="Builder"/></returns>
            public Builder Database(string database)
            {
                DatabaseString = database;

                return this;
            }

            /// <summary>
            /// Add default tag that will be use for writes by Point and POJO.
            ///
            /// <para>
            /// The expressions can be:
            /// <list type="bullet">
            /// <item>"California Miner" - static value</item>
            /// <item>"${version}" - application settings</item>
            /// <item>"${env.hostname}" - environment property</item>
            /// </list>
            /// </para>
            /// </summary>
            /// <param name="tagName">the tag name</param>
            /// <param name="expression">the tag value expression</param>
            /// <returns></returns>
            public Builder AddDefaultTag(string tagName, string expression)
            {
                Arguments.CheckNotNull(tagName, nameof(tagName));

                PointSettings.AddDefaultTag(tagName, expression);

                return this;
            }

            /// <summary>
            /// Configure Builder via App.config.
            /// </summary>
            /// <returns><see cref="Builder"/></returns>
            internal Builder LoadConfig()
            {
                var config = (ConnectionSettings)ConfigurationManager.GetSection("questdb");

                var url = config?.Url;
                var database = config?.Database;
                var token = config?.Token;
                var timeout = config?.Timeout;
                var readWriteTimeout = config?.ReadWriteTimeout;

                return Configure(url, database, token, timeout, readWriteTimeout);
            }

            private Builder Configure(string url, string database, string token,
                string timeout, string readWriteTimeout)
            {
                Url(url);
                Database(database);

                if (!string.IsNullOrWhiteSpace(token))
                {
                    AuthenticateToken(token);
                }

                if (!string.IsNullOrWhiteSpace(timeout))
                {
                    TimeOut(ToTimeout(timeout));
                }

                if (!string.IsNullOrWhiteSpace(readWriteTimeout))
                {
                    ReadWriteTimeOut(ToTimeout(readWriteTimeout));
                }

                return this;
            }

            private TimeSpan ToTimeout(string value)
            {
                var matcher = DurationRegex.Match(value);
                if (!matcher.Success)
                {
                    throw new QuestdbException($"'{value}' is not a valid duration");
                }

                var amount = matcher.Groups["Amount"].Value;
                var unit = matcher.Groups["Unit"].Value;

                TimeSpan duration;
                switch (string.IsNullOrWhiteSpace(unit) ? "ms" : unit.ToLower())
                {
                    case "ms":
                        duration = TimeSpan.FromMilliseconds(double.Parse(amount));
                        break;

                    case "s":
                        duration = TimeSpan.FromSeconds(double.Parse(amount));
                        break;

                    case "m":
                        duration = TimeSpan.FromMinutes(double.Parse(amount));
                        break;

                    default:
                        throw new QuestdbException($"unknown unit for '{value}'");
                }

                return duration;
            }

            /// <summary>
            /// Build an instance of QuestDBClientOptions.
            /// </summary>
            /// <returns><see cref="QuestdbClientOptions"/></returns>
            /// <exception cref="InvalidOperationException">If url is not defined.</exception>
            public QuestdbClientOptions Build()
            {
                if (string.IsNullOrEmpty(UrlString))
                {
                    throw new InvalidOperationException("The url to connect the Questdb has to be defined.");
                }

                if (Timeout == TimeSpan.Zero || Timeout == TimeSpan.FromMilliseconds(0))
                {
                    Timeout = TimeSpan.FromSeconds(10);
                }

                if (ReadWriteTimeout == TimeSpan.Zero || ReadWriteTimeout == TimeSpan.FromMilliseconds(0))
                {
                    ReadWriteTimeout = TimeSpan.FromSeconds(10);
                }

                return new QuestdbClientOptions(this);
            }
        }
    }
}