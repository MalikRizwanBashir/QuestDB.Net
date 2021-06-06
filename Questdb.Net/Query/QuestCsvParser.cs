using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using NodaTime;
using NodaTime.Text;
using Questdb.Net.Exceptions;

namespace Questdb.Net.Query
{
    /// <summary>
    /// This class us used to construct <see cref="QuestdbResponse"/> from CSV.+9
    /// </summary>
    public class QuestCsvParser
    {
        private enum ParsingState
        {
            Normal,
            InError
        }

        public interface ICSVResponseConsumer
        {
            /// <summary>
            /// Add new <see cref="QuestdbResponse"/> to a consumer.
            /// </summary>
            /// <param name="cancellable">cancellable</param>
            /// <param name="table">new <see cref="FluxTable"/></param>
            void Accept(ICancellable cancellable, QuestdbResponse table);

            /// <summary>
            /// Add new <see cref="FluxRecord"/> to a consumer.
            /// </summary>
            /// <param name="cancellable">cancellable</param>
            /// <param name="record">new <see cref="FluxRecord"/></param>
            void Accept(ICancellable cancellable, List<TableColumn> columns, List<object> record);
        }

        public class CSVResponseConsumerTable : ICSVResponseConsumer
        {
            public List<QuestdbResponse> Tables { get; } = new List<QuestdbResponse>();

            public void Accept(ICancellable cancellable, QuestdbResponse table)
            {
                Tables.Add(table);
            }

            public void Accept(ICancellable cancellable, List<TableColumn> columns, List<object> record)
            {
                Tables[0].DataSet.Add(record);
            }
        }

        public void ParseCSVResponse(string source, ICancellable cancellable, ICSVResponseConsumer consumer)
        {
            Arguments.CheckNonEmptyString(source, "source");

            ParseCSVResponse(ToStream(source), cancellable, consumer);
        }

        /// <summary>
        /// Parse Flux CSV response to <see cref="ICSVResponseConsumer"/>.
        /// </summary>
        /// <param name="source">CSV Data source</param>
        /// <param name="cancellable">to cancel parsing</param>
        /// <param name="consumer">to accept <see cref="FluxTable"/> or <see cref="FluxRecord"/></param>
        public void ParseCSVResponse(Stream source, ICancellable cancellable, ICSVResponseConsumer consumer)
        {
            Arguments.CheckNotNull(source, "source");
            using var csv = new CsvReader(new StreamReader(source), CultureInfo.InvariantCulture);
            var state = new ParseFluxResponseState { csv = csv };

            while (csv.Read())
            {
                if (cancellable != null && cancellable.IsCancelled())
                {
                    return;
                }

                foreach (var (table, record) in ParseNextResponse(state))
                {
                    if (record == null)
                        consumer.Accept(cancellable, table);
                    else if (table != null)
                        consumer.Accept(cancellable, table.Columns, record);
                }
            }
        }

        /// <summary>
        /// Parse Flux CSV response to <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <param name="reader">CSV Data source reader</param>
        /// <param name="cancellationToken">cancellation token</param>
        public IEnumerable<T> ParseCSVResponse<T>(string data)
        {
            Arguments.CheckNotNull(data, nameof(data));
            StringReader sr = new StringReader(data);

            using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
            return csv.GetRecords<T>();
        }

        private class ParseFluxResponseState
        {
            public ParsingState parsingState = ParsingState.Normal;
            public QuestdbResponse table;
            public List<TableColumn> columns;
            public bool startNewTable = true;
            public CsvReader csv;
        }

        private IEnumerable<(QuestdbResponse, List<object>)> ParseNextResponse(ParseFluxResponseState state)
        {
            //
            // Response has HTTP status ok, but response is error.
            //
            if ("error".Equals(state.csv[1]) && "reference".Equals(state.csv[2]))
            {
                state.parsingState = ParsingState.InError;
                yield break;
            }

            //
            // Throw QuestException with error response
            //
            if (ParsingState.InError.Equals(state.parsingState))
            {
                var error = state.csv[1];
                var referenceValue = state.csv[2];

                var reference = 0;

                if (referenceValue != null && !String.IsNullOrEmpty(referenceValue))
                {
                    reference = Convert.ToInt32(referenceValue);
                }

                throw new QuestdbException(error, reference);
            }
            //#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
            // parse column names
            if (state.startNewTable)
            {
                state.table = new QuestdbResponse();
                AddColumnNames(state.table, state.csv);
                state.startNewTable = false;
                yield break;
            }

            yield return (state.table, ParseRecord(state.table, state.csv));
        }

        private List<object> ParseRecord(QuestdbResponse table, CsvReader csv)
        {
            var record = new List<object>();

            foreach (var fluxColumn in table.Columns)
            {
                var columnName = fluxColumn.Name;

                var strValue = csv[fluxColumn.Index + 1];

                record.Add(ToValue(strValue, fluxColumn));
            }

            return record;
        }

        private Object ToValue(string strValue, TableColumn column)
        {
            Arguments.CheckNotNull(column, "column");

            try
            {
                switch (column.Type)
                {
                    case "boolean":
                        return bool.TryParse(strValue, out var value) && value;
                    case "unsignedLong":
                        return Convert.ToUInt64(strValue);
                    case "long":
                        return Convert.ToInt64(strValue);
                    case "double":
                        return Convert.ToDouble(strValue, CultureInfo.InvariantCulture);
                    case "base64Binary":
                        return Convert.FromBase64String(strValue);
                    case "dateTime:RFC3339":
                    case "dateTime:RFC3339Nano":
                        return InstantPattern.ExtendedIso.Parse(strValue).Value;
                    case "duration":
                        return Duration.FromNanoseconds(Convert.ToDouble(strValue));
                    default:
                        return strValue;
                }
            }
            catch (Exception)
            {
                throw new QuestdbException("Unable to parse CSV response.");
            }
        }

        public static Stream ToStream(string str)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(str);
            writer.Flush();
            stream.Position = 0;
            return new BufferedStream(stream);
        }

        private void AddColumnNames(QuestdbResponse table, CsvReader columnNames)
        {
            Arguments.CheckNotNull(table, "table");
            Arguments.CheckNotNull(columnNames, "columnNames");

            for (var index = 0; index < columnNames.Context.Record.Length; index++)
            {
                var name = columnNames[index];

                if (string.IsNullOrEmpty(name))
                {
                    continue;
                }

                var columnDef = new TableColumn
                {
                    Name = name,
                    Index = index - 1
                };

                table.Columns.Add(columnDef);
            }
        }
    }
}