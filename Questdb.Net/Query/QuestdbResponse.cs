using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Questdb.Net.Query
{
    public class QuestdbResponse
    {
        /// <summary>
        /// Table column's labels and types.
        /// </summary>
        public List<TableColumn> Columns { get; } = new List<TableColumn>();

        /// <summary>
        /// Table records.
        /// </summary>
        public List<List<object>> DataSet { get; } = new List<List<object>>();

        public long Count { get; set; }

        public bool IsSuccessResponse
        {
            get
            {
                if ((Position == null && string.IsNullOrEmpty(Error)) || DDL == "OK")
                    return true;
                return false;
            }
        }

        public string DDL { get; set; } = string.Empty;

        public int? Position { get; set; }

        public string Error { get; set; }

        public string Query { get; set; }

        public Timing Timings { get; set; } = new Timing();


        public override string ToString()
        {
            return new StringBuilder(GetType().Name + "[")
                .Append("columns=" + Columns.Count)
                .Append(", records=" + DataSet.Count)
                .Append("]")
                .ToString();
        }
    }


    public class Timing
    {
        public long Count { get; set; }

        public long Compiler { get; set; }

        public long Execute { get; set; }
    }
}