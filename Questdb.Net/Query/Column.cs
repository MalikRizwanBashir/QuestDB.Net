using System.Text;

namespace Questdb.Net.Query
{
    /// <summary>
    /// This class represents column header specification of <see cref="QuestResponse"/>.
    /// </summary>
    public class TableColumn
    {
        /// <summary>
        /// Column index in record.
        /// </summary>
        public int Index { get; set; }

        /// <summary>
        /// The label of column (e.g., "_start", "_stop", "_time").
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The data type of column (e.g., "string", "long", "dateTime:RFC3339").
        /// </summary>
        public string Type { get; set; }

        public override string ToString()
        {
            return new StringBuilder(GetType().Name + "[")
                .Append("index=" + Index)
                .Append(", label='" + Name + "'")
                .Append(", dataType='" + Type + "'")
                .Append("]").ToString();
        }
    }
}