using System;

namespace Questdb.Net
{
    /// <summary>
    /// The annotation is used for mapping POCO class into line protocol.
    /// </summary>
    public sealed class Table : Attribute
    {
        public string Name { get; }

        public Table(string name)
        {
            Name = name;
        }
    }

    /// <summary>
    /// The annotation is used to customize bidirectional mapping between POCO and Flux query result or Line Protocol.
    /// </summary>
    public sealed class Column : Attribute
    {
        public string Name { get; }

        public bool IsTag { get; set; }

        public bool IsTimestamp { get; set; }

        public Column()
        {
        }

        public Column(string name)
        {
            Name = name;
        }
    }
}