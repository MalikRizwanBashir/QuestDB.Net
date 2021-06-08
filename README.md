<div align="center">
  <img alt="QuestDB Logo" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/logo-readme.png" width="305px"/>
</div>
<p>&nbsp;</p>

[![Nuget](https://img.shields.io/nuget/v/Questdb.Net)](https://www.nuget.org/packages/Questdb.Net/)

# QuestDB

QuestDB is a high-performance, open-source SQL database for applications in
financial services, IoT, machine learning, DevOps and observability. It includes
endpoints for PostgreSQL wire protocol, high-throughput schema-agnostic
ingestion using InfluxDB Line Protocol, and a REST API for queries, bulk
imports, and exports.

# QuestDB.Net (C#)
QuestDB.Net is a lightweight client library written in C# for QuestDB.
It currently supports insertion through influxdb lineprotocol and Query using rest api.

You can insert influx points or any measurement(Table) model.

You can Query raw response or QuestDbResponseModel or directly to measurement(Table) model.

#Examples:

Write using point:
```csharp
QuestDBClient client = new QuestDBClient("http://127.0.0.1");

var writeApi = client.GetWriteApi();
var point = PointData.Measurement("trades")
                    .Tag("name", "tagevalue")
                    .Tag("name2", "secondtagvalue")
                    .Field("value", 123)
		    .Timestamp(DateTimeOffset.Now, WritePrecision.Nanoseconds);
writeApi.WritePoint(point);
```
Write model:
```csharp
    [Table("table_name")]
    public class Calculations
    {
        [Column(IsTimestamp = true)]
        public DateTimeOffset TimeSatmp { get; set; }
        [Column(IsTag = true)]
        public string Name { get; set; }
        public int Value { get; set; }
    }
    
writeApi.WriteMeasurement<Calculations>(TObject);
```

Query Data:
```csharp
var queryApi = client.GetQueryApi();
var questDbResponseModel = queryApi.Query("select * from table");
var dataModel = queryApi.Query<T>("select * from table");
var rawResponse = queryApi.QueryRaw("select * from table");
var csvStringResponse = queryApi.QueryCSV("select * from table");
```

You can also stream the response by using callback
```csharp
QueryAsync<T>(string query, Action<ICancellable, T> onNext);
```

## Contribute

We are always happy to have contributions to the project whether it is source
code, documentation, bug reports, feature requests or feedback.

This project follows the
[all-contributors](https://github.com/all-contributors/all-contributors)
specification. Contributions of any kind welcome!
