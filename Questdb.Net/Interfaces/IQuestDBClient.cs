using Questdb.Net.Write;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net
{
    public interface IQuestDBClient
    {
        IQueryApi GetQueryApi();
        IWriteLineApi GetWriteApi(WriteOptions writeOptions);
        IWriteLineApi GetWriteApi();

        QuestDBClient EnableGzip();
        QuestDBClient DisableGzip();
        bool IsGzipEnabled();
    }
}
