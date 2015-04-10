using System;
using System.Data;
namespace XqStorageEngine
{
    interface ISEHelper
    {
        string DataTable2InsterSQL(DataTable dt);
        string Entity2InsterSQL<T>(string tableName, System.Collections.Generic.List<T> ts);
        string Entity2InsterSQL<T>(string tableName, T t);
    }
}
