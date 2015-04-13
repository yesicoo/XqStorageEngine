using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
namespace XqStorageEngine
{
    interface ISEHelper
    {
        #region Fields
        string DBConnentString { set; }
        string TimeColumnName { set; }
        Dictionary<string, SEntity> SEntitys { set; }
        string Pref { set; }

        string GetTableName(SEntity se, DateTime DataTime); 
        #endregion

        #region ExecuteReader
        DbDataReader ExecuteReader(string sql);
        DbDataReader ExecuteReader(string sql, params DbParameter[] dbParameters);
        Task<DbDataReader> ExecuteReaderAsync(string sql);
        Task<DbDataReader> ExecuteReaderAsync(string sql, params DbParameter[] dbParameters);
        #endregion

        #region ExecuteScalar
        object ExecuteScalar(string sql);
        object ExecuteScalar(string sql, params DbParameter[] dbParameters);
        Task<object> ExecuteScalarAsync(string sql);
        Task<object> ExecuteScalarAsync(string sql, params DbParameter[] dbParameters);
        #endregion

        #region ExecuteCount
        int ExecuteCount(string TestType, DateTime TestTime, string Query = "");
        int ExecuteCount(string TableName, string Query = "");
        int ExecuteCount(string TableName, string Query, DateTime StartTime, DateTime EndTime);
        #endregion

        #region 2InsterSQL
        string DataTable2InsterSQL(DataTable dt);
        string Entity2InsterSQL<T>(string tableName, System.Collections.Generic.List<T> ts);
        string Entity2InsterSQL<T>(string tableName, T t);
        #endregion



    }
}
