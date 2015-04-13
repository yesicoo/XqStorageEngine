#region 文件描述
//---------------------------------------------------------------------------------------------
// 文 件 名: MS_SEHelper.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/10 15:59:53
// 描    述：
// 版    本：Version 1.0
//---------------------------------------------------------------------------------------------
// 历史更新纪录
//---------------------------------------------------------------------------------------------
// 版    本：           修改时间：           修改人：           
// 修改内容：
//---------------------------------------------------------------------------------------------
// 本文件内代码如果没有特殊说明均遵守MIT开源协议 http://opensource.org/licenses/mit-license.php
//---------------------------------------------------------------------------------------------
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XqStorageEngine
{
    class MS_SEHelper:ISEHelper
    {

        public string DBConnentString
        {
            set { throw new NotImplementedException(); }
        }

        public string TimeColumnName
        {
            set { throw new NotImplementedException(); }
        }

        public Dictionary<string, SEntity> SEntitys
        {
            set { throw new NotImplementedException(); }
        }

        public string Pref
        {
            set { throw new NotImplementedException(); }
        }

        public string GetTableName(SEntity se, DateTime DataTime)
        {
            throw new NotImplementedException();
        }

        public System.Data.Common.DbDataReader ExecuteReader(string sql)
        {
            throw new NotImplementedException();
        }

        public System.Data.Common.DbDataReader ExecuteReader(string sql, params System.Data.Common.DbParameter[] dbParameters)
        {
            throw new NotImplementedException();
        }

        public System.Threading.Tasks.Task<System.Data.Common.DbDataReader> ExecuteReaderAsync(string sql)
        {
            throw new NotImplementedException();
        }

        public System.Threading.Tasks.Task<System.Data.Common.DbDataReader> ExecuteReaderAsync(string sql, params System.Data.Common.DbParameter[] dbParameters)
        {
            throw new NotImplementedException();
        }

        public object ExecuteScalar(string sql)
        {
            throw new NotImplementedException();
        }

        public object ExecuteScalar(string sql, params System.Data.Common.DbParameter[] dbParameters)
        {
            throw new NotImplementedException();
        }

        public System.Threading.Tasks.Task<object> ExecuteScalarAsync(string sql)
        {
            throw new NotImplementedException();
        }

        public System.Threading.Tasks.Task<object> ExecuteScalarAsync(string sql, params System.Data.Common.DbParameter[] dbParameters)
        {
            throw new NotImplementedException();
        }

        public int ExecuteCount(string TestType, DateTime TestTime, string Query = "")
        {
            throw new NotImplementedException();
        }

        public int ExecuteCount(string TableName, string Query = "")
        {
            throw new NotImplementedException();
        }

        public int ExecuteCount(string TableName, string Query, DateTime StartTime, DateTime EndTime)
        {
            throw new NotImplementedException();
        }

        public string DataTable2InsterSQL(System.Data.DataTable dt)
        {
            throw new NotImplementedException();
        }

        public string Entity2InsterSQL<T>(string tableName, List<T> ts)
        {
            throw new NotImplementedException();
        }

        public string Entity2InsterSQL<T>(string tableName, T t)
        {
            throw new NotImplementedException();
        }
    }
}
