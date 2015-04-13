#region 文件描述
//---------------------------------------------------------------------------------------------
// 文 件 名: _SEHelper.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/10 14:16:35
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
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace XqStorageEngine
{
    public class MySQL_SEHelper :ISEHelper
    {
        string _Pref = null;
        string _TimeColumnName = null;
        Dictionary<string, SEntity> _SEntitys = null;
        string dbStr = string.Empty;
        public string DBConnentString
        {
            set { dbStr = value; }
        }
        public Dictionary<string, SEntity> SEntitys
        {
            set{ _SEntitys = value; }
        }

        public string Pref
        {
            set{ _Pref = value;}
        }
        public string TimeColumnName
        {
            set { _TimeColumnName = value; }
        }


        #region 读取数据

        #region ExecuteReader
        public DbDataReader ExecuteReader(string sql)
        {
            try
            {
                if (string.IsNullOrEmpty(dbStr)) { SELog.SendLog("DBConnentString is Null !"); return null; }
                return MySqlHelper.ExecuteReader(dbStr, sql);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public DbDataReader ExecuteReader(string sql,params DbParameter[] dbParameters)
        {
            try
            {
                if (string.IsNullOrEmpty(dbStr)) { SELog.SendLog("DBConnentString is Null !"); return null; }
                return MySqlHelper.ExecuteReader(dbStr, sql, (MySqlParameter[])dbParameters);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public Task<DbDataReader> ExecuteReaderAsync(string sql)
        {
            if (string.IsNullOrEmpty(dbStr)) { SELog.SendLog("DBConnentString is Null !"); return null; }
            TaskCompletionSource<DbDataReader> source = new TaskCompletionSource<DbDataReader>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    DbDataReader result = ExecuteReader(sql);
                    source.SetResult(result);
                }
                catch (Exception exception)
                {
                    source.SetException(exception);
                }
            }
            else
            {
                source.SetCanceled();
            }
            return source.Task;

        }
        public Task<DbDataReader> ExecuteReaderAsync(string sql, params DbParameter[] dbParameters) {
            if (string.IsNullOrEmpty(dbStr)) { SELog.SendLog("DBConnentString is Null !"); return null; }
            TaskCompletionSource<DbDataReader> source = new TaskCompletionSource<DbDataReader>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    DbDataReader result = ExecuteReader(sql, dbParameters);
                    source.SetResult(result);
                }
                catch (Exception exception)
                {
                    source.SetException(exception);
                }
            }
            else
            {
                source.SetCanceled();
            }
            return source.Task;
        
        }
        #endregion

        #region ExecuteScalar
        public object ExecuteScalar(string sql)
        {
            try
            {
                return MySqlHelper.ExecuteScalar(dbStr, sql);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public object ExecuteScalar(string sql,params DbParameter[] dbParameters)
        {
            try
            {
                return MySqlHelper.ExecuteScalar(dbStr, sql,(MySqlParameter[])dbParameters);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public Task<object> ExecuteScalarAsync(string sql)
        {
            return MySqlHelper.ExecuteScalarAsync(dbStr, sql);
        }
        public Task<object> ExecuteScalarAsync(string sql, params DbParameter[] dbParameters)
        {
            return MySqlHelper.ExecuteScalarAsync(dbStr, sql,(MySqlParameter[])dbParameters);
        }
        #endregion

        #region ExecuteCount
        public int ExecuteCount(string TestType, DateTime TestTime, string Query = "")
        {
            SEntity se = null;
            if (!_SEntitys.TryGetValue(TestType, out se))
            {
                SELog.SendLog("[" + TestType + "]测试类型未配置");
                return -1;
            }
            // eq: limit
            Query = Query.Contains("=") ? " and " + Query : Query;

            string SelectTable = GetTableName(se, TestTime);
            return int.Parse(ExecuteScalar(GetSQL("Count(*) as TCount", Query, SelectTable, "")).ToString());
        }
        public int ExecuteCount(string TableName, string Query = "")
        {
            // eq: limit
            Query = Query.Contains("=") ? " and " + Query : Query;
            return int.Parse(ExecuteScalar(GetSQL("Count(*) as TCount", Query, TableName, "")).ToString());
        }
        public int ExecuteCount(string TableName, string Query, DateTime StartTime, DateTime EndTime)
        {
            return int.Parse(ExecuteScalar(GetSQL(StartTime, EndTime, "Count(*) as TCount", Query, TableName, "")).ToString());
        }
        #endregion

        #endregion


        #region 保存数据

        #endregion

        #region 数据转SQL
        /// <summary>
        /// DataTable转SQL
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public string DataTable2InsterSQL(DataTable dt)
        {
            if (dt == null) { SELog.SendLog("[DataTable2InsterSQL] dt Is Null !"); return string.Empty; }
            if (string.IsNullOrEmpty(dt.TableName)) { SELog.SendLog("[DataTable2InsterSQL] dt.TableName Is Null or Empty !"); return string.Empty; }
            if (dt.Rows.Count == 0) { SELog.SendLog("[DataTable2InsterSQL] dt.Rows.Count Is 0 !"); return string.Empty; }
            string TableName = dt.TableName;
            List<string> ColumnNames = new List<string>();
            StringBuilder sb_Columns = new StringBuilder();
            StringBuilder sb_Values = new StringBuilder();
            #region 遍历列名
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string ColumnName = dt.Columns[i].ColumnName;
                sb_Columns.Append("`").Append(ColumnName).Append("`,");
                ColumnNames.Add(ColumnName);
            }
            #endregion

            #region 遍历结果
            foreach (DataRow dr in dt.Rows)
            {
                StringBuilder stemp = new StringBuilder();
                if (sb_Values.Length == 0)
                    stemp.Append("(");
                else
                    stemp.Append(",(");

                ColumnNames.ForEach(ColumnName =>
                {
                    if (ColumnName.ToLower() != "id")
                    {
                        stemp.Append("'").Append(dr[ColumnName]).Append("',");
                    }
                });
                sb_Values.Append(stemp.ToString().TrimEnd(',')).Append(")");
            }
            #endregion
            DateTime dte = Convert.ToDateTime(dt.Rows[0]["TestTime"]);
            string InsertSql = string.Format("insert into `{0}` ({1}) values {2}", TableName, sb_Columns.ToString().TrimEnd(','), sb_Values.ToString());
            return InsertSql;
        }

        public string Entity2InsterSQL<T>(string tableName, T t)
        {
            if (t == null) { SELog.SendLog("[Entity2InsterSQL] t Is Null !"); return string.Empty; }
            StringBuilder sb_Columns = new StringBuilder();
            StringBuilder sb_Values = new StringBuilder();
            foreach (PropertyInfo pInfo in t.GetType().GetProperties())
            {
                if (pInfo.Name.ToLower() != "id")
                {
                    sb_Columns.Append("`").Append(pInfo.Name).Append("`,");
                    sb_Values.Append("'").Append(pInfo.GetConstantValue().ToString()).Append("',");
                }
            }
            string InsertSql = string.Format("insert into `{0}` ({1}) values ({2});", tableName, sb_Columns.ToString().TrimEnd(','), sb_Values.ToString().TrimEnd(','));
            return InsertSql;
        }

        public string Entity2InsterSQL<T>(string tableName, List<T> ts)
        {
            if (ts == null) { SELog.SendLog("[Entity2InsterSQL] t Is Null !"); return string.Empty; }
            if (ts.Count == 0) { SELog.SendLog("[Entity2InsterSQL] ts.Count  Is 0 !"); return string.Empty; }
            StringBuilder sb_Columns = new StringBuilder();
            StringBuilder sb_Values = new StringBuilder();
            ts.First().GetType().GetProperties().Select(x => x.Name).ToList().ForEach(Name => sb_Columns.Append("`").Append(Name).Append("`,"));
            ts.ForEach(t => { sb_Values.Append(sb_Values.Length == 0 ? "(" : ",("); t.GetType().GetProperties().Select(x => x.GetValue(t, null).ToString()).ToList().ForEach(value => { sb_Values.Append("'").Append(value).Append("',"); }); sb_Values.Remove(sb_Values.Length - 1, 1).Append(")"); });
            string InsertSql = string.Format("insert into `{0}` ({1}) values {2};", tableName, sb_Columns.ToString().TrimEnd(','), sb_Values.ToString());
            return InsertSql;
        }
        
        #endregion

        public  string GetTableName( SEntity se, DateTime DataTime)
        {
            if (se.IsSplit)
                return string.Format("{0}_{1}_{2}", _Pref, se.TableName, DataTime.ToString("yyyy-MM-dd"));
            else
                return se.TableName;
        }

        #region 获取可执行SQL语句
        private static string GetSQL(string Columns, string Query, string SelectTable, string OrderLimit)
        {
            return string.Format("select {1} from `{0}` where 1=1 {2} {3}", SelectTable, Columns, Query, OrderLimit);
        }
        private static string GetSQL(DateTime StartTime, DateTime EndTime, string Columns, string Query, string SelectTable, string OrderLimit)
        {
            return string.Format("select {1} from `{0}` where `{6}`>='{2}' and `{6}`<'{3}' {4} {5}", SelectTable, Columns, StartTime, EndTime, Query, OrderLimit, _TimeColumnName);
        }
        #endregion

    }
}
