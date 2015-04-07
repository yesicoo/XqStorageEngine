#region 文件描述
//---------------------------------------------------------------------------------------------
// 文 件 名: SEHelper.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/3 9:54:09
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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace XqStorageEngine
{
    public class SEHelper
    {
        public string DataSqlComm = string.Empty;
        public string ConfigComm = string.Empty;
        public Dictionary<string, SEntity> SEntitys = new Dictionary<string, SEntity>();

        #region 构造函数
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="sqlComm">连接字符串</param>
        public SEHelper(string dataSqlComm, string configComm = "")
        {
            DataSqlComm = dataSqlComm;
            if (configComm == "")
            {
                ConfigComm = dataSqlComm;
            }
            else
            {
                ConfigComm = configComm;
            }
            LoadConfig();
            SELog.SendLog("存储引擎初始化完毕!");
        }

        #endregion

        public void RegisterItem(string testType, long maxID, string tableName, string createSQl, Action<string, long> updateID) { 
        
        
        
        
        }



        #region 载入配置
        /// <summary>
        /// 载入配置
        /// </summary>
        private void LoadConfig()
        {
            List<dynamic> splitConfigs = ExecuteDynamicEntities("select * from storageengine_config", ConfigComm);
            SEntitys.Clear();
            splitConfigs.ForEach((sc) =>
            {
                SEntity se = new SEntity();
                se._MaxID = sc.MaxID;
                se.CreateSQL = sc.CreateSQL;
                se.TableName = sc.TableName;
                se.TestType = sc.TestType;
                se.IsSplit = !string.IsNullOrEmpty(se._CreateSQL);
                SEntitys.Add(se.TestType, se);
            });
        }
        /// <summary>
        /// 重新载入
        /// </summary>
        public void ReLoadConfig()
        {
            LoadConfig();
        }
        #endregion

        #region 数据库操作
        #region ExecuteNonQuery
        /// <summary>
        /// 执行SQL
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public int ExecuteNonQuery(string sql)
        {
            return MySqlHelper.ExecuteNonQuery(DataSqlComm, sql);
        }

        public int ExecuteNonQuery(string sql, string sqlComm)
        {
            return MySqlHelper.ExecuteNonQuery(sqlComm, sql);
        }
        /// <summary>
        /// 执行SQL
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public Task<int> ExecuteNonQueryAsync(string sql)
        {
            return MySqlHelper.ExecuteNonQueryAsync(DataSqlComm, sql);
        }
        public Task<int> ExecuteNonQueryAsync(string sql, string sqlComm)
        {
            return MySqlHelper.ExecuteNonQueryAsync(sqlComm, sql);
        }
        #endregion
        #endregion

        #region 获取最大测试类型分配的ID
        /// <summary>
        /// 获取最大测试类型分配的ID
        /// </summary>
        /// <param name="TestType"></param>
        /// <returns></returns>
        public long GetTypeMaxID(string TestType)
        {
            SEntity se = null;
            if (SEntitys.TryGetValue(TestType, out se))
            {
                return se._MaxID;
            }
            else
            {
                return -1;
            }

        }
        #endregion

        #region 获取测试类型实体
        /// <summary>
        /// 获取测试类型实体
        /// </summary>
        /// <param name="TestType"></param>
        /// <returns></returns>
        public SEntity GetSEntity(string TestType)
        {
            SEntity se = null;
            if (SEntitys.TryGetValue(TestType, out se))
            {
                return se;
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region 保存数据
        /// <summary>
        /// 保存数据
        /// </summary>
        /// <param name="datatable"></param>
        public int SaveData(DataTable datatable)
        {
            string sql = DataTable2InsterSQL(datatable);
            int result = 0;
            try
            {
                result = ExecuteNonQuery(sql);
            }
            catch (Exception e)
            {
                SELog.SendLog(e.Message);
                SELog.SendLog(sql);
            }
            return result;

        }
        /// <summary>
        /// 保存数据
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="datatable"></param>
        public int SaveData(string TestType, DataTable datatable, bool IsContainsID = false)
        {

            int result = 0;
            SEntity se = null;
            if (SEntitys.TryGetValue(TestType, out se))
            {
                string sql = DataTable2InsterSQL(datatable, se, IsContainsID);
                try
                {
                    result = ExecuteNonQuery(sql);
                }
                catch (Exception e)
                {
                    SELog.SendLog(e.Message);
                    // Log.SendLog(sql);
                }
                if (result < 1)
                {
                    ExecuteNonQuery(se.CreateSQL);
                    SELog.SendLog(se.LastTableName + "表 创建成功");
                    result = ExecuteNonQuery(sql);
                }
            }
            else
            {
                SELog.SendLog("TestType:" + TestType);
                result = SaveData(datatable);
            }
            return result;

        }

        public int SaveData(SEntity se, DataTable datatable, bool IsContainsID = false)
        {

            int result = 0;
            string sql = DataTable2InsterSQL(datatable, se, IsContainsID);
            try
            {
                result = ExecuteNonQuery(sql);
            }
            catch (Exception e)
            {
                SELog.SendLog(e.Message);
                //Log.SendLog(sql);
            }
            if (result < 1)
            {
                ExecuteNonQuery(se.CreateSQL);
                SELog.SendLog(se.LastTableName + "表 创建成功");
                result = ExecuteNonQuery(sql);
            }
            return result;

        }
        /// <summary>
        /// 保存数据
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="dataTime">数据时间</param>
        /// <param name="sql">SQL语句 表名{0}替换</param>
        /// <returns></returns>
        public int SaveData(string TestType, DateTime dataTime, string sql)
        {
            int result = 0;
            SEntity se = null;
            if (SEntitys.TryGetValue(TestType, out se))
            {
                se.LastTableName = GetTableName(se, dataTime);
                try
                {
                    sql = string.Format(sql, se.LastTableName);
                    result = ExecuteNonQuery(sql);
                }
                catch (Exception e)
                {
                    SELog.SendLog(e.Message);
                    // Log.SendLog(sql);
                }
                if (result < 1)
                {
                    ExecuteNonQuery(se.CreateSQL);
                    SELog.SendLog(se.LastTableName + "表 创建成功");
                    ExecuteNonQuery(sql);
                }
            }
            return result;
        }
        /// <summary>
        /// 异步保存数据
        /// </summary>
        /// <param name="datatable"></param>
        public Task<int> SaveDataAsync(DataTable datatable)
        {
            TaskCompletionSource<int> source = new TaskCompletionSource<int>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    int result = SaveData(datatable);
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
        /// <summary>
        /// 异步保存数据
        /// </summary>
        /// <param name="TestType"></param>
        /// <param name="datatable"></param>
        public Task<int> SaveDataAsync(string TestType, DataTable datatable, bool IsContainsID = false)
        {

            TaskCompletionSource<int> source = new TaskCompletionSource<int>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    int result = SaveData(TestType, datatable, IsContainsID);
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
        /// <summary>
        /// 异步保存数据
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="dataTime">数据时间</param>
        /// <param name="sql">SQL语句 表名{0}替换</param>
        /// <returns></returns>
        public Task<int> SaveDataAsync(string TestType, DateTime dataTime, string sql)
        {
            TaskCompletionSource<int> source = new TaskCompletionSource<int>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    int result = SaveData(TestType, dataTime, sql);
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

        #region 读取数据
        #region ExecuteReader
        public MySqlDataReader ExecuteReader(string sql)
        {
            try
            {
                return MySqlHelper.ExecuteReader(DataSqlComm, sql);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }

        public Task<MySqlDataReader> ExecuteReaderAsync(string sql)
        {
            return MySqlHelper.ExecuteReaderAsync(DataSqlComm, sql);
        }
        #endregion

        #region ExecuteScalar
        public object ExecuteScalar(string sql)
        {
            try
            {
                return MySqlHelper.ExecuteScalar(DataSqlComm, sql);
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
            return MySqlHelper.ExecuteScalarAsync(DataSqlComm, sql);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public object ExecuteScalar(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            string QuerySQL = GetQuerySQL(TestType, TestTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return null;
            else
                return ExecuteScalar(QuerySQL);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public Task<object> ExecuteScalarAsync(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<object> source = new TaskCompletionSource<object>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    object result = ExecuteScalar(TestType, TestTime, Columns, Query, OrderLimit);
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

        #region ExecuteCount
        /// <summary>
        /// 查询个数
        /// </summary>
        /// <param name="TestType"></param>
        /// <param name="StartTime"></param>
        /// <param name="EndTime"></param>
        /// <param name="Query"></param>
        /// <returns></returns>
        public int ExecuteCount(string TestType, DateTime StartTime, DateTime EndTime, string Query = "")
        {

            if (EndTime < StartTime)
            {
                SELog.SendLog("起始时间不得大于结束时间(ExecuteDataTable)!");
                return 0;
            }
            else
            {
                SEntity se = null;
                if (!SEntitys.TryGetValue(TestType, out se))
                {
                    SELog.SendLog("[" + TestType + "]测试类型未配置");
                    return 0;
                }
                // eq: limit
                Query = Query.Contains("=") ? " and " + Query : Query;
                if (se.IsSplit)
                {
                    int count = 0;
                    string SelectEndTable = GetTableName(se, EndTime);
                    DateTime SelectTime = StartTime;
                    while (true)
                    {
                        string SelectTable = GetTableName(se, SelectTime);
                        if (IsExistTable(SelectTable))
                        {
                            int result = int.Parse(ExecuteScalar(GetSQL(StartTime, EndTime, "Count(*) as TCount", Query, SelectTable, "")).ToString());
                            count = count + result;
                        }
                        if (SelectTable == SelectEndTable)
                        {
                            break;
                        }
                        else
                        {
                            SelectTime = SelectTime.AddDays(1);
                        }
                    }
                    return count;
                }
                else
                {
                    string SelectTable = se.TableName;
                    return int.Parse(ExecuteScalar(GetSQL(StartTime, EndTime, "Count(*) as TCount", Query, SelectTable, "")).ToString());
                }
            }
        }


        public int ExecuteCount(string TestType, DateTime TestTime, string Query = "")
        {
            SEntity se = null;
            if (!SEntitys.TryGetValue(TestType, out se))
            {
                SELog.SendLog("[" + TestType + "]测试类型未配置");
                return 0;
            }
            // eq: limit
            Query = Query.Contains("=") ? " and " + Query : Query;

            string SelectTable = se.TableName;
            return int.Parse(ExecuteScalar(GetSQL("Count(*) as TCount", Query, SelectTable, "")).ToString());
        }
        public int ExecuteCount(string TableName, string Query = "")
        {
            return int.Parse(ExecuteScalar(GetSQL("Count(*) as TCount", Query, TableName, "")).ToString());
        }
        public int ExecuteCount(string TableName, string Query, DateTime StartTime, DateTime EndTime)
        {
            return int.Parse(ExecuteScalar(GetSQL(StartTime, EndTime, "Count(*) as TCount", Query, TableName, "")).ToString());
        }
        #endregion

        #region ExecuteDataRow
        /// <summary>
        /// 查询一行数据 返回DataRow
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataRow ExecuteDataRow(string sql)
        {
            try
            {
                return MySqlHelper.ExecuteDataRow(DataSqlComm, sql);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public Task<DataRow> ExecuteDataRowAsync(string sql)
        {
            return MySqlHelper.ExecuteDataRowAsync(DataSqlComm, sql);
        }
        /// <summary>
        /// 查询一行分表数据 返回DataRow
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public DataRow ExecuteDataRow(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {

            string QuerySQL = GetQuerySQL(TestType, TestTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return null;
            else
                return ExecuteDataRow(QuerySQL);
        }
        /// <summary>
        ///  异步查询一行分表数据 返回DataRow
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public Task<DataRow> ExecuteDataRowAsync(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<DataRow> source = new TaskCompletionSource<DataRow>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    DataRow dataRow = ExecuteDataRow(TestType, TestTime, Columns, Query, OrderLimit);
                    source.SetResult(dataRow);
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

        #region ExecuteDataTable
        public DataTable ExecuteDataTable(string sql)
        {
            try
            {
                DataSet ds = MySqlHelper.ExecuteDataset(DataSqlComm, sql);
                if (ds == null) { return null; }
                if (ds.Tables.Count == 0) { return null; }
                return ds.Tables[0];
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }
        public Task<DataTable> ExecuteDataTableAsync(string sql)
        {
            TaskCompletionSource<DataTable> source = new TaskCompletionSource<DataTable>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    DataSet ds = MySqlHelper.ExecuteDataset(DataSqlComm, sql);
                    if (ds == null) { source.SetResult(null); }
                    if (ds.Tables.Count == 0) { source.SetResult(null); }
                    source.SetResult(ds.Tables[0]);
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
        /// <summary>
        /// 查询分表数据 返回DataTable
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>DataTable</returns>
        public DataTable ExecuteDataTable(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            string QuerySQL = GetQuerySQL(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return null;
            else
                return ExecuteDataTable(QuerySQL);
        }

        /// <summary>
        /// 异步查询分表数据 返回DataTable
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>DataTable</returns>
        public Task<DataTable> ExecuteDataTableAsync(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<DataTable> source = new TaskCompletionSource<DataTable>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    DataTable table = ExecuteDataTable(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
                    source.SetResult(table);
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

        /// <summary>
        /// Order By TestTime Desc 专用
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Columns">查询列名</param>
        /// <param name="Query">查询条件</param>
        /// <param name="limitStart">截断起始位置</param>
        /// <param name="LimitLength">截断长度</param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string TestType, DateTime StartTime, DateTime EndTime, string Columns, string Query, int PageCount, int LimitLength)
        {
            if (EndTime < StartTime)
            {
                SELog.SendLog("起始时间不得大于结束时间!");
                return null;
            }
            else
            {
                SEntity se = null;
                if (!SEntitys.TryGetValue(TestType, out se))
                {
                    SELog.SendLog("[" + TestType + "]测试类型未配置");
                    return null;
                }

                // eq: limit
                Query = Query.Contains("=") ? " and " + Query : Query;
                // 查询起始位数
                int limitStart = (PageCount - 1) * LimitLength;

                if (se.IsSplit)
                {
                    string StartTable = GetTableName(se, StartTime);

                    DateTime SelectTime = EndTime;
                    int LostCount = 0;
                    while (true)
                    {
                        string SelectTable = GetTableName(se, SelectTime);
                        if (IsExistTable(SelectTable))
                        {
                            //表内数据个数
                            int tableCount = ExecuteCount(SelectTable, Query, StartTime, EndTime);

                            //有效数据个数
                            int SurplusCount = (LostCount + tableCount) - limitStart;
                            if (SurplusCount >= LimitLength)  //有效数据大于或等于查询数据个数
                            {
                                string QuerySQL = GetSQL(StartTime, EndTime, Columns, Query, SelectTable, string.Format(" Order By TestTime Desc limit {0},{1}", limitStart - LostCount, LimitLength));
                                return ExecuteDataTable(QuerySQL);
                            }
                            else if (SurplusCount < LimitLength && SurplusCount > 0)  //有效数据小于查询数据个数 并且大于0个
                            {
                                string QuerySQL = GetSQL(StartTime, EndTime, Columns, Query, SelectTable, string.Format(" Order By TestTime Desc limit {0},{1}", limitStart - LostCount, LimitLength));
                                DataTable ResultData = ExecuteDataTable(QuerySQL);
                                if (StartTable == SelectTable)
                                {
                                    return ResultData;
                                }
                                while (true)
                                {
                                    SelectTime = SelectTime.AddDays(-1);
                                    SelectTable = GetTableName(se, SelectTime);
                                    QuerySQL = GetSQL(StartTime, EndTime, Columns, Query, SelectTable, string.Format(" Order By TestTime Desc limit {0},{1}", 0, LimitLength - SurplusCount));
                                    ResultData.Merge(ExecuteDataTable(QuerySQL));
                                    if (StartTable == SelectTable)
                                    {
                                        return ResultData;
                                    }
                                }
                            }
                            LostCount += tableCount;
                        }

                        if (StartTable == SelectTable)
                            return null;
                        else
                            SelectTime = SelectTime.AddDays(-1);
                    }
                }
                else
                {
                    string SelectTable = se.TableName;
                    return ExecuteDataTable(GetSQL(StartTime, EndTime, Columns, Query, SelectTable, string.Format(" Order By TestTime Desc limit {0},{1}", limitStart, LimitLength)));
                }
            }
        }
        #endregion

        #region ExecuteDataset
        public DataSet ExecuteDataset(string sql)
        {
            try
            {
                return MySqlHelper.ExecuteDataset(DataSqlComm, sql);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
                return null;
            }
        }

        public Task<DataSet> ExecuteDatasetAsync(string sql)
        {
            return MySqlHelper.ExecuteDatasetAsync(DataSqlComm, sql);
        }
        #endregion

        #region UpdateDataSet
        public void UpdateDataSet(string sql, DataSet ds, string tableName)
        {
            try
            {
                MySqlHelper.UpdateDataSet(DataSqlComm, sql, ds, tableName);
            }
            catch (Exception ex)
            {
                SELog.SendLog(sql);
                SELog.SendLog(ex.Message);
            }
        }
        public Task ExecuteScalarAsync(string sql, DataSet ds, string tableName)
        {
            return MySqlHelper.UpdateDataSetAsync(DataSqlComm, sql, ds, tableName);
        }
        #endregion

        #region ExecuteEntity
        public T ExecuteEntity<T>(string query)
        {
            MySqlConnection conn = new MySqlConnection(DataSqlComm);
            conn.Open();
            IEnumerable<T> result = null;
            using (conn)
            {
                result = conn.Query<T>(query, null);
            }
            if (result.Count() > 0)
                return result.First();
            else
                return default(T);
        }
        public Task<T> ExecuteEntityAsync<T>(string query)
        {
            TaskCompletionSource<T> source = new TaskCompletionSource<T>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    T result = ExecuteEntity<T>(query);
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
        /// <summary>
        /// 查询单条分表数据 返回实体类
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>T</returns>
        public T ExecuteEntity<T>(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {

            string QuerySQL = GetQuerySQL(TestType, TestTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return default(T);
            else
                return ExecuteEntity<T>(QuerySQL);
        }
        /// <summary>
        /// 异步查询单条分表数据 返回实体类
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>T</returns>
        public Task<T> ExecuteSplitEntityAsync<T>(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<T> source = new TaskCompletionSource<T>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    T result = ExecuteEntity<T>(TestType, TestTime, Columns, Query, OrderLimit);
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

        #region ExecuteEntities
        public List<T> ExecuteEntities<T>(string query)
        {
            MySqlConnection conn = new MySqlConnection(DataSqlComm);
            conn.Open();
            IEnumerable<T> result = null;
            using (conn)
            {
                result = conn.Query<T>(query, null);
            }
            if (result.Count() > 0)
                return result.ToList();
            else
                return new List<T>();
        }
        public Task<List<T>> ExecuteEntitiesAsync<T>(string query)
        {
            TaskCompletionSource<List<T>> source = new TaskCompletionSource<List<T>>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    List<T> result = ExecuteEntities<T>(query);
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

        /// <summary>
        /// 批量查询分表数据  返回实体集合
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public List<T> ExecuteEntities<T>(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {

            string QuerySQL = GetQuerySQL(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return new List<T>();
            else
                return ExecuteEntities<T>(QuerySQL);
        }
        /// <summary>
        /// 异步批量查询分表数据  返回实体集合
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public Task<List<T>> ExecuteEntitiesAsync<T>(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<List<T>> source = new TaskCompletionSource<List<T>>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    List<T> result = ExecuteEntities<T>(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
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

        #region ExecuteDynamicEntity
        public dynamic ExecuteDynamicEntity(string query)
        {
            MySqlConnection conn = new MySqlConnection(DataSqlComm);
            conn.Open();
            IEnumerable<dynamic> result = null;
            using (conn)
            {
                result = conn.Query<dynamic>(query, null);
            }
            if (result.Count() > 0)
                return result.First();
            else
                return null;
        }
        public Task<dynamic> ExecuteDynamicEntityAsync(string query)
        {
            TaskCompletionSource<dynamic> source = new TaskCompletionSource<dynamic>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    dynamic result = ExecuteDynamicEntity(query);
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
        /// <summary>
        /// 查询单条分表数据 返回动态类型
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>dynamic</returns>
        public dynamic ExecuteDynamicEntity(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {

            string QuerySQL = GetQuerySQL(TestType, TestTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return null;
            else
                return ExecuteDynamicEntity(QuerySQL);
        }
        /// <summary>
        /// 异步查询单条分表数据 返回动态类型
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="TestType">测试类型</param>
        /// <param name="TestTime">测试时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns>dynamic</returns>
        public Task<dynamic> ExecuteDynamicEntityAsync(string TestType, DateTime TestTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<dynamic> source = new TaskCompletionSource<dynamic>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    dynamic result = ExecuteDynamicEntity(TestType, TestTime, Columns, Query, OrderLimit);
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

        #region ExecuteDynamicEntities
        public List<dynamic> ExecuteDynamicEntities(string query)
        {
            MySqlConnection conn = new MySqlConnection(DataSqlComm);
            conn.Open();
            IEnumerable<dynamic> result = null;
            using (conn)
            {
                result = conn.Query<dynamic>(query, null);
            }
            if (result.Count() > 0)
                return result.ToList();
            else
                return new List<dynamic>();
        }

        public List<dynamic> ExecuteDynamicEntities(string query, string sqlComm)
        {
            MySqlConnection conn = new MySqlConnection(sqlComm);
            conn.Open();
            IEnumerable<dynamic> result = null;
            using (conn)
            {
                result = conn.Query<dynamic>(query, null);
            }
            if (result.Count() > 0)
                return result.ToList();
            else
                return new List<dynamic>();
        }

        public Task<List<dynamic>> ExecuteDynamicEntitiesAsync(string query)
        {
            TaskCompletionSource<List<dynamic>> source = new TaskCompletionSource<List<dynamic>>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    dynamic result = ExecuteDynamicEntities(query);
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

        public Task<List<dynamic>> ExecuteDynamicEntitiesAsync(string query, string sqlComm)
        {
            TaskCompletionSource<List<dynamic>> source = new TaskCompletionSource<List<dynamic>>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    dynamic result = ExecuteDynamicEntities(query, sqlComm);
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


        /// <summary>
        /// 批量查询分表数据  返回返回动态类型集合
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public List<dynamic> ExecuteDynamicEntities(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            string QuerySQL = GetQuerySQL(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
            if (string.IsNullOrEmpty(QuerySQL))
                return new List<dynamic>();
            else
                return ExecuteDynamicEntities(QuerySQL);
        }
        /// <summary>
        /// 异步批量查询分表数据  返回动态类型集合
        /// </summary>
        /// <param name="TestType">测试类型</param>
        /// <param name="StartTime">起始时间</param>
        /// <param name="EndTime">结束时间</param>
        /// <param name="Query">查询条件 eq：`NeName`="www.qq.com"</param>
        /// <param name="Columns">查询字段（默认全部） eq：NeName，LineName"</param>
        /// <returns></returns>
        public Task<List<dynamic>> ExecuteEntitiesAsync(string TestType, DateTime StartTime, DateTime EndTime, string Columns = "*", string Query = "", string OrderLimit = "")
        {
            TaskCompletionSource<List<dynamic>> source = new TaskCompletionSource<List<dynamic>>();
            CancellationToken cancellationToken = CancellationToken.None;
            if ((cancellationToken == CancellationToken.None) || !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    List<dynamic> result = ExecuteDynamicEntities(TestType, StartTime, EndTime, Columns, Query, OrderLimit);
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

        #endregion

        #region 检查表是否存在
        /// <summary>
        /// 检查表是否存在
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private bool IsExistTable(string tableName)
        {
            object result = ExecuteScalar("SHOW TABLES like '" + tableName + "'");
            return (result == null) ? false : true;
        }
        #endregion

        #region 更新最大ID
        /// <summary>
        /// 更新最大ID
        /// </summary>
        /// <param name="se"></param>
        public void UpdateIndexID(SEntity se)
        {
            ExecuteNonQuery(string.Format("UPDATE storageengine_config SET `MaxID`='{0}',`LastTableName`='{1}' WHERE (`TestType`='{2}')", se._MaxID, se.LastTableName, se.TestType), ConfigComm);
        }
        #endregion

        #region 依据时间 获取表名
        /// <summary>
        /// 依据时间 获取表名
        /// </summary>
        /// <param name="CreateTime"></param>
        /// <returns></returns>
        private string GetTableName(SEntity se, DateTime CreateTime)
        {
            if (se.IsSplit)
                return string.Format("log_{0}_{1}", se.TableName, CreateTime.ToString("yyyy-MM-dd"));
            else
                return se.TableName;
        }
        #endregion

        #region 获取入库SQL语句
        public string DataTable2InsterSQL(DataTable dt, SEntity se, bool IsContainsID = false)
        {
            if (dt == null) { return string.Empty; }
            if (dt.Rows.Count == 0) { return string.Empty; }
            List<string> ColumnNames = new List<string>();
            StringBuilder sb_Columns = new StringBuilder("(");
            StringBuilder sb_Values = new StringBuilder();
            #region 遍历列名
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string ColumnName = dt.Columns[i].ColumnName;
                if (ColumnName.ToLower() == "id")
                {
                    continue;
                }
                sb_Columns.Append("`").Append(ColumnName).Append("`,");
                ColumnNames.Add(ColumnName);
            }
            sb_Columns.Append("`id`)");

            #endregion

            #region 遍历结果
            foreach (DataRow dr in dt.Rows)
            {
                if (sb_Values.Length == 0)
                    sb_Values.Append("(");
                else
                    sb_Values.Append(",(");

                ColumnNames.ForEach(ColumnName =>
                {
                    sb_Values.Append("'").Append(dr[ColumnName]).Append("',");
                });
                sb_Values.Append("'");
                if (!IsContainsID)
                    sb_Values.Append(se.MaxID);
                else
                    sb_Values.Append(dr["ID"].ToString());
                sb_Values.Append("')");
            }
            #endregion

            DateTime dte = Convert.ToDateTime(dt.Rows[0]["TestTime"]);
            se.LastTableName = GetTableName(se, dte);
            string InsertSql = string.Format("insert into `{0}` {1} values {2}", se.LastTableName, sb_Columns.ToString(), sb_Values.ToString());
            UpdateIndexID(se);
            return InsertSql;
        }

        public string DataTable2InsterSQL(DataTable dt)
        {
            if (dt == null) { return string.Empty; }
            if (dt.Rows.Count == 0) { return string.Empty; }
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
        #endregion

        #region 获取分表查询语句
        private string GetQuerySQL(string TestType, DateTime StartTime, DateTime EndTime, string Columns, string Query, string OrderLimit)
        {
            if (EndTime < StartTime)
            {
                SELog.SendLog("起始时间不得大于结束时间!");
                return string.Empty;
            }
            else
            {
                SEntity se = null;
                if (!SEntitys.TryGetValue(TestType, out se))
                {
                    SELog.SendLog("[" + TestType + "]测试类型未配置");
                    return string.Empty;
                }
                // eq: limit
                Query = Query.Contains("=") ? " and " + Query : Query;
                if (se.IsSplit)
                {
                    string SelectEndTable = GetTableName(se, EndTime);
                    DateTime SelectTime = StartTime;
                    List<string> Tables = new List<string>();
                    while (true)
                    {
                        string SelectTable = GetTableName(se, SelectTime);
                        if (IsExistTable(SelectTable))
                            Tables.Add(SelectTime.ToString("yyyy-MM-dd"));
                        if (SelectTable == SelectEndTable)
                            break;
                        else
                            SelectTime = SelectTime.AddDays(1);
                    }
                    if (Tables.Count <= 0) { return string.Empty; }
                    StringBuilder sb_SQL = new StringBuilder();
                    foreach (var item in Tables)
                    {
                        if (sb_SQL.Length != 0)
                        {
                            sb_SQL.Append(" union all ");
                        }
                        sb_SQL.Append(GetSQL(StartTime, EndTime, Columns, Query, string.Format("log_{0}_{1}", se.TableName, item), ""));
                    }
                    string SelectSQL = string.Empty;
                    if (string.IsNullOrEmpty(OrderLimit))
                    {
                        SelectSQL = sb_SQL.ToString();
                    }
                    else
                    {
                        SelectSQL = string.Format("select * from ({0}) as TempTable {1};", sb_SQL.ToString(), OrderLimit);
                    }
                    return SelectSQL;
                }
                else
                {
                    string SelectTable = se.TableName;
                    return GetSQL(StartTime, EndTime, Columns, Query, SelectTable, OrderLimit);
                }
            }
        }

        private string GetQuerySQL(string TestType, DateTime TestTime, string Columns, string Query, string OrderLimit)
        {
            SEntity se = null;
            if (!SEntitys.TryGetValue(TestType, out se))
            {
                SELog.SendLog("[" + TestType + "]测试类型未配置");
                return string.Empty;
            }
            string SelectTable = string.Empty;
            if (se.IsSplit)
            {
                SelectTable = GetTableName(se, TestTime);
            }
            else
            {
                SelectTable = se.TableName;
            }
            if (IsExistTable(SelectTable))
            {
                // eq: limit
                Query = Query.Contains("=") ? " and " + Query : Query;
                return GetSQL(Columns, Query, SelectTable, OrderLimit);
            }
            else
            {
                return string.Empty;
            }
        }
        #endregion

        #region 获取可执行SQL语句
        private static string GetSQL(string Columns, string Query, string SelectTable, string OrderLimit)
        {
            return string.Format("select {1} from `{0}` where 1=1 {2} {3}", SelectTable, Columns, Query, OrderLimit);
        }
        private static string GetSQL(DateTime StartTime, DateTime EndTime, string Columns, string Query, string SelectTable, string OrderLimit)
        {
            return string.Format("select {1} from `{0}` where TestTime>='{2}' and TestTime<='{3}' {4} {5}", SelectTable, Columns, StartTime, EndTime, Query, OrderLimit);
        }
        #endregion
    }
}
