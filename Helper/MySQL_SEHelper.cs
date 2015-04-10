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
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace XqStorageEngine
{
    public class MySQL_SEHelper :ISEHelper
    {
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
    }
}
