#region 文件描述
//---------------------------------------------------------------------------------------------
// 文 件 名: SEManager.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/10 15:40:41
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
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace XqStorageEngine
{
    public class SEManager
    {
        ISEHelper SEHelper = null;
        public Dictionary<string, SEntity> SEntitys = new Dictionary<string, SEntity>();
        
        #region 构造函数
        public SEManager(DBType dbt, string dataSqlComm,string timeColumnName="TestTime",string Pref="Log")
        {
            var seHelperName = ((DescriptionAttribute)dbt.GetType().GetField(dbt.ToString()).GetCustomAttributes(typeof(DescriptionAttribute), false).First()).Description;
            SEHelper = Assembly.GetExecutingAssembly().CreateInstance("XqStorageEngine." + seHelperName) as ISEHelper;
            SEHelper.DBConnentString = dataSqlComm;
            SEHelper.SEntitys = SEntitys;
            SEHelper.Pref = Pref;
            SEHelper.TimeColumnName=timeColumnName;
        } 
        #endregion

        #region 注册存储实体类
        /// <summary>
        /// 注册存储实体类
        /// </summary>
        /// <param name="DataType">测试类型</param>
        /// <param name="maxID">主键ID</param>
        /// <param name="tableName">表名</param>
        /// <param name="createSQl">分表存储SQL，填空表示不分表存储</param>
        /// <param name="updateID">数据更新ID</param>
        public void RegisterItem(string dataType, long maxID, string tableName, string createSQl, Action<string, string, long> updateAction)
        {
            SEntity se = new SEntity();
            se._MaxID = maxID;
            se.CreateSQL = createSQl;
            se.TableName = tableName;
            se.DataType = dataType;
            se.IsSplit = !string.IsNullOrEmpty(createSQl);
            se.updateAction = updateAction;
            if (SEntitys.ContainsKey(dataType))
            {
                SEntitys[se.DataType] = se;
                SELog.SendLog("updated " + dataType + " Storage Entity!");
            }
            else
            {
                SEntitys.Add(se.DataType, se);
                SELog.SendLog("Created " + dataType + " Storage Entity!");
            }
        }
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

        #region 依据时间 获取表名
        /// <summary>
        /// 依据时间 获取表名
        /// </summary>
        /// <param name="CreateTime"></param>
        /// <returns></returns>
        public string GetTableName(SEntity se, DateTime dataTime)
        {
            return SEHelper.GetTableName(se, dataTime);
        }

        public string GetTableName(string TestType, DateTime dataTime)
        {
            SEntity se = null;
            if (SEntitys.TryGetValue(TestType, out se))
            {
                return SEHelper.GetTableName(se, dataTime);
            }
            else {
                return null;
            }
           
        }

        #endregion

        #region 数据转SQL
        public string DataTable2InsterSQL(DataTable dt) {
            return SEHelper.DataTable2InsterSQL(dt);
        }
        public string Entity2InsterSQL<T>(string tableName, List<T> ts) {
            return SEHelper.Entity2InsterSQL(tableName,ts);
        }
        public string Entity2InsterSQL<T>(string tableName, T t) {
            return SEHelper.Entity2InsterSQL(tableName, t);
        }
        #endregion
    }
}
