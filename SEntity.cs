#region 文件描述
//-----------------------------------------------------------------------------
// 文 件 名: SEntity.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/3 9:35:28
// 描    述：
// 版    本：Version 1.0
//-----------------------------------------------------------------------------
// 历史更新纪录
//-----------------------------------------------------------------------------
// 版    本：           修改时间：           修改人：           
// 修改内容：
//-----------------------------------------------------------------------------
// 本文件内代码如果没有特殊说明均遵守WTFPL开源协议 http://www.wtfpl.net/about/
//-----------------------------------------------------------------------------
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XqStorageEngine
{
    public class SEntity
    {
        /// <summary>
        /// 测试类型
        /// </summary>
        public string TestType { set; get; }
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { set; get; }
        /// <summary>
        /// 最新分表名称
        /// </summary>
        public string LastTableName { set; get; }

        public string _CreateSQL;
        /// <summary>
        /// 创建SQL语句
        /// 表名用 {0} 替换
        /// </summary>
        public string CreateSQL
        {
            get
            {
                lock (this)
                {
                    return string.Format(_CreateSQL, LastTableName);
                }
            }
            set { _CreateSQL = value; }
        }
        /// <summary>
        /// 最大ID
        /// </summary>
        public long _MaxID;
        public long MaxID
        {
            get
            {
                lock (this)
                {
                    return _MaxID++;
                }
            }
            set { _MaxID = value; }
        }

        public bool IsSplit { get; set; }
    }
}
