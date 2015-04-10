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
        public string DataTable2InsterSQL(System.Data.DataTable dt)
        {
            return "";
        }

        public string Entity2InsterSQL<T>(string tableName, List<T> ts)
        {
            return "";
        }

        public string Entity2InsterSQL<T>(string tableName, T t)
        {
            return "";
        }
    }
}
