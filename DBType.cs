#region 文件描述
//---------------------------------------------------------------------------------------------
// 文 件 名: DBType.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/10 16:01:41
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
using System.Linq;
using System.Text;

namespace XqStorageEngine
{
    public enum DBType
    {
        [DescriptionAttribute("MySQL_SEHelper")]
        MySQL_Or_MariaDB,
        [DescriptionAttribute("MS_SEHelper")]
        MsSQLServer
    }
}
