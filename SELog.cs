#region 文件描述
//----------------------------------------------------------------------------------------------
// 文 件 名: SELog.cs
// 作    者：XuQing
// 邮    箱：Code@XuQing.me
// 创建时间：2015/4/3 9:37:13
// 描    述：
// 版    本：Version 1.0
//----------------------------------------------------------------------------------------------
// 历史更新纪录
//----------------------------------------------------------------------------------------------
// 版    本：           修改时间：           修改人：           
// 修改内容：
//----------------------------------------------------------------------------------------------
// 本文件内代码如果没有特殊说明均遵守MIT开源协议 http://opensource.org/licenses/mit-license.php
//----------------------------------------------------------------------------------------------
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XqStorageEngine
{
    public delegate void LogHandler(string log, bool IsException);
    public class SELog
    {
        public static event LogHandler sendLog;

        public static void SendLog(string log, bool IsException=false)
        {
            try
            {
                sendLog("[XqStorageEngine]:" + log, IsException);
            }
            catch (Exception)
            {
            }
        }
    }
}
