using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WebSocketLib.Test
{
    class Program
    {
      static  WsMain wsMain = null;
        static void Main(string[] args)
        {
            wsMain = new WsMain(10801);
            wsMain.RegisterCommItem("Hello", DoHello);
            Console.ReadLine();
        }

        private static void DoHello(string commKey, Guid guid, Dictionary<string, string> keyValues)
        {
            Console.WriteLine(string.Format("CommKey:{0};Guid{1};Key1:{2}", commKey, guid, keyValues["Key1"]));
            wsMain.SendMessage(guid, "SB", keyValues);
        }
    }
}
