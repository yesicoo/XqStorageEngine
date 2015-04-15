using Fleck;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;

namespace WebSocketLib
{
    public class WsMain
    {
        ConcurrentDictionary<Guid, IWebSocketConnection> Wscs = new ConcurrentDictionary<Guid, IWebSocketConnection>();
        ConcurrentDictionary<string, Action<string, Guid, Dictionary<string, string>>> CommItems = new ConcurrentDictionary<string, Action<string, Guid, Dictionary<string, string>>>();
        WebSocketServer WSS = null;

        public WsMain(int port)
        {
            WSS = new WebSocketServer("ws://0.0.0.0:" + port);
        }

        public void Start()
        {
            WSS.Start(socket =>
              {
                  socket.OnOpen = () =>
                  {
                      Wscs.TryAdd(socket.ConnectionInfo.Id, socket);
                  };
                  socket.OnClose = () =>
                  {
                      IWebSocketConnection so = null;
                      Wscs.TryRemove(socket.ConnectionInfo.Id, out so);
                  };
                  socket.OnMessage = (msg) => DoMessage(socket, msg);
                  socket.OnBinary = (bs) => DoBinary(socket, bs);
                  socket.OnError = (e) => DoError(socket, e);
              });

        }

        private void DoError(IWebSocketConnection socket, Exception e)
        {
            Console.WriteLine(e.Message);
        }

        public bool RegisterCommItem(string CommKey, Action<string, Guid, Dictionary<string, string>> doAction)
        {
            return CommItems.TryAdd(CommKey, doAction);
        }

        public bool UnRegisterCommItem(string CommKey)
        {
            Action<string, Guid, Dictionary<string, string>> doAction = null;
            return CommItems.TryRemove(CommKey, out doAction);
        }


        private object DoBinary(IWebSocketConnection socket, byte[] bs)
        {
            throw new NotImplementedException();
        }

        private void DoMessage(IWebSocketConnection socket, string msg)
        {

            dynamic result = JsonConvert.DeserializeObject(msg);
            string CommKey = result.CommKey;
            Dictionary<string, string> KeyValues = result.KeyValue;
            Action<string, Guid, Dictionary<string, string>> doAction = null;
            if (CommItems.TryGetValue(CommKey, out doAction))
            {
                new Thread(() => { doAction(CommKey, socket.ConnectionInfo.Id, KeyValues); }).Start();
            }
            else
            {

            }
        }

        public bool SendMessage(Guid guid, string commKey, Dictionary<string, string> keyValues)
        {

            IWebSocketConnection socket = null;
            if (Wscs.TryGetValue(guid, out socket))
            {
                dynamic SendData = new ExpandoObject();
                SendData.CommKey = commKey;
                SendData.KeyValues = keyValues;
                string strData = JsonConvert.SerializeObject(SendData);
                if (socket.IsAvailable)
                {
                    socket.Send(strData);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;

            }
        }

    }
}
