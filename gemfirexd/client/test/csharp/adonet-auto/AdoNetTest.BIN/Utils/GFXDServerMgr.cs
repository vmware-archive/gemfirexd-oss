/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AdoNetTest.BIN.Configuration;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Manages GFXD locator and server processes
    /// </summary>
    class GFXDServerMgr
    {
        private static bool startServer;
        private static int numServers;
        private static string logDir;
        private static IDictionary<String, GFXDServer> GFXDServers;
        private static GFXDLocator locator;

        private static Logger.Logger logger;
        private static object locker = new object();

        public static void Initialize()
        {
            LoadConfig();

            if (!startServer)
                return;

            if (logger != null)
                logger.Close();

            logger = new Logger.Logger(logDir  , typeof(GFXDServerMgr).FullName + ".log");

            locator = new GFXDLocator();
            locator.Start();

            try
            {
                for (int i = 0; i < numServers; i++)
                {
                    GFXDServer server = new GFXDServer(locator);
                    server.Start();
                    AddServer(server);
                }
            }
            catch (Exception e)
            {
                Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
            }
        }

        private static void LoadConfig()
        {
            startServer = bool.Parse(GFXDConfigManager.GetServerSetting("startServer"));
            numServers = int.Parse(GFXDConfigManager.GetServerSetting("numServers"));
            //logDir = GFXDConfigManager.GetClientSetting("logDir");
            logDir = Environment.GetEnvironmentVariable("GFXDADOOUTDIR");
            GFXDServers = new Dictionary<String, GFXDServer>();
        }

        public static void AddServer(GFXDServer server)
        {
            GFXDServers.Add(server.ServerName, server);
        }

        public static GFXDServer GetServer(String serverName)
        {
            GFXDServer server = null;
            String error = string.Empty;

            if (GFXDServers.ContainsKey(serverName))
            {
                try
                {
                    server = GFXDServers[serverName];
                    if (server == null)
                    {
                        error = String.Format("Server {0} is null", serverName);
                        Log(error);
                        throw new Exception(error);
                    }
                }
                catch(Exception e)
                {
                    error = String.Format(
                        "Encounter exception in GetServer() for {0}. {1}", serverName, e.Message);
                    Log(error);
                    throw new Exception(error);
                }
            }
            else
            {
                error = String.Format("Server {0} does not exist", serverName);
                Log(error);
                throw new Exception(error);
            }

            return server;
        }

        public static GFXDServer GetServer(int serverIndex)
        {
            String error = string.Empty;

            if (serverIndex < 0 || serverIndex >= GFXDServers.Count)
            {
                error = String.Format("Invalid server index {0}", serverIndex);
                Log(error);
                throw new Exception(error);
            }

            return GetServer(GFXDServers.Keys.ToArray<String>()[serverIndex]);
        }

        public static void RemoveServer(String serverName)
        {
            StopServer(serverName, true);
            GFXDServers.Remove(serverName);
        }

        public static void StartServer(String serverName)
        {
            GFXDServer server = GetServer(serverName);

            if(server.ServerState == GFXDState.STOPPED)
                server.Start();
        }

        public static void StartServer(int serverIndex)
        {
            StartServer(GetServer(serverIndex).ServerName);
        }

        public static void StopServer(String serverName, bool removeDir)
        {
            GFXDServer server = GetServer(serverName);

            if (server.ServerState == GFXDState.STARTED)
                server.Stop(removeDir);
        }

        public static void StopServer(int serverIndex, bool removeDir)
        {
            StopServer(GetServer(serverIndex).ServerName, removeDir);
        }        

        public static void StartAllServers()
        {
            foreach (String svrName in GFXDServers.Keys)
                StartServer(svrName);
        }

        public static void StopAllServers()
        {
            foreach (String svrName in GFXDServers.Keys)
                StopServer(svrName, true);
        }

        public static void Cleanup()
        {
            StopAllServers();
            GFXDServers.Clear();

            locator.Stop(true);
        }

        private static void Log(String msg)
        {            
            lock (locker)
            {
                logger.Write(msg);
            }
        }
    }
}
