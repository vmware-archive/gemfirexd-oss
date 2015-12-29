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
using System.Diagnostics;
using AdoNetTest.BIN.Configuration;

namespace AdoNetTest.BIN
{
    class GFXDServer
    {
        /// <summary>
        /// Configurable class member properties
        /// </summary>
        private static int numServers;
        private static string installPath;
        private static string scriptFile;
        private static string defaultServerDir;
        private static string defaultClientBindAddress;
        private static int defaultClientPort;
        private static int mcastPort;
        private static string drdaDebug;
        private static string logLevel;
        private static string logDir;
        private static string serverScript;
        private static int serverCount = 0;
        private static Logger.Logger logger;
        private static object locker = new object();

        /// <summary>
        /// Instance member properties
        /// </summary>
        private string serverDir;
        private string clientBindAddress;
        private int clientPort;
        private GFXDLocator locator;
        public string ServerName { get; set; }
        public GFXDState ServerState { get; set; }

        public GFXDServer()
        {
            LoadConfig();

            if (logger != null)
                logger.Close();

            logger = new Logger.Logger(logDir, String.Format("{0}.log", typeof(GFXDServer).FullName));

            if (numServers == 1)
                mcastPort = 0;           

            clientBindAddress = defaultClientBindAddress;
            clientPort = defaultClientPort + (serverCount++);
            ServerName = string.Format("{0}{1}", defaultServerDir, serverCount);
            serverDir = string.Format(@"{0}\{1}", installPath, ServerName);
            this.ServerState = GFXDState.STOPPED;
        }

        public GFXDServer(GFXDLocator locator)
            : this()
        {
            this.locator = locator;
        }

        public static void LoadConfig()
        {
            numServers = int.Parse(GFXDConfigManager.GetServerSetting("numServers"));
            //installPath = GFXDConfigManager.GetServerSetting("installPath");
            installPath = Environment.GetEnvironmentVariable("GEMFIREXD");
            scriptFile = GFXDConfigManager.GetServerSetting("scriptFile");
            defaultServerDir = GFXDConfigManager.GetServerSetting("defaultServerDir");
            defaultClientBindAddress = GFXDConfigManager.GetServerSetting("defaultClientBindAddress");
            defaultClientPort = int.Parse(GFXDConfigManager.GetServerSetting("defaultClientPort"));
            mcastPort = int.Parse(GFXDConfigManager.GetServerSetting("mcastPort"));
            drdaDebug = GFXDConfigManager.GetServerSetting("drdaDebug");
            logLevel = GFXDConfigManager.GetServerSetting("logLevel");
            //logDir = GFXDConfigManager.GetClientSetting("logDir");
            logDir = Environment.GetEnvironmentVariable("GFXDADOOUTDIR");
            serverScript = String.Format(@"{0}\{1}", installPath, scriptFile);            
        }

        public void Start()
        {
            Log(String.Format("Starting server {0}, serverState: {1}", ServerName, ServerState));
            Log(String.Format("serverScript: {0}, startCommand: {1}", serverScript, GetStartCmd()));

            if (this.ServerState == GFXDState.STARTED)
                return;            

            ProcessStartInfo psinfo = new ProcessStartInfo(serverScript, GetStartCmd());

            psinfo.UseShellExecute = true;
            psinfo.WindowStyle = ProcessWindowStyle.Hidden;

            Process proc = new Process();
            proc.StartInfo = psinfo;

            if (!proc.Start())
            {
                String msg = "Failed to start eslasticsql process.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            bool started = proc.WaitForExit(60000);
            if (!started)
            {
                proc.Kill();
                String msg = "Timeout waiting for elasticsql process to start.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            this.ServerState = GFXDState.STARTED;

            Log(String.Format("Server {0} started, serverState: {1}", ServerName, ServerState));
        }

        public void Stop(bool removeDir)
        {
            Log(String.Format("Stopping server {0}, serverState: {1}", ServerName, ServerState));
            Log(String.Format("serverScript: {0}, stopCommand: {1}", serverScript, GetStopCmd()));

            if (this.ServerState == GFXDState.STOPPED)
                return;

            
            ProcessStartInfo psinfo = new ProcessStartInfo(serverScript, GetStopCmd());

            psinfo.UseShellExecute = true;
            psinfo.WindowStyle = ProcessWindowStyle.Hidden;

            Process proc = new Process();
            proc.StartInfo = psinfo;

            if (!proc.Start())
            {
                String msg = "Failed to stop elasticsql process.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }
            
            bool started = proc.WaitForExit(60000);

            if (!started)
            {
                proc.Kill();
                String msg = "Timeout waiting for elasticsql process to stop.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            this.ServerState = GFXDState.STOPPED;

            if (removeDir)
                RemoveServerDir();

            Log(String.Format("Server {0} stopped, serverState: {1}", ServerName, ServerState));
            serverCount--;
        }


        private string GetStartCmd()
        {
            CreateServerDir();

            String cmd = string.Empty;

            if(locator != null)
                cmd = String.Format(
                    @"server start -dir={0} -J-Dgemfirexd.drda.debug={1} -log-level={2} "
                    + "-locators={3}[{4}] -client-bind-address={5} -client-port={6} -run-netserver=true ",
                    serverDir, drdaDebug, logLevel, 
                    locator.PeerDiscoveryAddress,locator.PeerDiscoveryPort, 
                    clientBindAddress, clientPort);
            else
                cmd = String.Format(
                    @"server start -dir={0} -J-Dgemfirexd.drda.debug={1} "
                    + "-log-level={2} -client-bind-address={3}  -client-port={4} -mcast-port={5}",
                    serverDir, drdaDebug, logLevel, clientBindAddress, clientPort, mcastPort);

            return cmd;
        }

        private string GetStopCmd()
        {
            return String.Format(@"server stop -dir={0}", serverDir);
        }

        private void CreateServerDir()
        {
            try
            {
                if (!Directory.Exists(serverDir))
                    Directory.CreateDirectory(serverDir);
            }
            catch (Exception e)
            {
                Log(e.Message);
                Log("Server Dir.: " + serverDir);
            }
        }

        private void RemoveServerDir()
        {
            try
            {
                if (Directory.Exists(serverDir))
                    Directory.Delete(serverDir, true);
            }
            catch (Exception e)
            {
                Log(e.Message);
            }
        }

        private void Log(String msg)
        {
            lock (locker)
            {
                logger.Write(msg);
            }
        }
    }
}
