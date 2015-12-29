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
    class GFXDLocator
    {
        /// <summary>
        /// Configurable class member properties
        /// </summary>
        private static string installPath;
        private static string scriptFile;
        private static string peerDiscoveryAddress;
        private static int peerDiscoveryPort;
        private static string clientBindAddress;
        private static int clientPort;
        private static string defaultDir;
        private static string logDir;
        private static string locatorScript;
        private static Logger.Logger logger;
        private static object locker = new object();

        /// <summary>
        /// Instance member properties
        /// </summary>
        public GFXDState LocatorState { get; set; }
        private String locatorDir;

        public string PeerDiscoveryAddress
        {
            get
            {
                return peerDiscoveryAddress;
            }
        }

        public int PeerDiscoveryPort
        {
            get
            {
                return peerDiscoveryPort;
            }
        }

        public string ClientBindAddress
        {
            get
            {
                return clientBindAddress;
            }
        }

        public int ClientPort
        {
            get
            {
                return clientPort;
            }
        }


        public GFXDLocator()
        {
            LoadConfig();

            if (logger != null)
                logger.Close();

            logger = new Logger.Logger(logDir, String.Format("{0}.log", typeof(GFXDLocator).FullName));

            locatorDir = string.Format(@"{0}\{1}", installPath, defaultDir);
            LocatorState = GFXDState.STOPPED;
        }

        public static void LoadConfig()
        {
            //installPath = GFXDConfigManager.GetServerSetting("installPath");
            installPath = Environment.GetEnvironmentVariable("GEMFIREXD");
            scriptFile = GFXDConfigManager.GetServerSetting("scriptFile");
            peerDiscoveryAddress = GFXDConfigManager.GetLocatorSetting("peerDiscoveryAddress");
            peerDiscoveryPort = int.Parse(GFXDConfigManager.GetLocatorSetting("peerDiscoveryPort"));
            clientBindAddress = GFXDConfigManager.GetLocatorSetting("clientBindAddress");
            clientPort = int.Parse(GFXDConfigManager.GetLocatorSetting("clientPort"));
            defaultDir = GFXDConfigManager.GetLocatorSetting("locatorDir");
            //logDir = GFXDConfigManager.GetClientSetting("logDir");
            logDir = Environment.GetEnvironmentVariable("GFXDADOOUTDIR");
            locatorScript = String.Format(@"{0}\{1}", installPath, scriptFile);
        }

        public void Start()
        {
            Log(String.Format("Starting Locator, LocatorState: {0}", LocatorState));
            Log(String.Format("locatorScript: {0}, startCommand: {1}", locatorScript, GetStartCmd()));

            if (this.LocatorState == GFXDState.STARTED)
                return;            

            ProcessStartInfo psinfo = new ProcessStartInfo(locatorScript, GetStartCmd());

            psinfo.UseShellExecute = true;
            psinfo.WindowStyle = ProcessWindowStyle.Hidden;

            Process proc = new Process();
            proc.StartInfo = psinfo;


            if (!proc.Start())
            {
                String msg = "Failed to start locator process.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            bool started = proc.WaitForExit(60000);

            if (!started)
            {
                proc.Kill(); 
                
                String msg = "Timeout waiting for locator process to start.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            this.LocatorState = GFXDState.STARTED;

            Log(String.Format("Locator started, LocatorState: {0}", LocatorState));
        }

        public void Stop(bool removeDir)
        {
            Log(String.Format("Stopping Locator, LocatorState: {0}", LocatorState));

            Log(String.Format("locatorScript: {0}, startCommand: {1}", locatorScript, GetStopCmd()));

            if (this.LocatorState == GFXDState.STOPPED)
                return;

            
            ProcessStartInfo psinfo = new ProcessStartInfo(locatorScript, GetStopCmd());

            psinfo.UseShellExecute = true;
            psinfo.WindowStyle = ProcessWindowStyle.Hidden;

            Process proc = new Process();
            proc.StartInfo = psinfo;

            if (!proc.Start())
            {
                String msg = "Failed to stop locator process.";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            bool started = proc.WaitForExit(60000);

            if (!started)
            {
                proc.Kill();

                String msg = "Timeout waiting for locator process to stop";
                Log(msg);
                throw new Exception(String.Format("Exception: {0}", msg));
            }

            this.LocatorState = GFXDState.STOPPED;

            if (removeDir)
                RemoveLocatorDir();

            Log(String.Format("Locator stopped, LocatorState: {0}", LocatorState));
        }


        private string GetStartCmd()
        {
            CreateLocatorDir();

            return String.Format(
                @"locator start -dir={0} -peer-discovery-address={1} -peer-discovery-port={2} "
                + " -client-bind-address={3} -client-port={4} -run-netserver=true",
                locatorDir, 
                peerDiscoveryAddress, peerDiscoveryPort, 
                clientBindAddress, clientPort);
        }

        private string GetStopCmd()
        {
            return String.Format(@"locator stop -dir={0}", locatorDir);
        }

        private void CreateLocatorDir()
        {
            try
            {
                if (!Directory.Exists(locatorDir))
                    Directory.CreateDirectory(locatorDir);
            }
            catch (Exception e)
            {
                Log(e.Message);
                Log("Locator Dir.: " + locatorDir);
            }
        }

        private void RemoveLocatorDir()
        {
            try
            {
                if (Directory.Exists(locatorDir))
                    Directory.Delete(locatorDir, true);
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
