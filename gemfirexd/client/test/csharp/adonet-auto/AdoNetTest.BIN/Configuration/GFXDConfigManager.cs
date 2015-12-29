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
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;


namespace AdoNetTest.BIN.Configuration
{
    /// <summary>
    /// Provides methods for symbolizing access to application configuration
    /// </summary>
    public class GFXDConfigManager
    {
        private static String configXML = String.Format("{0}\\AdoNetTest.BIN.config", Environment.CurrentDirectory);

        public static void SetConfigFile(String fileName)
        {
            if(!String.IsNullOrEmpty(fileName))
                configXML = fileName;
        }

        public static String GetGFXDServerConnectionString()
        {
            return String.Format("server={0}:{1}",
                GetClientSetting("gfxdHost"), GetClientSetting("gfxdPort"));
        }

        public static String GetGFXDLocatorConnectionString()
        {
            return String.Format("server={0}:{1}",
                GetLocatorSetting("clientBindAddress"), GetLocatorSetting("clientPort"));
        }

        public static String GetDbDefaultSetting(String key)
        {
            GFXDDbDefaultConfig config = null;

            try
            {
                config = (GFXDDbDefaultConfig)(GetConfig().Sections["gfxdDbDefaultConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetGFXDServerConnectionString()", e);
            }

            return config.GFXDDbDefaultSettings[key].Value;            
        }

        public static string GetClientSetting(String key)
        {
            GFXDClientConfig config = null;

            try
            {
                config = (GFXDClientConfig)(GetConfig().Sections["gfxdClientConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetClientSetting()", e);
            }            

            return config.GFXDClientSettings[key].Value;            
        }

        public static string GetServerSetting(String key)
        {
            GFXDServerConfig config = null;

            try
            {
                config = (GFXDServerConfig)(GetConfig().Sections["gfxdServerConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetServerSetting()", e);
            }

            return config.GFXDServerSettings[key].Value;
        }

        public static string GetLocatorSetting(String key)
        {
            GFXDLocatorConfig config = null;

            try
            {
                config = (GFXDLocatorConfig)(GetConfig().Sections["gfxdLocatorConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetLocatorSetting()", e);
            }

            return config.GFXDLocatorSettings[key].Value;
        }

        public static string GetServerGroupSetting(String key)
        {
            GFXDServerGroupConfig config = null;

            try
            {
                config = (GFXDServerGroupConfig)(GetConfig().Sections["gfxdServerGroupConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetServerGroupSetting()", e);
            }

            return config.GFXDServerGroupSettings[key].Value;
        }

        public static IList<String> GetServerGroupNames()
        {
            GFXDServerGroupConfig config = null;
            IList<String> serverGroups = new List<String>();

            try
            {
                config = (GFXDServerGroupConfig)(GetConfig().Sections["gfxdServerGroupConfig"]);
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetServerGroupNames()", e);
            }
            foreach (GFXDServerGroupSetting group in config.GFXDServerGroupSettings)
            {
                serverGroups.Add(group.Value);
            }

            return serverGroups;
        }

        public static string GetAppSetting(String key)
        {
            AppSettingsSection config = null;

            try
            {
                config = (AppSettingsSection)GetConfig().AppSettings;
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetAppSetting()", e);
            }

            return config.Settings[key].Value;
        }

        private static System.Configuration.Configuration GetConfig()
        {
            ExeConfigurationFileMap fileMap = new ExeConfigurationFileMap();

            try
            {
                if (String.IsNullOrEmpty(configXML))
                    throw new Exception(String.Format("Unable to find .config file {0}", configXML));

                fileMap.ExeConfigFilename = configXML;
            }
            catch (Exception e)
            {
                throw new Exception("Encounter exception in GetConfig()", e);
            }

            return ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None);
        }
    }
}
