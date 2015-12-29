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
using System.Configuration;
using GemFireXDDBI.DBObjects;

namespace GemFireXDDBI.Configuration
{
    /// <summary>
    /// Implementation of utility methods for reading and writing connection strings
    /// </summary>
    class Configurator
    {
        public static IList<ConnectionStringSettings> GetDBConnStrings()
        {
            IList<ConnectionStringSettings> connStrings = new List<ConnectionStringSettings>();
            ConnectionStringSettingsCollection settingColl = ConfigurationManager.ConnectionStrings;            

            if (settingColl != null)
            {
                foreach (ConnectionStringSettings cs in settingColl)
                    connStrings.Add(cs);
            }

            return connStrings;
        }

        public static IList<ConnectionStringSettings> GetDBConnStrings(String providerName)
        {
            IList<ConnectionStringSettings> connStrings = new List<ConnectionStringSettings>();
            ConnectionStringSettingsCollection settingColl = ConfigurationManager.ConnectionStrings;

            if (settingColl != null)
            {
                foreach (ConnectionStringSettings cs in settingColl)
                {
                    if (cs.ProviderName == providerName)
                        connStrings.Add(cs);
                }
            }

            return connStrings;
        }

        public static String GetDBConnString(String name)
        {
            ConnectionStringSettings setting = ConfigurationManager.ConnectionStrings[name];
            
            if (setting != null)
                return setting.ConnectionString;

            return null;
        }

        public static ConnectionStringSettings GetDBConnSetting(String name)
        {
            ConnectionStringSettings setting = ConfigurationManager.ConnectionStrings[name];

            if (setting != null)
                return setting;

            return null;
        }

        public static void AddDBConnString(ConnectionStringSettings setting)
        {
            try
            {
                System.Configuration.Configuration config =
                    ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

                config.ConnectionStrings.ConnectionStrings.Add(setting);
                config.Save(ConfigurationSaveMode.Modified, true);
                ConfigurationManager.RefreshSection("connectionStrings");
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }
        }

        public static void RemoveDBConnString(string  connName)
        {
            try
            {
                System.Configuration.Configuration config =
                    ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

                config.ConnectionStrings.ConnectionStrings.Remove(connName);
                config.Save(ConfigurationSaveMode.Modified, true);
                ConfigurationManager.RefreshSection("connectionStrings");
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }
        }

        public static void UpdateDBConnString(ConnectionStringSettings setting)
        {
            try
            {
                System.Configuration.Configuration config =
                    ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

                config.ConnectionStrings.ConnectionStrings.Remove(setting.Name);
                config.Save(ConfigurationSaveMode.Modified, true);
                ConfigurationManager.RefreshSection("connectionStrings");
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }
        }
    }
}
