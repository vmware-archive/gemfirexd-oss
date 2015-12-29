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

namespace GemFireXDDBI.DBI
{
    /// <summary>
    /// Abstracts instantiation of specific data access interface implementations
    /// </summary>
    class SQLFactory
    {
        public static SQLBase GetSqlDBI(string dbConnName)
        {
            ConnectionStringSettings setting = Configuration.Configurator.GetDBConnSetting(dbConnName);

            switch (setting.ProviderName)
            {
                case "System.Data.SqlClient":
                    return new SQLSvr(setting.ConnectionString);
                case "Pivotal.Data.GemFireXD":
                    return new GemFireXD(setting.ConnectionString);
            }

            return null;
        }
    }
}
