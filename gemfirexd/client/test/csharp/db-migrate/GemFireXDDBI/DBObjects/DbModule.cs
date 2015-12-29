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
using Pivotal.Data.GemFireXD;
using GemFireXDDBI.DBI;
using GemFireXDDBI.DBMigrate;

namespace GemFireXDDBI.DBObjects
{
    public enum ModuleType
    {
        StoredProcedure,
        Function,
        Trigger,
        View,
        Unknown
    }

    /// <summary>
    /// Represents basic attributes for stored procedures, functions, triggers, and views
    /// </summary>
    class DbModule
    {
        public String SchemaName { get; set; }
        public String ModuleName { get; set; }
        public String ModuleFullName
        {
            get
            {
                if (!String.IsNullOrEmpty(SchemaName))
                    return String.Format("{0}.{1}", SchemaName, ModuleName);
                else
                    return ModuleName;
            }
        }
        public String Definition { get; set; }
        public ModuleType ModuleType { get; set; }

        public DbModule()
            : this(String.Empty, String.Empty, String.Empty, ModuleType.Unknown)
        {
        }

        public DbModule(String schemaName, String moduleName, String definition, ModuleType type)
        {
            this.SchemaName = schemaName;
            this.ModuleName = moduleName;
            this.Definition = definition;
            this.ModuleType = type;
        }

        public void Migrate()
        {
            String sql = GetCreateModuleSql();
            try
            {
                Util.Helper.Log(sql);
                GemFireXDDbi.Create(sql);
                Util.Helper.Log(String.Format("Module {0} created", ModuleName));
            }
            catch (Exception e)
            {
                Migrator.Errorred = true;
                Util.Helper.Log(e);
            }
        }

        private String GetCreateModuleSql()
        {            
            //Definition = Definition.Replace("AS", String.Empty);
            //Definition = Definition.Replace("BEGIN", String.Empty);
            //Definition = Definition.Replace("END;", String.Empty);
            //Definition = Definition.Replace(";", String.Empty);
            String sql = Definition.Replace("[", String.Empty).Replace("]", String.Empty);
            sql = sql.TrimEnd(new char[] { ';' });

            return sql;
        }
    }
}
