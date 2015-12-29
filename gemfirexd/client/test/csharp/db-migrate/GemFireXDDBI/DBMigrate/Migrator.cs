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
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using GemFireXDDBI.DBObjects;
using GemFireXDDBI.DBI;
using GemFireXDDBI.Util;

namespace GemFireXDDBI.DBMigrate
{
    /// <summary>
    /// Automates the migration process per user options
    /// </summary>
    class Migrator
    {
        public delegate void MigrateEventHandler(MigrateEventArgs e);
        public static event MigrateEventHandler MigrateEvent;

        public static String SourceDBConnName { get; set; }
        public static String DestDBConnName { get; set; }
        public static bool MigrateTables { get; set; }
        public static bool MigrateViews { get; set; }
        public static bool MigrateProcedures { get; set; }
        public static bool MigrateFunctions { get; set; }
        public static bool MigrateTriggers { get; set; }
        public static bool MigrateIndexes { get; set; }
        public static bool Errorred { get; set; }
        public static Result Result { get; set; }

        private static IDictionary<String, DbTable> dbTableList = new Dictionary<String, DbTable>();

        public static void MigrateDB()
        {
            SQLSvrDbi.DbConnectionString = Configuration.Configurator.GetDBConnString(SourceDBConnName);
            GemFireXDDbi.DbConnectionString = Configuration.Configurator.GetDBConnString(DestDBConnName);

            try
            {
                if (MigrateTables)
                    MigrateDbTables();
                if (MigrateViews)
                    MigrateDbViews();
                if (MigrateIndexes)
                    MigrateDbIndexes();
                if (MigrateProcedures)
                    MigrateDbStoredProcedures();
                if (MigrateFunctions)
                    MigrateDbFunctions();
                if (MigrateTriggers)
                    MigrateDbTriggers();
            }
            catch (ThreadAbortException e)
            {
                Result = Result.Aborted;
                OnMigrateEvent(new MigrateEventArgs(Result.Aborted, "Operation aborted!"));
            }
            catch (Exception e)
            {
                Helper.Log(e);
            }
            finally
            {
                dbTableList.Clear();
            }
        }

        public static DbTable GetTableFromList(String tableFullName)
        {
            return dbTableList[tableFullName];
        }

        public static String GetTableFullName(String tableName)
        {
            foreach (DbTable table in dbTableList.Values)
            {
                if (table.TableName == tableName.ToUpper())
                    return table.TableFullName;
            }

            return tableName;
        }

        private static void MigrateDbTables()
        {
            Helper.Log("Start migrating database tables");

            DataTable dt = SQLSvrDbi.GetTableNames();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbTable table = new DbTable(
                        row["SchemaName"].ToString().ToUpper(), row["TableName"].ToString().ToUpper());
                    dbTableList.Add(table.TableFullName, table);
                }

                foreach (DbTable table in dbTableList.Values)
                    table.Migrate();
            }
        
            Helper.Log("Finished migrating database tables");
        }

        private static void MigrateDbIndexes()
        {
            Helper.Log("Start migrating database indexes");
            
            DataTable dt = SQLSvrDbi.GetIndexes();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbIndex index = new DbIndex(
                        row["SchemaName"].ToString().ToUpper(), row["TableName"].ToString().ToUpper(),
                        row["IndexName"].ToString().ToUpper(), row["ColumnName"].ToString().ToUpper(),
                        bool.Parse(row["IsUnique"].ToString()));

                    index.Migrate();

                }
            }
            
            Helper.Log("Finished migrating database indexes");
        }

        private static void MigrateDbStoredProcedures()
        {
            Helper.Log("Start migrating stored procedures");
                        
            DataTable dt = SQLSvrDbi.GetStoredProcedures();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbModule module = new DbModule(
                        row["SchemaName"].ToString(), row["ProcedureName"].ToString(),
                        row["ProcedureDefinition"].ToString(), ModuleType.StoredProcedure);

                    module.Migrate();
                }
            }
        
            Helper.Log("Finished migrating stored procedures");
        }

        private static void MigrateDbFunctions()
        {
            Helper.Log("Start migrating database functions");
                        
            DataTable dt = SQLSvrDbi.GetFunctions();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbModule module = new DbModule(
                        row["SchemaName"].ToString(), row["FunctionName"].ToString(),
                        row["FunctionDefinition"].ToString(), ModuleType.Function);

                    module.Migrate();
                }
            }
        
            Helper.Log("Finished migrating database functions");
        }

        private static void MigrateDbTriggers()
        {
            Helper.Log("Start migrating database triggers");

            DataTable dt = SQLSvrDbi.GetTriggers();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbModule module = new DbModule(
                        row["SchemaName"].ToString(), row["TriggerName"].ToString(),
                        row["TriggerDefinition"].ToString(), ModuleType.Trigger);

                    module.Migrate();
                }
            }

            Helper.Log("Finished migrating database triggers");
        }

        private static void MigrateDbViews()
        {
            Helper.Log("Start migrating database views");

            DataTable dt = SQLSvrDbi.GetViews();

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbModule module = new DbModule(row["SchemaName"].ToString(),
                        row["ViewName"].ToString(), row["ViewDefinition"].ToString(), 
                        ModuleType.View);

                    module.Migrate();
                }
            }
        
            Helper.Log("Finished migrating database views");
        }

        private static void ValidateTableCreation()
        {
            foreach (DbTable table in dbTableList.Values)
            {
                if (!table.Validate())
                    Helper.Log(String.Format("Failed to create table {0}", 
                        table.TableFullName));
            }
        }

        private static void ListTables()
        {
            foreach (DbTable table in dbTableList.Values)
                Helper.Log(table.GetCreateTableSql());
        }

        public static void OnMigrateEvent(MigrateEventArgs e)
        {
            if (MigrateEvent != null)
                MigrateEvent(e);
        }
    }
}
