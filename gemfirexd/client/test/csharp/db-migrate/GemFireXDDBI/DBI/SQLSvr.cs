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

namespace GemFireXDDBI.DBI
{
    /// <summary>
    /// Facade for SQLServer interface
    /// </summary>
    class SQLSvr : SQLBase
    {
        public SQLSvr(String connString)
        {
            SQLSvrDbi.DbConnectionString = connString;
        }

        public override int TestConnection()
        {
            return SQLSvrDbi.TestConnection();
        }

        public override DataTable GetTableNames()
        {
            return SQLSvrDbi.GetTableNames();
        }

        public static String GetTableSchema(string tableName)
        {
            return SQLSvrDbi.GetTableSchema(tableName);
        }

        public DataTable GetTableColumns(string schemaName, string tableName)
        {
            return SQLSvrDbi.GetTableColumns(schemaName, tableName);
        }

        public static DataTable GetPKConstraint(string schemaName, string tableName)
        {
            return SQLSvrDbi.GetPKConstraint(schemaName, tableName);
        }

        public static DataTable GetFKConstraints(string schemaName, string tableName)
        {
            return SQLSvrDbi.GetFKConstraints(schemaName, tableName);
        }

        public static DataTable GetCKConstraints(string schemaName, string tableName)
        {
            return SQLSvrDbi.GetCKConstraints(schemaName, tableName);
        }

        public override DataTable GetTableData(string tableName)
        {
            return (DataTable)SQLSvrDbi.GetTableData(tableName);
        }

        public override DataTable GetViews()
        {
            return SQLSvrDbi.GetViews();
        }

        public override DataTable GetViewData(string viewName)
        {
            return SQLSvrDbi.GetViewData(viewName);
        }

        public override DataTable GetIndexes()
        {
            return SQLSvrDbi.GetIndexes();
        }

        public override DataTable GetIndex(string indexName)
        {
            return SQLSvrDbi.GetIndex(indexName);
        }

        public override DataTable GetView(string viewName)
        {
            return SQLSvrDbi.GetViewData(viewName);
        }

        public override DataTable GetStoredProcedures()
        {
            return SQLSvrDbi.GetStoredProcedures();
        }

        public override DataTable GetStoredProcedure(string procedureName)
        {
            return SQLSvrDbi.GetStoredProcedure(procedureName);
        }

        public override DataTable GetFunctions()
        {
            return SQLSvrDbi.GetFunctions();
        }

        public override DataTable GetFunction(string functionName)
        {
            return SQLSvrDbi.GetFunction(functionName);
        }

        public override DataTable GetTriggers()
        {
            return SQLSvrDbi.GetTriggers();   
        }

        public override DataTable GetTrigger(string triggerName)
        {
            return SQLSvrDbi.GetTrigger(triggerName);
        }
    }
}
