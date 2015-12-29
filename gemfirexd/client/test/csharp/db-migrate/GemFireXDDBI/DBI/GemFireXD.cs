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
    /// Facade for GemFireXD client interface
    /// </summary>
    class GemFireXD : SQLBase
    {
        public GemFireXD(String connString)
        {
            GemFireXDDbi.DbConnectionString = connString;
        }

        public override int TestConnection()
        {
            return GemFireXDDbi.TestConnection();
        }

        public override DataTable GetTableNames()
        {
            return GemFireXDDbi.GetTableNames();
        }

        public override DataTable GetTableData(string tableName)
        {
            return GemFireXDDbi.GetTableData(tableName);
        }

        public override DataTable GetViews()
        {
            return GemFireXDDbi.GetViews();
        }

        public override DataTable GetView(string viewName)
        {
            return GemFireXDDbi.GetViewData(viewName);
        }

        public override DataTable GetViewData(string viewName)
        {
            return GemFireXDDbi.GetViewData(viewName);
        }

        public override DataTable GetIndexes()
        {
            return GemFireXDDbi.GetIndexes();
        }
        public override DataTable GetIndex(string indexName)
        {
            return GemFireXDDbi.GetIndex(indexName);
        }

        public override DataTable GetStoredProcedures()
        {
            return GemFireXDDbi.GetStoredProcedures();
        }

        public override DataTable GetStoredProcedure(string procedureName)
        {
            return GemFireXDDbi.GetStoredProcedure(procedureName);
        }

        public override DataTable GetFunctions()
        {
            return GemFireXDDbi.GetFunctions();
        }

        public override DataTable GetFunction(string functionName)
        {
            return GemFireXDDbi.GetFunction(functionName);
        }

        public override DataTable GetTriggers()
        {
            return GemFireXDDbi.GetTriggers();
        }

        public override DataTable GetTrigger(string triggerName)
        {
            return GemFireXDDbi.GetTrigger(triggerName);
        }
    }
}
