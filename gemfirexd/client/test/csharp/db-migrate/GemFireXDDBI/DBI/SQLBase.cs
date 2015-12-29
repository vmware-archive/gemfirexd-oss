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
    /// Base class for database access interface
    /// </summary>
    abstract class SQLBase
    {
        public abstract int TestConnection();
        public abstract DataTable GetTableNames();
        public abstract DataTable GetTableData(String tableName);
        public abstract DataTable GetViews();
        public abstract DataTable GetView(string viewName);
        public abstract DataTable GetViewData(string viewName);
        public abstract DataTable GetIndexes();
        public abstract DataTable GetIndex(string indexName);
        public abstract DataTable GetStoredProcedures();
        public abstract DataTable GetStoredProcedure(string procedureName);
        public abstract DataTable GetFunctions();
        public abstract DataTable GetFunction(string functionName);
        public abstract DataTable GetTriggers();
        public abstract DataTable GetTrigger(string triggerName);
    }
}
