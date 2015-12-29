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

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Represents default test database basic definition
    /// </summary>
    class DbTable
    {
        public String TableName { get; set; }
        public IList<String> PKColumns { get; set; }
        public IList<String> FKColumns { get; set; }
        public IList<DbField> Columns { get; set; }
        public IList<String> Constraints { get; set; }
        public long StartId { get; set; }
        private static long nextId;

        public DbTable(String tableName, IList<String> pkColumns, IList<String> fkColumns, 
            IList<DbField> columns, List<String> constraints, long startId)
        {
            TableName = tableName;
            PKColumns = pkColumns;
            FKColumns = fkColumns;
            Columns = columns;
            Constraints = constraints;
            StartId = startId;
            nextId = startId;
        }

        public long NextId()
        {
            return nextId++;
        }
    }
}
