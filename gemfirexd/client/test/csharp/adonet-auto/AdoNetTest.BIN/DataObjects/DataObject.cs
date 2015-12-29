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
using AdoNetTest.BIN.BusinessObjects;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.DataObjects
{
    /// <summary>
    /// Base class for GemFireXD data access
    /// </summary>
    public class DataObject
    {
        private String SchemaName;
        protected GFXDClientConnection Connection;

        public DataObject()
        {
            SchemaName = String.Empty;
        }

        public DataObject(GFXDClientConnection connection)
            : this()
        {
            Connection = connection;
        }

        public DataObject(String schemaName)
        {
            SchemaName = schemaName;
        }

        public DataObject(GFXDClientConnection connection, String schemaName)
        {
            Connection = connection;
            SchemaName = schemaName;
        }

        protected String QualifyTableName(String sql)
        {
            if (String.IsNullOrEmpty(SchemaName))
                return sql;

            foreach (TableName tname in Enum.GetValues(typeof(TableName)))
                if (sql.Contains(tname.ToString()))
                    sql = sql.Replace(tname.ToString(), String.Format("{0}.{1}",
                        SchemaName, tname.ToString()));

            return sql;
        }
    }
}
