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
using GemFireXDDBI.DBI;
using GemFireXDDBI.Util;
using GemFireXDDBI.DBMigrate;

namespace GemFireXDDBI.DBObjects
{
    /// <summary>
    /// Represents database index defined attributes
    /// </summary>
    class DbIndex
    {
        public String SchemaName { get; set; }
        public String TableName { get; set; }
        public String IndexName { get; set; }
        public String ColumnName { get; set; }
        public Boolean IsUnique { get; set; }
        public Boolean Created { get; set; }

        public DbIndex()
            : this(String.Empty, String.Empty, String.Empty, String.Empty, false)
        {
        }

        public DbIndex(String schemaName, String tableName, String indexName, String columnName, bool isUnique)
        {
            this.SchemaName = schemaName.ToUpper();
            this.TableName = tableName.ToUpper();
            this.IndexName = indexName.ToUpper();
            this.ColumnName = columnName.ToUpper();
            this.IsUnique = isUnique;
        }

        public void Migrate()
        {
            try
            {
                if (!GemFireXDDbi.IndexExists(IndexName) && !Created)
                {
                    Migrator.OnMigrateEvent(new MigrateEventArgs(
                            Result.Unknown, String.Format("Migrating index {0}...", IndexName)));

                    Util.Helper.Log(GetCreateIndexSql());
                    GemFireXDDbi.Create(GetCreateIndexSql());
                    Util.Helper.Log(String.Format("Index {0} created", IndexName));

                    this.Created = true;
                }
                else
                    Util.Helper.Log(String.Format("Index {0} already exists", IndexName));
                
            }
            catch (Exception e)
            {
                Migrator.Errorred = true;
                Util.Helper.Log(e);
            }
        }

        private String GetCreateIndexSql()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("CREATE ");
            if (IsUnique)
                sql.Append("UNIQUE ");

            sql.AppendFormat("INDEX {0} ON {1}.{2} ({3} ASC)", IndexName, SchemaName, TableName, ColumnName);

            return sql.ToString();
        }
    }
}
