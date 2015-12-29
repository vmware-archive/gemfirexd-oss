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
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace AdoNetTest.BIN.Command
{
    /// <summary>
    /// Verifies support for all JDBC data types in DDL commands 
    /// </summary>
    class CreateTableWithAllSupportedGFXDTypes : GFXDTest
    {
        private String tableName;

        public CreateTableWithAllSupportedGFXDTypes(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            tableName = DbHelper.GetRandomString(10);   

            try
            {   
                Command.CommandText = BuildCommand();
                Command.ExecuteNonQuery();

                Log(String.Format("Table [{0}] created", tableName));
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    Command.CommandText = "DROP TABLE " + tableName;
                    Command.ExecuteNonQuery();

                    Log(String.Format("Table [{0}] dropped", tableName));

                    Command.Close();                    
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }

        private String BuildCommand()
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("CREATE TABLE ");
            sql.Append(tableName);
            sql.Append(" (");
            sql.Append("col_id INT PRIMARY KEY NOT NULL, ");
            sql.Append("col_bigint BIGINT, ");
            sql.Append("col_binary CHAR(5) FOR BIT DATA, ");
            sql.Append("col_blob BLOB, ");
            sql.Append("col_char CHAR, ");
            sql.Append("col_clob CLOB, ");
            sql.Append("col_date DATE, ");
            sql.Append("col_decimal DECIMAL(9,2), ");
            sql.Append("col_double DOUBLE PRECISION, ");
            sql.Append("col_float DOUBLE PRECISION, ");
            sql.Append("col_integer INTEGER, ");
            sql.Append("col_varbinary VARCHAR(10) FOR BIT DATA, ");
            sql.Append("col_longvarbinary LONG VARCHAR FOR BIT DATA, ");
            sql.Append("col_longvarchar LONG VARCHAR, ");
            sql.Append("col_real REAL, ");
            sql.Append("col_smallint SMALLINT, ");
            sql.Append("col_time TIME, ");
            sql.Append("col_timestamp TIMESTAMP, ");
            sql.Append("col_varchar VARCHAR(100))");

            return sql.ToString();
        }
    }
}
