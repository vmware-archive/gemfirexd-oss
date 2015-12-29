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
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.DDLOperations
{
    /// <summary>
    /// Create a new database schema running on multiple server groups
    /// </summary>
    class CreateSchemaWithMultipleServerGroups : GFXDTest
    {        
        public CreateSchemaWithMultipleServerGroups(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String schemaName = DbHelper.GetRandomString(8);
            String serverGroup1 = DbHelper.GetRandomString(10);
            String serverGroup2 = DbHelper.GetRandomString(10);
            String tableName = null;

            try
            {
                // create the schema
                Command.CommandText = String.Format("CREATE SCHEMA {0} DEFAULT SERVER GROUPS ({1}, {2})",
                    schemaName, serverGroup1, serverGroup2);
                //Command.CommandText = String.Format("CREATE SCHEMA {0} ", schemaName);
                Command.ExecuteNonQuery();

                // check schema has been correctly created
                String sql = String.Format("SELECT * FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '{0}'",
                    schemaName);

                DataRow row = (DataRow)DataObjects.GFXDDbi.Select(sql, QueryTypes.DATAROW);

                ParseDataRow(row);

                if (row["SCHEMANAME"].ToString() != schemaName)
                    Fail(String.Format("Schema name is incorrectly written. Expected [{0}]; Actual [{1}]",
                        schemaName, row[1].ToString()));

                String[] svrGroups = row["DEFAULTSERVERGROUPS"].ToString().Split(new char[] { ',' });

                if (svrGroups[0] != serverGroup1)
                    Fail(String.Format("Server group is incorrectly written. Expected [{0}]; Actual [{1}]",
                        serverGroup1, svrGroups[0]));
                if (svrGroups[1] != serverGroup2)
                    Fail(String.Format("Server group is incorrectly written. Expected [{0}]; Actual [{1}]",
                        serverGroup2, svrGroups[1]));

                tableName = DbRandom.BuildRandomTable(schemaName, 10);
                Command.CommandText = String.Format("SELECT * FROM {0}.{1}", schemaName, tableName);

                DataTable dt = new DataTable();
                DataAdapter.Fill(dt);

                if (dt.Rows.Count != 10)
                    Fail("Failed to create table on new schema");

                DbHelper.DropTable(String.Format("{0}.{1}", schemaName, tableName));
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    Command.CommandText = "DROP SCHEMA " + schemaName + " RESTRICT";
                    Command.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Log(e);
                }

                base.Run(context);
            }
        }
    }
}
