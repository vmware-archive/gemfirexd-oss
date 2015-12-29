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
    /// Create a new database schema with a specified default server group
    /// </summary>
    class CreateSchemaWithADefaultServerGroup : GFXDTest
    {
        public CreateSchemaWithADefaultServerGroup(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String schemaName = DbHelper.GetRandomString(8);
            String serverGroup = DbHelper.GetRandomString(10);
                    
            try
            {
                // create the schema
                Command.CommandText = String.Format("CREATE SCHEMA {0} DEFAULT SERVER GROUPS ({1})",
                    schemaName, serverGroup);
                Command.ExecuteNonQuery();

                // check schema has been correctly created
                String sql = String.Format("SELECT * FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '{0}'",
                    schemaName);
                DataRow row = (DataRow)DataObjects.GFXDDbi.Select(sql, QueryTypes.DATAROW);

                ParseDataRow(row);

                if (row["SCHEMANAME"].ToString() != schemaName)
                    Fail(String.Format("Schema name is incorrectly written. Expected [{0}]; Actual [{1}]",
                        schemaName, row[1].ToString()));
                if (row["DEFAULTSERVERGROUPS"].ToString() != serverGroup)
                    Fail(String.Format("Server group is incorrectly written. Expected [{0}]; Actual [{1}]",
                        serverGroup, row[3].ToString()));
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
