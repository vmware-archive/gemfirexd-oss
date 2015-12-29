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

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Simple test verifying the creation of a random table
    /// </summary>
    class VerifyDbRandomCreation : GFXDTest
    {
        public VerifyDbRandomCreation(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String tableName = null;
                      
            try
            {
                tableName = DbRandom.BuildRandomTable(10);

                if (DbHelper.TableExists(tableName))
                {
                    Log(String.Format("Table {0} has been created", tableName));
                }
                else
                {
                    Log(String.Format("Failed to create table {0}", tableName));
                }

                //GFXDClientConnection conn = Helper.OpenNewConnection();
                GFXDCommand cmd = Connection.CreateCommand();
                cmd.CommandText = String.Format("SELECT * FROM {0}", tableName);
                GFXDDataReader rdr = cmd.ExecuteReader();
                int colCount = DbHelper.GetTableColumnCount(tableName);

                while (rdr.Read())
                {
                    StringBuilder row = new StringBuilder();
                    for (int i = 0; i < colCount; i++)
                    {
                        try
                        {
                            String data = rdr.GetString(i);
                            if (data == null && data == String.Empty)
                            {
                                Log("GetString() failed to retrieve column data");
                            }

                            row.Append(data);
                            row.Append(", ");
                        }
                        catch (Exception e)
                        {
                            Log(e.Message);
                        }
                    }

                    Log(row.ToString());
                }
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbHelper.DropTable(tableName);
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }
    }
}
