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

namespace AdoNetTest.BIN.Connection
{
    /// <summary>
    /// Creates multiple GFXDCommand objects from the connection object and execute queries
    /// on each of the commands. Expects no exception and all operations succeeded
    /// </summary>
    class CreateMultipleCommandsFromConnection : GFXDTest
    {
        public CreateMultipleCommandsFromConnection(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(Object context)
        {
            int numCommands = 10;
            GFXDCommand[] commands = new GFXDCommand[numCommands];

            try
            {
                int rowCount = DbHelper.GetTableRowCount("Product");

                for (int i = 0; i < numCommands; i++)
                {
                    commands[i] = Connection.CreateCommand();
                    commands[i].CommandText = "SELECT * FROM Product";

                    if (commands[i] == null)
                        Fail("Failed to create GFXDCommand from Connection object.");

                    DataTable dt = new DataTable();
                    GFXDDataAdapter adpt = commands[i].CreateDataAdapter();
                    adpt.Fill(dt);

                    if (dt.Rows.Count != rowCount)
                        Fail("Failed to retrieve all records from table");                    
                }
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {                
                base.Run(context);
            }
        }
    }
}