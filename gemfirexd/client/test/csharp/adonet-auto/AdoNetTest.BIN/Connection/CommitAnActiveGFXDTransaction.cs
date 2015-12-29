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
    /// Initiates and commit GFXDTransaction that create, insert, and delete from
    /// a table. Expect no exception and all operations to be successful
    /// </summary>
    class CommitAnActiveGFXDTransaction : GFXDTest
    {        
        public CommitAnActiveGFXDTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(Object context)
        {
            GFXDTransaction tx = null;
            String tableName = null;
            int numRows = 10;
            try
            {
                tx = Connection.BeginTransaction(IsolationLevel.ReadCommitted);

                tableName = DbRandom.BuildRandomTable(numRows);
                Command.CommandText = "SELECT * FROM " + tableName;

                DataTable dt = new DataTable();
                DataAdapter.Fill(dt);
                if(dt.Rows.Count != 10)
                {
                    Fail("Transaction failed to create table");
                    return;
                }

                DbHelper.ExecuteNonQueryStatement(Connection, "DELETE FROM " + tableName);

                tx.Commit();

                dt.Clear();
                DataAdapter.Fill(dt);
                if (dt.Rows.Count != 0)
                    Fail("Transaction failed to delete all table records");
            }
            catch (Exception e)
            {
                Fail(e);
                tx.Rollback();
            }
            finally
            {
                DbHelper.DropTable(tableName);
                base.Run(context);
            }
        }
    }
}
