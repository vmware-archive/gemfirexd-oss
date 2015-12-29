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
    /// Simple test to ensure no adverse behavior when attempt is made to commit a transaction
    /// that has not been initiated for a connection
    /// </summary>
    class AttemptToCommitANonActiveTransaction : GFXDTest
    {        
        public AttemptToCommitANonActiveTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(Object context)
        {
            try
            {
                Connection.Commit();

                Command.CommandText = "SELECT * FROM Product";
                DataTable dt = new DataTable();
                DataAdapter.Fill(dt);

                if (dt.Rows.Count <= 0)
                    Fail("Failed to query for table data");
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
