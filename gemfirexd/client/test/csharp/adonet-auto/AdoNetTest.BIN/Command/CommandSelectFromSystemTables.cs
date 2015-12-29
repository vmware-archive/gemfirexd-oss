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
    /// Verifies GFXDCommand can execute a query against any of the systems
    /// tables
    /// </summary>
    class CommandSelectFromSystemTables : GFXDTest
    {
        public CommandSelectFromSystemTables(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            try
            {
                Command.CommandText = "SELECT * FROM SYS.SYSCOLUMNS";
                DataReader = Command.ExecuteReader();
                if (DataReader == null || DataReader.FieldCount <= 0)
                    Fail("Failed to query SYS.SYSCOLUMNS table");
                else
                    ParseDataReader(DataReader);

                Command.CommandText = "SELECT * FROM SYS.SYSTABLES";
                DataReader = Command.ExecuteReader();
                ParseDataReader(DataReader);
                if (DataReader == null || DataReader.FieldCount <= 0)
                    Fail("Failed to query SYS.SYSTABLES table");
                else
                    ParseDataReader(DataReader);

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
