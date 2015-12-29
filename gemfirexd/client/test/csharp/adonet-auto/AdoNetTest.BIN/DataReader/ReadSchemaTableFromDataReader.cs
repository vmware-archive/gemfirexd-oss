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

namespace AdoNetTest.BIN.DataReader
{
    /// <summary>
    /// Retreives schema definition for a table.
    /// </summary>
    class ReadSchemaTableFromDataReader : GFXDTest
    {
        public ReadSchemaTableFromDataReader(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String tableName = null;

            try
            {
                tableName = DbRandom.BuildRandomTable(10);
                int colCount = DbHelper.GetTableColumnCount(tableName);

                Command.CommandText = String.Format("SELECT * FROM {0}", tableName);

                DataTable table = DataReader.GetSchemaTable();

                if (table.Rows.Count != DataReader.FieldCount)
                    Fail(String.Format("GetSchemaTable() returns incorrect number of rows. "
                        + "Expected [{0}]; Actual [{1}]",
                        table.Rows.Count, DataReader.FieldCount));
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbRandom.DropTable(tableName);
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
