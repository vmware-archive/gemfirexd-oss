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

namespace AdoNetTest.BIN.Transaction
{
    /// <summary>
    /// Verifies transaction involving DataReader updates. This test should fail
    /// until the ReaderUpdatable property and all update methods are re-implemented.
    /// </summary>
    class MultiDataReaderUpdatesTransaction : GFXDTest
    {
        public MultiDataReaderUpdatesTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int updateSize = 5;
            DataTable[] tables = new DataTable[updateSize];
            String[] tableNames = new String[updateSize];

            try
            {
                Connection.AutoCommit = false;
                Connection.BeginGFXDTransaction(IsolationLevel.Chaos);

                //Connection.ReadOnly = false;
                // CHANGED Command.ReaderUpdatable = true;

                object[] data = DbRandom.GetRandomRowData().ToArray<object>();

                for (int i = 0; i < updateSize; i++)
                {
                    tableNames[i] = DbRandom.BuildRandomTable(5);

                    Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableNames[i]);

                    DataReader = Command.ExecuteReader();

                    while (DataReader.Read())
                    {
                        // CHANGED DataReader.UpdateValues(data);
                    }

                    // CHANGED DataReader.UpdateRow();
                }
                Connection.Commit();

                for (int i = 0; i < updateSize; i++)
                {
                    Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableNames[i]);

                    DataReader = Command.ExecuteReader();

                    while (DataReader.Read())
                    {
                        for (int j = 1; j < DataReader.FieldCount; j++)
                        {
                            if (DataReader.GetString(j) != data[j].ToString())
                                Fail("Failed to commit DataReader update transaction.");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Connection.Rollback();
                Fail(e);
            }
            finally
            {
                foreach (String tableName in tableNames)
                    DbRandom.DropTable(tableName);

                base.Run(context);
            }
        }
    }
}
