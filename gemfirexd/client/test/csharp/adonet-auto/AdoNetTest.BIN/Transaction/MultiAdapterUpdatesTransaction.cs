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
    /// Perfrom multiple update on a DataAdapter within the same transaction
    /// </summary>
    class MultiAdapterUpdatesTransaction : GFXDTest
    {
        public MultiAdapterUpdatesTransaction(ManualResetEvent resetEvent)
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

                IList<object> data = DbRandom.GetRandomRowData();

                for (int i = 0; i < updateSize; i++)
                {
                    tableNames[i] = DbRandom.BuildRandomTable(5);

                    Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableNames[i]);

                    DataAdapter = Command.CreateDataAdapter();
                    tables[i] = new DataTable();
                    DataAdapter.Fill(tables[i]);

                    CommandBuilder = new GFXDCommandBuilder(DataAdapter);

                    for (int j = 0; j < tables[i].Rows.Count; j++)
                        for (int k = 1; k < tables[i].Columns.Count; k++)
                            tables[i].Rows[j][k] = data[k];

                    if (DataAdapter.Update(tables[i]) != tables[i].Rows.Count)
                        Fail(String.Format(
                            "Failed to update table {0}", tableNames[i]));
                }

                Connection.Commit();
            }
            catch (Exception e)
            {
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
