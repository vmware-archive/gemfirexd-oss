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
    /// Verifies transaction involving DataReader and DataAdapter updates. This test should fail
    /// until the ReaderUpdatable property and all update methods are re-implemented.
    /// </summary>
    class MixedAdapterReaderUpdatesTransaction : GFXDTest
    {
        public MixedAdapterReaderUpdatesTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DataTable table = new DataTable();
            String tableName = null; ;
            try
            {
                Connection.AutoCommit = false;
                Connection.BeginGFXDTransaction(IsolationLevel.Chaos);

                tableName = DbRandom.BuildRandomTable(10);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableName);    

                // Adapter update
                DataAdapter.Fill(table);

                CommandBuilder = new GFXDCommandBuilder(DataAdapter);

                IList<object> adptData = DbRandom.GetRandomRowData();

                for (int i = 0; i < table.Rows.Count; i++)
                    for (int j = 1; j < adptData.Count; j++)  // do not update identity column
                        table.Rows[i][j] = adptData[j];

                int rowsUpdated = DataAdapter.Update(table);

                if (rowsUpdated != table.Rows.Count)
                    Fail(String.Format("Failed to update all changed rows. "
                        + "Expected [{0}]; Actual [{1}]",
                        rowsUpdated, table.Rows.Count));

                // Reader update
                //Connection.ReadOnly = false;
                // CHANGED Command.ReaderUpdatable = true;

                object[] rdrData = DbRandom.GetRandomRowData().ToArray<object>();
                DataReader = Command.ExecuteReader();

                while (DataReader.Read())
                {
                    // CHANGED DataReader.UpdateValues(rdrData);
                }
                // CHANGED DataReader.UpdateRow();

                Connection.Commit();
            }
            catch (Exception e)
            {
                Connection.Rollback();
                Fail(e);
            }
            finally
            {
                DbRandom.DropTable(tableName);
                base.Run(context);
            }
        }
    }
}
