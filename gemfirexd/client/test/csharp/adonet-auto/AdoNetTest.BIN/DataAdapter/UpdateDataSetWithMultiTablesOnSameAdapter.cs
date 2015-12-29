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

namespace AdoNetTest.BIN.DataAdapter
{
    /// <summary>
    /// GFXDDataAdapter attempts to update multiple changed table within a data set.
    /// This test current
    /// </summary>
    class UpdateDataSetWithMultiTablesOnSameAdapter : GFXDTest
    {
        public UpdateDataSetWithMultiTablesOnSameAdapter(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int numTables = 1;
            DataSet dataset = new DataSet();
            String[] tableNames = new String[numTables];
            StringBuilder sql = new StringBuilder();

            try
            {
                for (int i = 0; i < numTables; i++)
                    tableNames[i] = DbRandom.BuildRandomTable(10);

                for (int i = 0; i < numTables; i++)
                {
                    Command.CommandText = String.Format("SELECT * FROM {0} ORDER BY COL_ID ASC", tableNames[i]);
                    DataAdapter = Command.CreateDataAdapter();
                    DataAdapter.Fill(dataset, tableNames[i]);
                }

                ParseDataSet(dataset);

                IList<object> data = DbRandom.GetRandomRowData();

                for (int i = 0; i < numTables; i++)
                {
                    Command.CommandText = String.Format(
                        "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableNames[i]);

                    DataAdapter = Command.CreateDataAdapter();
                    CommandBuilder = new GFXDCommandBuilder(DataAdapter);

                    for (int j = 0; j < dataset.Tables[tableNames[i]].Rows.Count; j++)
                        for (int k = 1; k < dataset.Tables[tableNames[i]].Columns.Count; k++) // do not update identity column
                            dataset.Tables[tableNames[i]].Rows[j][k] = data[k];

                    if (DataAdapter.Update(dataset, tableNames[i]) != dataset.Tables[tableNames[i]].Rows.Count)
                        Fail(String.Format(
                            "Failed to update all changed rows in table {0}", tableNames[i]));

                }

                dataset.Clear();

                foreach (String tableName in tableNames)
                {
                    Command.CommandText = String.Format(
                        "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableName);

                    DataAdapter.SelectCommand = Command;
                    DataAdapter.Fill(dataset, tableName);
                }

                ParseDataSet(dataset);

                foreach (DataTable table in dataset.Tables)
                {
                    foreach (DataRow row in table.Rows)
                    {
                        for (int i = 1; i < row.Table.Columns.Count; i++)
                        {
                            if (!DbRandom.Compare(data[i], row, i))
                            {
                                Fail(String.Format(
                                    "Inconsistent updated data in table [{0}] at row [{1}] column [{2}]. "
                                    + "Expected [{3}]; Actual [{4}]",
                                    table.TableName,
                                    row[0].ToString(), row.Table.Columns[i].ColumnName,
                                    data[i].ToString(), row[i].ToString()));
                            }
                        }
                    }
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
                    foreach(String tableName in tableNames)
                        DbRandom.DropTable(tableName);
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
