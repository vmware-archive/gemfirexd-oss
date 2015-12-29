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
    /// GFXDDataAdapter performs update on multiple data rows within a data
    /// table
    /// </summary>
    class UpdateAnArrayOfChangedDataRows : GFXDTest
    {
        public UpdateAnArrayOfChangedDataRows(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DataTable table = new DataTable();
            String tableName = null;        

            try
            {
                tableName = DbRandom.BuildRandomTable(5);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableName);    

                DataAdapter.Fill(table);

                ParseDataTable(table);

                CommandBuilder = new GFXDCommandBuilder(DataAdapter);

                DataRow[] rows = new DataRow[table.Rows.Count];
                for (int i = 0; i < table.Rows.Count; i++)
                    rows[i] = table.Rows[i];

                IList<object> data = DbRandom.GetRandomRowData();
                for (int j = 0; j < rows.Length; j++)
                    for (int k = 1; k < table.Columns.Count; k++) // do not update identity column
                        rows[j][k] = data[k];
                
                int rowsUpdated = DataAdapter.Update(rows);

                if (rowsUpdated != table.Rows.Count)
                {
                    Fail(String.Format("Failed to update all changed rows. "
                        + "Expected [{0}]; Actual [{1}]",
                        rowsUpdated, table.Rows.Count));
                }

                table.Clear();

                DataAdapter.Fill(table);

                ParseDataTable(table);

                foreach (DataRow row in table.Rows)
                {
                    for (int i = 1; i < row.Table.Columns.Count; i++)
                    {
                        if (!DbRandom.Compare(data[i], row, i))
                        {
                            Fail(String.Format("Inconsistent updated data at row [{0}] column [{1}]. "
                                    + "Expected [{2}]; Actual [{3}]",
                                    row[0].ToString(), row.Table.Columns[i].ColumnName,
                                    data[i].ToString(), row[i].ToString()));
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
