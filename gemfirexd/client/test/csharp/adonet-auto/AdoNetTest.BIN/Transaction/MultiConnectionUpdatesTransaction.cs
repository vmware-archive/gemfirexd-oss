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
    /// Verifies update transactions being executed on multple connections from within the 
    /// same client. All transactions should complete successfully and no exception should
    /// occur
    /// </summary>
    class MultiConnectionUpdatesTransaction : GFXDTest
    {
        public MultiConnectionUpdatesTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int updateSize = 5;
            DataTable[] tables = new DataTable[updateSize];
            String[] tableNames = new String[updateSize];
            GFXDClientConnection[] conns = new GFXDClientConnection[updateSize];
            GFXDCommand[] cmds = new GFXDCommand[updateSize];
            GFXDDataAdapter[] adpts = new GFXDDataAdapter[updateSize];

            try
            {
                for (int i = 0; i < updateSize; i++)
                {
                    tableNames[i] = DbRandom.BuildRandomTable(5);
                    conns[i] = new GFXDClientConnection(ConnectionString);
                    cmds[i] = conns[i].CreateCommand();
                    cmds[i].CommandText = String.Format(
                        "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableNames[i]);

                    conns[i].Open();
                    conns[i].AutoCommit = false;
                    conns[i].BeginGFXDTransaction();

                    adpts[i] = cmds[i].CreateDataAdapter();
                    tables[i] = new DataTable();
                    adpts[i].Fill(tables[i]);
                    ParseDataTable(tables[i]);
                }

                IList<object> data = DbRandom.GetRandomRowData();

                for (int i = 0; i < updateSize; i++)
                {
                    CommandBuilder = new GFXDCommandBuilder(adpts[i]);

                    for (int j = 0; j < tables[i].Rows.Count; j++)
                        for (int k = 1; k < tables[i].Columns.Count; k++)
                            tables[i].Rows[j][k] = data[k];

                    if (adpts[i].Update(tables[i]) != tables[i].Rows.Count)
                        Fail(String.Format(
                            "Failed to update table {0}", tableNames[i]));

                    try
                    {
                        conns[i].Commit();
                    }
                    catch (Exception e)
                    {
                        conns[i].Rollback();
                        Fail(e);
                    }
                }

                for (int i = 0; i < updateSize; i++)
                {
                    tables[i].Clear();
                    adpts[i].Fill(tables[i]);             
                }

                foreach (DataTable table in tables)
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
                foreach (String tableName in tableNames)
                    DbRandom.DropTable(tableName);

                base.Run(context);
            }
        }
    }
}
