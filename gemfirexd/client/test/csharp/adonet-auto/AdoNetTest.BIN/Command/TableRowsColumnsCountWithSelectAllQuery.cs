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
    /// Verify "SELECT *" correctly returns the number of rows and columns
    /// in the queried table
    /// </summary>
    class TableRowsColumnsCountWithSelectAllQuery : GFXDTest
    {
        public TableRowsColumnsCountWithSelectAllQuery(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
            
        }

        public override void Run(object context)
        {
            DataTable table = new DataTable();
            String tableName = TableName.CUSTOMER.ToString();     

            try
            {                
                Command.CommandText = "SELECT * FROM " + tableName;
                DataAdapter.Fill(table);

                long rowCount = DbHelper.GetTableRowCount(tableName);
                if (table.Rows.Count != rowCount)
                {
                    Fail(String.Format("Query did not return all rows. "
                        + "Expected [{0}]; Actual [{1}]",
                        rowCount, table.Rows.Count));
                }

                long colCount = DbHelper.GetTableColumnCount(tableName);
                if (table.Columns.Count != colCount)
                {
                    Fail(String.Format("Query did not return all columns. "
                        + "Expected [{0}]; Actual [{1}]",
                        colCount, table.Columns.Count));
                }
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
