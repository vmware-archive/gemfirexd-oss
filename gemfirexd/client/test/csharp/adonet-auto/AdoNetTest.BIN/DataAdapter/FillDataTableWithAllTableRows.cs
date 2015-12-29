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
    /// GFXDDataAdapter retrieves and fills a DataTable with all records contained
    /// within a database table
    /// </summary>
    class FillDataTableWithAllTableRows : GFXDTest
    {
        public FillDataTableWithAllTableRows(ManualResetEvent resetEvent)
            : base(resetEvent)
        {   
        }

        public override void Run(object context)
        {
            DataTable table = new DataTable();
            Command.CommandText = DbDefault.GetOrderDetailQuery();

            try
            {
                DataAdapter.Fill(table);

                if(table.Columns.Count <= 0)
                    Fail(String.Format("Table [{0}] has {1} columns",
                        table.TableName, table.Columns.Count));
                if (table.Rows.Count <= 0)
                    Fail(String.Format("Table [{0}] has {1} rows",
                        table.TableName, table.Rows.Count));

                ParseDataTable(table);
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
