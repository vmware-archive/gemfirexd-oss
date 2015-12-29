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
    /// Simple test to verify filling a DataTable object using the GFXDDataAdapter
    /// </summary>
    class FillDataSetWithASingleTable : GFXDTest
    {        
        public FillDataSetWithASingleTable(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DataSet dataset = new DataSet();
            String tableName = TableName.PRODUCT.ToString();
            Command.CommandText = DbDefault.GetAddressQuery();
            int count = 0;

            try
            {
                DataAdapter = Command.CreateDataAdapter();
                dataset = new DataSet();

                // fill product table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetProductQuery();
                DataAdapter.Fill(dataset, tableName);

                if (dataset.Tables.Count != 1)
                {
                    Fail(String.Format("Tables.Count is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        count, dataset.Tables.Count));
                }
                if (dataset.Tables[0].TableName != tableName)
                {
                    Fail(String.Format("TableName is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        tableName, dataset.Tables[0].TableName));
                }
                if(dataset.Tables[0].Columns.Count <= 0)
                {
                    Fail(String.Format("Table has {0} columns",
                         dataset.Tables[0].Columns.Count));
                }
                if (dataset.Tables[0].Rows.Count <= 0)
                {
                    Fail(String.Format("Table has {0} rows",
                         dataset.Tables[0].Rows.Count));
                }

                ParseDataSet(dataset);
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
