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
    /// GFXDDataAdapter retrieves and fill a data set with all records from multple tables 
    /// </summary>
    class FillDataSetWithMultipleTables : GFXDTest
    {
        public FillDataSetWithMultipleTables(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            DataSet dataset = new DataSet();
            Command.CommandText = DbDefault.GetAddressQuery();
            int count = 0;

            try
            {
                // fill address table
                DataAdapter.Fill(dataset, TableName.ADDRESS.ToString());
                count++;

                // fill supplier table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetSupplierQuery();
                DataAdapter.Fill(dataset, TableName.SUPPLIER.ToString());
                count++;

                // fill category table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetCategoryQuery();
                DataAdapter.Fill(dataset, TableName.CATEGORY.ToString());
                count++;

                // fill product table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetProductQuery();
                DataAdapter.Fill(dataset, TableName.PRODUCT.ToString());
                count++;

                // fill customer table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetCustomerQuery();
                DataAdapter.Fill(dataset, TableName.CUSTOMER.ToString());
                count++;

                // fill order table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetOrderQuery();
                DataAdapter.Fill(dataset, TableName.ORDERS.ToString());
                count++;

                // fill orderdetail table
                DataAdapter.SelectCommand.CommandText = DbDefault.GetOrderDetailQuery();
                DataAdapter.Fill(dataset, TableName.ORDERDETAIL.ToString());
                count++;

                if (dataset.Tables.Count != count)
                {
                    Fail(String.Format("Tables.Count is incorrect. "
                        + "Expected {0}; Actual {1}",
                        count, dataset.Tables.Count));
                }

                foreach (DataTable table in dataset.Tables)
                {
                    if(table.Columns.Count <= 0)
                    {
                        Fail(String.Format("Table [{0}] has {1} columns",
                            table.TableName, table.Columns.Count));
                    }
                    if (table.Rows.Count <= 0)
                    {
                        Fail(String.Format("Table [{0}] has {1} rows",
                            table.TableName, table.Rows.Count));
                    }
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
