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
using AdoNetTest.BIN.BusinessObjects;

namespace AdoNetTest.BIN.JoinQuery
{
    /// <summary>
    /// GFXDDataAdapter populates a data set with multiple join queries. Expects no exception
    /// and data set's Tables contain correct number of columns specified in each of the join
    /// queries
    /// </summary>
    class DataAdapterFillsDataSetWithMultiJoinQueries : GFXDTest
    {
        public DataAdapterFillsDataSetWithMultiJoinQueries(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                DataSet ds = new DataSet(); 

                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    Relation.PRODUCT_CATEGORY, new long[] { dbc.GetRandomCategory().CategoryId });

                DataAdapter.Fill(ds, "Product_Category");

                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    Relation.ORDER_CUSTOMER, new long[] { dbc.GetRandomCustomer().CustomerId });

                DataAdapter.Fill(ds, "Order_Customer");

                if (ds.Tables["Product_Category"].Columns.Count != 
                    (DbDefault.GetTableStructure(TableName.PRODUCT).Columns.Count
                    + DbDefault.GetTableStructure(TableName.CATEGORY).Columns.Count))
                    Fail("Number of returned columns is incorrect");

                if (ds.Tables["Order_Customer"].Columns.Count !=
                    (DbDefault.GetTableStructure(TableName.ORDERS).Columns.Count
                    + DbDefault.GetTableStructure(TableName.CUSTOMER).Columns.Count))
                    Fail("Number of returned columns is incorrect");
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
