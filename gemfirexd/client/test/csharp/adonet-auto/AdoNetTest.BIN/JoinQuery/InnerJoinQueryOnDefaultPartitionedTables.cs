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
    /// Performs inner join query on related tables that are created on default
    /// partition
    /// </summary>
    class InnerJoinQueryOnDefaultPartitionedTables : GFXDTest
    {
        public InnerJoinQueryOnDefaultPartitionedTables(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Order order = dbc.GetRandomOrder();
                Product product = dbc.GetRandomProduct();               

                DataSet dataset = new DataSet();

                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    Relation.ORDERDETAIL_ORDER, new long[] { order.OrderId });
                Log(Command.CommandText);
                DataAdapter.Fill(dataset, "OrderDetail_Order");

                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    Relation.ORDERDETAIL_PRODUCT, new long[] { product.ProductId });
                Log(Command.CommandText);
                DataAdapter.Fill(dataset, "OrderDetail_Product");

                if (dataset.Tables["OrderDetail_Order"].Columns.Count
                    != (DbDefault.GetTableStructure(TableName.ORDERDETAIL).Columns.Count
                    + DbDefault.GetTableStructure(TableName.ORDERS).Columns.Count))
                    Fail("Incorrect number of columns returned for joined OrderDetail_Order query");

                if (dataset.Tables["OrderDetail_Product"].Columns.Count
                    != (DbDefault.GetTableStructure(TableName.ORDERDETAIL).Columns.Count
                    + DbDefault.GetTableStructure(TableName.PRODUCT).Columns.Count))
                    Fail("Incorrect number of columns returned for joined OrderDetail_Product query");

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
