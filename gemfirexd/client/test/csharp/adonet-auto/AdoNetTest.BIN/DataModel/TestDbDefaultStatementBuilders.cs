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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.BusinessObjects;

namespace AdoNetTest.BIN.DataModel
{
    class TestDbDefaultStatementBuilders : GFXDTest
    {
        public TestDbDefaultStatementBuilders(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String schemaName = "ADONETTEST";
            try
            {
                DbDefault.CreateDB(schemaName, DbCreateType.PKPartition);

                DbController dbc = new DbController(Connection, schemaName);

                IList<Address> objList1 = dbc.GetAddresses();
                foreach (Address obj in objList1)
                    Log(obj.ToString());

                IList<Supplier> objList2 = dbc.GetSuppliers();
                foreach (Supplier obj in objList2)
                    Log(obj.ToString());

                IList<Category> objList3 = dbc.GetCategories();
                foreach (Category obj in objList3)
                    Log(obj.ToString());

                IList<Product> objList4 = dbc.GetProducts();
                foreach (Product obj in objList4)
                    Log(obj.ToString());

                IList<Customer> objList5 = dbc.GetCustomers();
                foreach (Customer obj in objList5)
                    Log(obj.ToString());

                IList<Order> objList6 = dbc.GetOrders();
                foreach (Order obj in objList6)
                    Log(obj.ToString());

                IList<OrderDetail> objList7 = dbc.GetOrderDetails();
                foreach (OrderDetail obj in objList7)
                    Log(obj.ToString());
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                DbDefault.DropDB(schemaName);
                base.Run(context);
            }
        }
    }
}
