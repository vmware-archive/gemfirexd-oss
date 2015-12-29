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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.BusinessObjects;
using AdoNetTest.BIN.BusinessRules;
using AdoNetTest.BIN.DataObjects;

namespace AdoNetTest.BIN.Usability
{
    /// <summary>
    /// Rollback a new order transaction. Expect no exception
    /// </summary>
    class RollbackAllTableInsertTransaction : GFXDTest
    {
        public RollbackAllTableInsertTransaction(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            IList<BusinessObject> bObjects = new List<BusinessObject>();
            DbController dbc = new DbController(Connection);

            try
            {
                Connection.AutoCommit = false;
                Connection.BeginGFXDTransaction();

                Address addr1 = (Address)ObjectFactory.Create(ObjectType.Address);
                bObjects.Add(addr1);

                Supplier supp = (Supplier)ObjectFactory.Create(ObjectType.Supplier);
                supp.Address = addr1;
                bObjects.Add(supp);

                Category cate = (Category)ObjectFactory.Create(ObjectType.Category);
                bObjects.Add(cate);

                Product prod = (Product)ObjectFactory.Create(ObjectType.Product);
                prod.Category = cate;
                bObjects.Add(prod);

                Address addr2 = (Address)ObjectFactory.Create(ObjectType.Address);
                addr2.AddressId = addr1.AddressId + 1;
                bObjects.Add(addr2);

                Customer cust = (Customer)ObjectFactory.Create(ObjectType.Customer);
                cust.Address = addr2;
                bObjects.Add(cust);

                Order order = (Order)ObjectFactory.Create(ObjectType.Order);
                order.Customer = cust;
                bObjects.Add(order);

                OrderDetail ordDetail = (OrderDetail)ObjectFactory.Create(ObjectType.OrderDetail);
                ordDetail.OrderId = order.OrderId;
                ordDetail.ProductId = prod.ProductId;
                bObjects.Add(ordDetail);

                dbc.ProcessNewCustomerOrder(bObjects);

                Connection.Rollback();

                if (dbc.ValidateTransaction(bObjects))
                    Fail("Failed to rollback transaction");
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
