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
    /// Verifies inserting, updating, and deleting a order record
    /// </summary>
    class ValidateOrderInsertUpdateDelete : GFXDTest
    {
        public ValidateOrderInsertUpdateDelete(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Customer customer = dbc.GetRandomCustomer();

                Log(customer.ToString());

                Order order = (Order)ObjectFactory.Create(ObjectType.Order);
                order.Customer = customer;

                Log(order.ToString());

                dbc.AddOrder(order);

                Order result = dbc.GetOrder(order.OrderId);
                if (result == null)
                {
                    Fail("Failed to insert new Order record");
                    return;
                }
                else if (!order.Equals(result))
                {
                    Fail("Inserted Order record having inconsistent data");
                    return;
                }

                order = (Order)ObjectFactory.Create(ObjectType.Order);
                order.OrderId = result.OrderId;
                order.Customer = customer;
                dbc.UpdateOrder(order);

                result = dbc.GetOrder(order.OrderId);
                if (!order.Equals(result))
                {
                    Fail("Failed to update Order record");
                    return;
                }

                dbc.DeleteOrder(order.OrderId);
                if (dbc.GetOrder(order.OrderId) != null)
                    Fail("Failed to delete Order record");
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
