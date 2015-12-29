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
    /// Verifies inserting, updating, and deleting a orderdetail record
    /// </summary>
    class ValidateOrderDetailInsertUpdateDelete : GFXDTest
    {
        public ValidateOrderDetailInsertUpdateDelete(ManualResetEvent resetEvent)
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

                OrderDetail ordDetail = (OrderDetail)ObjectFactory.Create(ObjectType.OrderDetail);
                ordDetail.OrderId = order.OrderId;
                ordDetail.ProductId = product.ProductId;
                dbc.AddOrderDetail(ordDetail);

                OrderDetail result = dbc.GetOrderDetail(ordDetail.OrderId, ordDetail.ProductId);
                if (result == null)
                {
                    Fail("Failed to insert new OrderDetail record");
                    return;
                }
                else if (!ordDetail.Equals(result))
                {
                    Fail("Inserted OrderDetail record having inconsistent data");
                    return;
                }

                ordDetail = (OrderDetail)ObjectFactory.Create(ObjectType.OrderDetail);
                ordDetail.OrderId = result.OrderId;
                ordDetail.ProductId = result.ProductId;
                dbc.UpdateOrderDetail(ordDetail);

                result = dbc.GetOrderDetail(ordDetail.OrderId, ordDetail.ProductId);
                if (!ordDetail.Equals(result))
                {
                    Fail("Failed to update OrderDetail record");
                    return;
                }

                dbc.DeleteOrderDetail(ordDetail.OrderId, ordDetail.ProductId);
                if (dbc.GetOrderDetail(ordDetail.OrderId, ordDetail.ProductId) != null)
                    Fail("Failed to delete OrderDetail record");
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
