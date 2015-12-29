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
    /// Creates and verifies order-customer view and view data
    /// </summary>
    class CreateOrderCustomerView : GFXDTest
    {
        public CreateOrderCustomerView(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = null;
            Relation relation = Relation.ORDER_CUSTOMER;

            try
            {
                dbc = new DbController(Connection);
                dbc.CreateView(relation);

                DataTable dt = dbc.GetView(relation.ToString());

                if (dt.Rows.Count <= 0)
                    Fail("Failed to create database view");
                else
                    ParseDataTable(dt);
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                dbc.DropView(relation.ToString());
                base.Run(context);
            }
        }
    }
}
