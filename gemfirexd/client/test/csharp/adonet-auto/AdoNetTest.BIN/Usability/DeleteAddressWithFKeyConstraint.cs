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
    /// Verfies deleting address records that are bound by a foreign key constraint 
    /// with supplier and customer records. Expects a contraint violation exception
    /// </summary>
    class DeleteAddressWithFKeyConstraint : GFXDTest
    {
        public DeleteAddressWithFKeyConstraint(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);
            try
            {
                dbc.DeleteAddress(dbc.GetRandomCustomer().Address.AddressId);
                Fail("Customer constraint violation exception should have occurred");

                dbc.DeleteAddress(dbc.GetRandomSupplier().Address.AddressId);
                Fail("Supplier constraint violation exception should have occurred");
            }
            catch (Exception e)
            {
                Log(e);
            }
            finally
            {
                base.Run(context);
            }
        }
    }
}
