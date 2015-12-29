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
    /// Verifies inserting, updating, and deleting an address record
    /// </summary>
    class ValidateAddressInsertUpdateDelete : GFXDTest
    {
        public ValidateAddressInsertUpdateDelete(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Address address = (Address)ObjectFactory.Create(ObjectType.Address);
                dbc.AddAddress(address);

                Address result = dbc.GetAddress(address.AddressId);
                if (result == null)
                {
                    Fail("Failed to insert new Address record");
                    return;
                }
                else if (!address.Equals(result))
                {
                    Fail("Inserted Address record having inconsistent data");
                    return;
                }

                address = (Address)ObjectFactory.Create(ObjectType.Address);
                address.AddressId = result.AddressId;
                dbc.UpdateAddress(address);

                result = dbc.GetAddress(address.AddressId);
                if (!address.Equals(result))
                {
                    Fail("Failed to update Address record");
                    return;
                }

                dbc.DeleteAddress(address.AddressId);
                if (dbc.GetAddress(address.AddressId) != null)
                    Fail("Failed to delete Address record");
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
