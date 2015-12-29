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
    /// Verifies inserting, updating, and deleting a supplier record
    /// </summary>
    class ValidateSupplierInsertUpdateDelete : GFXDTest
    {
        public ValidateSupplierInsertUpdateDelete(ManualResetEvent resetEvent)
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

                Supplier supplier = (Supplier)ObjectFactory.Create(ObjectType.Supplier);
                supplier.Address = address;
                dbc.AddSupplier(supplier);

                Supplier result = dbc.GetSupplier(supplier.SupplierId);
                if (result == null)
                {
                    Fail("Failed to insert new Supplier record");
                    return;
                }
                else if (!supplier.Equals(result))
                {
                    Fail("Inserted Supplier record having inconsistent data");
                    return;
                }

                supplier = (Supplier)ObjectFactory.Create(ObjectType.Supplier);
                supplier.SupplierId = result.SupplierId;
                dbc.UpdateSupplier(supplier);

                result = dbc.GetSupplier(supplier.SupplierId);
                if (!supplier.Equals(result))
                {
                    Fail("Failed to update Supplier record");
                    return;
                }

                dbc.DeleteSupplier(supplier.SupplierId);
                if (dbc.GetSupplier(supplier.SupplierId) != null)
                    Fail("Failed to delete Supplier record");
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
