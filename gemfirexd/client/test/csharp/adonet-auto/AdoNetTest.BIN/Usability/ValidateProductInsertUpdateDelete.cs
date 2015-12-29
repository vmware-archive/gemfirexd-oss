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
    /// Verifies inserting, updating, and deleting product record
    /// </summary>
    class ValidateProductInsertUpdateDelete : GFXDTest
    {
        public ValidateProductInsertUpdateDelete(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Category category = dbc.GetRandomCategory();
                Supplier supplier = dbc.GetRandomSupplier();

                Product product = (Product)ObjectFactory.Create(ObjectType.Product);
                product.Category = category;
                product.Supplier = supplier;
                dbc.AddProduct(product);

                Product result = dbc.GetProduct(product.ProductId);
                if (result == null)
                {
                    Fail("Failed to insert new Product record");
                    return;
                }
                else if (!product.Equals(result))
                {
                    Fail("Inserted Product record having inconsistent data");
                    return;
                }

                product = (Product)ObjectFactory.Create(ObjectType.Product);
                product.ProductId = result.ProductId;
                dbc.UpdateProduct(product);

                result = dbc.GetProduct(product.ProductId);
                if (!product.Equals(result))
                    Fail("Failed to update Product record");

                dbc.DeleteProduct(product.ProductId);
                if (dbc.GetProduct(product.ProductId) != null)
                    Fail("Failed to delete Product record");
                
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
