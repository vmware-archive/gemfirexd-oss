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
    /// Verifies inserting, updating, and deleting a category record
    /// </summary>
    class ValidateCategoryInsertUpdateDelete : GFXDTest
    {
        public ValidateCategoryInsertUpdateDelete(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Category category = (Category)ObjectFactory.Create(ObjectType.Category);
                dbc.AddCategory(category);

                Category result = dbc.GetCategory(category.CategoryId);
                if (result == null)
                {
                    Fail("Failed to insert new Category record");
                    return;
                }
                else if (!category.Equals(result))
                {
                    Fail("Inserted Category record having inconsistent data");
                    return;
                }

                category = (Category)ObjectFactory.Create(ObjectType.Category);
                category.CategoryId = result.CategoryId;
                dbc.UpdateCategory(category);

                result = dbc.GetCategory(category.CategoryId);
                if (!category.Equals(result))
                {
                    Fail("Failed to update Category record");
                    return;
                }

                dbc.DeleteCategory(category.CategoryId);
                if (dbc.GetCategory(category.CategoryId) != null)
                    Fail("Failed to delete Category record");
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
