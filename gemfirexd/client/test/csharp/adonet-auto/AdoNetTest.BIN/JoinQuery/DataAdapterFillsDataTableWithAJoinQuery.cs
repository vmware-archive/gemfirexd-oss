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
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.BusinessObjects;

namespace AdoNetTest.BIN.JoinQuery
{
    /// <summary>
    /// Verifies GFXDDataAdapter returns all columns specified in join query command
    /// </summary>
    class DataAdapterFillsDataTableWithAJoinQuery : GFXDTest
    {
        public DataAdapterFillsDataTableWithAJoinQuery(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);

            try
            {
                Product product = dbc.GetRandomProduct();                
                DataTable table = new DataTable();

                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    Relation.PRODUCT_CATEGORY, new long[] { product.ProductId });
                                
                DataAdapter.Fill(table);

                if (table.Columns.Count != (DbDefault.GetTableStructure(TableName.PRODUCT).Columns.Count
                    + DbDefault.GetTableStructure(TableName.CATEGORY).Columns.Count))
                    Fail("Number of returned columns is incorrect");

                ParseDataTable(table);
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
