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

namespace AdoNetTest.BIN.DataReader
{
    /// <summary>
    /// Populates a DataTable object with data from GFXDDataReader object
    /// </summary>
    class DataTableLoadsDataFromDataReader : GFXDTest
    {
        public DataTableLoadsDataFromDataReader(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);
            DataTable table = new DataTable();

            try
            {
                Command.CommandText = String.Format(
                    "SELECT * FROM {0}", TableName.PRODUCT.ToString());

                DataReader = Command.ExecuteReader();
                table.Load(DataReader, LoadOption.OverwriteChanges);

                if (table.Rows.Count != dbc.GetProductCount())
                    Fail("DataTable failed to load all rows from Product table");
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {                    
                    Command.Close();
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }        
    }
}
