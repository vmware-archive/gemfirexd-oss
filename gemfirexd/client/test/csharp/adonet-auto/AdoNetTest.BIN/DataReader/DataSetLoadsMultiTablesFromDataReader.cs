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
    /// DataSet object loads multiple tables from GFXDDataReader executing a command with 
    /// multple select queries
    /// </summary>
    class DataSetLoadsMultiTablesFromDataReader : GFXDTest
    {
        public DataSetLoadsMultiTablesFromDataReader(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            DbController dbc = new DbController(Connection);
            DataSet ds = new DataSet();

            StringBuilder sql = new StringBuilder();
            sql.AppendFormat("SELECT * FROM {0} ", TableName.PRODUCT.ToString());
            sql.AppendFormat("SELECT * FROM {0} ", TableName.CATEGORY.ToString());
            sql.AppendFormat("SELECT * FROM {0} ", TableName.SUPPLIER.ToString());

            DataTable[] tables = new DataTable[]{ 
                new DataTable(TableName.PRODUCT.ToString()),
                new DataTable(TableName.CATEGORY.ToString()),
                new DataTable(TableName.SUPPLIER.ToString())};
            
            try
            {
                ds.Tables.Add(tables[0]);
                ds.Tables.Add(tables[1]);
                ds.Tables.Add(tables[2]);

                Command.CommandText = sql.ToString();
                DataReader = Command.ExecuteReader();
                
                ds.Load(DataReader, LoadOption.OverwriteChanges, tables);

                if (ds.Tables[TableName.PRODUCT.ToString()].Rows.Count != dbc.GetProductCount())
                    Fail("DataSet failed to load all rows in Product table");
                if (ds.Tables[TableName.CATEGORY.ToString()].Rows.Count != dbc.GetCategoryCount())
                    Fail("DataSet failed to load all rows in Category table");
                if (ds.Tables[TableName.SUPPLIER.ToString()].Rows.Count != dbc.GetSupplierCount())
                    Fail("DataSet failed to load all rows in Supplier table");                
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
