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
    /// Executes multiple select queries in a single command to retrieve data into multiple
    /// result sets
    /// </summary>
    class ReadTableDataIntoMultipleResultSets : GFXDTest
    {
        public ReadTableDataIntoMultipleResultSets(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            Command.CommandText = String.Format(
                    "SELECT * FROM {0} SELECT * FROM {1} SELECT * FROM {2}",
                    TableName.PRODUCT.ToString(), 
                    TableName.CUSTOMER.ToString(),
                    TableName.ORDERDETAIL.ToString());

            try
            {
                DataReader = Command.ExecuteReader();

                while (DataReader.NextResult())
                {
                    while (DataReader.Read())
                    {
                        int colCount = DbHelper.GetTableColumnCount(TableName.PRODUCT.ToString());
                        StringBuilder row = new StringBuilder();
                        for (int i = 0; i < colCount; i++)
                        {
                            try
                            {
                                String data = DataReader.GetString(i);
                                if (data == null && data == String.Empty)
                                {
                                    Fail("GetString() failed to retrieve column data");
                                }

                                row.Append(data);
                                row.Append(", ");
                            }
                            catch (Exception e)
                            {
                                Fail(e);
                            }
                        }

                        Log(row.ToString());
                    }
                }
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
