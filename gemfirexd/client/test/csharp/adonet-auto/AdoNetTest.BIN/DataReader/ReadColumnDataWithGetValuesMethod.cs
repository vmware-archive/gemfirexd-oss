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
    /// Retrieves row data from the GFXDDataReader one row at a time using the 
    /// GetValues() method
    /// </summary>
    class ReadColumnDataWithGetValuesMethod : GFXDTest
    {
        public ReadColumnDataWithGetValuesMethod(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            String tableName = null;
                               
            try
            {
                tableName = DbRandom.BuildRandomTable(10);

                int colCount = DbHelper.GetTableColumnCount(tableName);

                Command.CommandText = "SELECT * FROM " + tableName;

                DataReader = Command.ExecuteReader();

                while (DataReader.Read())
                {
                    StringBuilder row = new StringBuilder();
                    try
                    {
                        object[] data = new object[colCount];
                        int ret = DataReader.GetValues(data);

                        foreach (object o in data)
                        {
                            if (o == null)
                                Fail("GetValues() failed to retrieve the column values");
                            row.Append(o.ToString());
                            row.Append(", ");
                        }

                        Log(row.ToString());
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                }
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbRandom.DropTable(tableName);
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
