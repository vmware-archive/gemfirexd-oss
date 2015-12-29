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
    /// Retrieve data from the GFXDDataReader one column at a time using the GetString()
    /// method
    /// </summary>
    class ReadColumnDataWithGetStringMethod : GFXDTest
    {        
        public ReadColumnDataWithGetStringMethod(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String tableName = null;

            try
            {
                tableName = DbRandom.BuildRandomTable(5);

                int colCount = DbHelper.GetTableColumnCount(tableName);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableName);

                DataReader = Command.ExecuteReader();

                while (DataReader.Read())
                {                    
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
