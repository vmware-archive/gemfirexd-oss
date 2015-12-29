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
    /// Verifies GFXDDataReader's ability to update a column using the UpdateValue()
    /// method. This test is expect to fail due to update implementation has been removed 
    /// from Beta release. This will be re-implemented for GA.
    /// </summary>
    class UpdateReaderDataWithUpdateValueMethod : GFXDTest
    {        
        public UpdateReaderDataWithUpdateValueMethod(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            String tableName = null;
            object[] newVals;

            try
            {
                int colCount = DbRandom.GetRandomTableColumnCount();

                tableName = DbRandom.BuildRandomTable(10);                
                //Connection.ReadOnly = false;

                // CHANGED Command.ReaderUpdatable = true;
                Command.ReaderLockForUpdate = true;
                
                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID DESC FETCH FIRST 1 ROWS ONLY", 
                    tableName);                           
                

                DataReader = Command.ExecuteReader();
               
                ReadData(DataReader);

                newVals = DbRandom.GetRandomRowData().ToArray<object>();
                newVals[0] = 111111111;

                // CHANGED Command.ReaderUpdatable = true;

                while (DataReader.Read())
                {
                    // CHANGED DataReader.UpdateValues(newVals);
                    // CHANGED DataReader.UpdateRow();

                    
                //    for (int i = 0; i < colCount; i++)
                //        DataReader.UpdateValue(i, newVals[i]);
                }

                //for (int i = 0; i < colCount; i++)
                //{
                //    //DataReader.UpdateValue(i, newVals[i]);
                    
                //}

               

                

                DataReader = Command.ExecuteReader();
                ReadData(DataReader);

                

                //while (DataReader.Read())
                //{
                //    for (int i = 0; i < colCount; i++)
                //    {
                //        object data = DataReader.GetValue(i);
                //        if (DataReader.GetValue(i) != newVals[i])
                //        {
                //            Fail(String.Format("UpdateValues() failed to update column {0} value "
                //                + "Expected [{1}]; Actual [{2}]",
                //                i, newVals[i].ToString(), data.ToString()));
                //        }
                //    }
                //}
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbHelper.DropTable(tableName);
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }

        private void ReadData(GFXDDataReader reader)
        {
            while (reader.Read())
            {
                StringBuilder row = new StringBuilder();
                try
                {
                    object[] data = new object[DbRandom.GetRandomTableColumnCount()];
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
    }
}
