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
    /// Verifies GFXDDataReader's ability to update multiple columns using the UpdateValues()
    /// method. This test is expect to fail due to update implementation has been removed 
    /// from Beta release. This will be re-implemented for GA.
    /// </summary>
    class UpdateReaderDataWithUpdateValuesMethod : GFXDTest
    {
        public UpdateReaderDataWithUpdateValuesMethod(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            String tableName = null;
            object[] newVals;

            try
            {
                tableName = DbRandom.BuildRandomTable(10);

                int colCount = DbHelper.GetTableColumnCount(tableName);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID DESC FETCH FIRST 1 ROWS ONLY", 
                    tableName);

                //Connection.ReadOnly = false;
                // CHANGED Command.ReaderUpdatable = true;

                DataReader = Command.ExecuteReader();
                newVals = DbRandom.GetRandomRowData().ToArray<object>();

                if (!DataReader.Read())
                    Fail("Reader returns no record");    

                // CHANGED DataReader.UpdateValues(newVals);
                // CHANGED DataReader.UpdateRow();

                DataReader = Command.ExecuteReader();
                while (DataReader.Read())
                {
                    for (int i = 0; i < colCount; i++)
                    {
                        object data = DataReader.GetValue(i);
                        if (DataReader.GetValue(i) != newVals[i])
                            Fail(String.Format("UpdateValues() failed to update column {0} value "
                                + "Expected [{1}]; Actual [{2}]",
                                newVals[i].ToString(), data.ToString()));
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
                    DbHelper.DropTable(tableName);
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
