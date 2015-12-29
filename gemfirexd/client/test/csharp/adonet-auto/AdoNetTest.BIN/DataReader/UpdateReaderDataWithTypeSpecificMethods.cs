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
    class UpdateReaderDataWithTypeSpecificMethods : GFXDTest
    {
        public UpdateReaderDataWithTypeSpecificMethods(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String tableName = null;

            long col_bigint = DbHelper.GetRandomNumber(7);
            byte[] col_binary = DbHelper.ConvertToBytes(DbHelper.GetRandomString(5));
            byte[] col_blob = DbHelper.ConvertToBytes(DbHelper.GetRandomString(500));
            char col_char = DbHelper.GetRandomChar();
            string col_clob = DbHelper.GetRandomString(100);
            DateTime col_date = DateTime.Today.AddDays(1.00);
            decimal col_decimal = (decimal)(DbHelper.GetRandomNumber(4) + 4.99);
            double col_double = DbHelper.GetRandomNumber(3) + 5.99;
            float col_float = (float)(DbHelper.GetRandomNumber(2) + 6.9999);
            int col_integer = DbHelper.GetRandomNumber(4);
            byte[] col_varbinary = DbHelper.ConvertToBytes(DbHelper.GetRandomString(10));
            byte[] col_longvarbinary = DbHelper.ConvertToBytes(DbHelper.GetRandomString(100));
            string col_longvarchar = DbHelper.GetRandomString(100);
            float col_real = (float)(DbHelper.GetRandomNumber(5) / 3 + 0.3145364758);
            short col_smallint = (short)DbHelper.GetRandomNumber(3);
            DateTime col_time = DateTime.Now.AddHours(1.00);
            DateTime col_timestamp = DateTime.Now.AddDays(1.00);
            string col_varchar = DbHelper.GetRandomString(100);

            try
            {
                tableName = DbRandom.BuildRandomTable(10);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID DESC FETCH FIRST 1 ROWS ONLY", 
                    tableName);

                int colCount = DbHelper.GetTableColumnCount(tableName);

                //Connection.ReadOnly = false;
                // CHANGED Command.ReaderUpdatable = true;

                DataReader = Command.ExecuteReader();
            
                while (DataReader.Read())
                {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i < colCount; i++)
                    {
                        switch (DataReader.GetDataTypeName(i))
                        {
                            case "INTEGER":
                                row.Append(DataReader.GetInt32(i));
                                
                                break;
                            case "BIGINT":
                                row.Append(DataReader.GetInt64(i));
                                break;
                            case "CHAR":
                                row.Append(DataReader.GetChar(i));
                                break;
                            case "DATE":
                                row.Append(DataReader.GetDateTime(i));
                                break;
                            case "DECIMAL":
                                row.Append(DataReader.GetDecimal(i));
                                break;
                            case "DOUBLE":
                                row.Append(DataReader.GetDouble(i));
                                break;
                            case "LONG VARCHAR":
                                row.Append(DataReader.GetChars(i, 0, 0));
                                break;
                            case "REAL":
                                row.Append(DataReader.GetDecimal(i));
                                break;
                            case "SMALLINT":
                                row.Append(DataReader.GetInt16(i));
                                break;
                            case "TIME":
                                row.Append(DataReader.GetDateTime(i));
                                break;
                            case "TIMESTAMP":
                                row.Append(DataReader.GetDateTime(i));
                                break;
                            case "VARCHAR":
                                row.Append(DataReader.GetChars(i, 0, 0));
                                break;
                            case "CHAR FOR BIT DATA":
                                row.Append(DbHelper.ConvertToString(DataReader.GetBytes(i)));
                                break;
                            case "VARCHAR FOR BIT DATA":
                                row.Append(DbHelper.ConvertToString(DataReader.GetBytes(i)));
                                break;
                            case "LONG VARCHAR FOR BIT DATA":
                                row.Append(DbHelper.ConvertToString(DataReader.GetBytes(i)));
                                break;
                            case "BLOB":
                                row.Append(DbHelper.ConvertToString(DataReader.GetBytes(i)));
                                break;
                            case "CLOB":
                                row.Append(DataReader.GetChars(i, 0, 0));
                                break;
                            default:
                                row.Append("UNKNOWN");
                                break;
                        }
                        row.Append(", ");
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
