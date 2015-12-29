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
    class ReadColumnDataWithTypeSpecificMethods : GFXDTest
    {
        public ReadColumnDataWithTypeSpecificMethods(ManualResetEvent resetEvent)
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
                    for (int i = 0; i < colCount; i++)
                    {
                        try
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
                                    DateTime dt = DataReader.GetDateTime(i);
                                    Log(String.Format("DateTime.Kind = {0}", dt.Kind));
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
