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

namespace AdoNetTest.BIN.Parameter
{
    class SelectDateTimeColumnsAsParameters : GFXDTest
    {
        public SelectDateTimeColumnsAsParameters(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {            
            try
            {
                Log("Connection String = " + Connection.ConnectionString);
                CreateTable();

                Command.CommandText = "SELECT * FROM datetime_test ORDER BY id ASC";
                ReadData();

                Command.CommandText = "SELECT * FROM datetime_test WHERE type_datetime=?";
                GFXDParameter param = Command.CreateParameter();
                param.DbType = DbType.DateTime;
                param.Value = DateTime.Parse("2009-09-09 09:09:09");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.DateTime;
                param.Value = DateTime.Parse("2010-10-10 10:10:10");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.DateTime;
                param.Value = DateTime.Parse("2011-11-11 11:11:11");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.DateTime;
                param.Value = DateTime.Parse("2011-04-24 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.DateTime;
                param.Value = DateTime.Parse("2011-04-26 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");


                ////////////////////////////////////////////////////////////////////////////////

                Command.CommandText = "SELECT * FROM datetime_test WHERE type_date=?";
                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Date;
                param.Value = DateTime.Parse("2009-09-09 09:09:09");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Date;
                param.Value = DateTime.Parse("2010-10-10 10:10:10");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Date;
                param.Value = DateTime.Parse("2011-11-11 11:11:11");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Date;
                param.Value = DateTime.Parse("2011-04-24 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Date;
                param.Value = DateTime.Parse("2011-04-26 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                /////////////////////////////////////////////////////////////////////////////////

                Command.CommandText = "SELECT * FROM datetime_test WHERE type_time=?";

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Time;
                param.Value = DateTime.Parse("2009-09-09 09:09:09");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Time;
                param.Value = DateTime.Parse("2010-10-10 10:10:10");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Time;
                param.Value = DateTime.Parse("2011-11-11 11:11:11");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Time;
                param.Value = DateTime.Parse("2011-04-24 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");

                Command.Parameters.Clear();
                param = Command.CreateParameter();
                param.DbType = DbType.Time;
                param.Value = DateTime.Parse("2011-04-26 12:12:12");
                Command.Parameters.Add(param);
                if (ReadData() < 1)
                    Fail("DataReader returns no rows");
                Log("===============================================================================");
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {   
                try
                {
                    DropTable();
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }

        private void CreateTable()
        {
            DropTable();

            Command.CommandText = "CREATE TABLE datetime_test "
                + "(id INT primary key, "
                + "type_date DATE, "
                + "type_time TIME," 
                + "type_datetime TIMESTAMP)";

            Log(Command.CommandText);                    
            Command.ExecuteNonQuery();

            Command.CommandText = "INSERT INTO datetime_test VALUES" +
                " (1001, '2009-09-09', '09:09:09', '2009-09-09 09:09:09')," +
                " (1002, '2010-10-10', '10:10:10', '2010-10-10 10:10:10')," +
                " (1003, '2011-11-11', '11:11:11', '2011-11-11 11:11:11')," +
                " (1004, '2012-12-12', '12:12:12', '2012-12-12 12:12:12')," +
                " (1005, '2011-04-24', '12:12:12', '2011-04-24 12:12:12')," +
                " (1006, '2011-04-26', '12:12:12', '2011-04-26 12:12:12')";
            Log(Command.CommandText);
            Command.ExecuteNonQuery();
        }

        private int ReadData()
        {
            int rows = 0;
            //Log(String.Format("{0}; param = {1}", Command.CommandText, Command.Parameters[0].ToString()));
            Log(String.Format("{0}", Command.CommandText));
            DataReader = Command.ExecuteReader();
            
            while (DataReader.Read())
            {
                rows += 1;
                Log(String.Format("{0}, {1}, {2}, {3}", DataReader.GetString(0),
                    DataReader.GetString(1), DataReader.GetString(2), DataReader.GetString(3)));
            }
            DataReader.Close();

            return rows;
        }

        private void DropTable()
        {
            if(DbHelper.TableExists("DATETIME_TEST"))
            {
                Command.CommandText = "DROP TABLE DATETIME_TEST";
                Command.Parameters.Clear();
                Command.ExecuteNonQuery();
            }
        }
    }
}