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
using System.Threading;
using System.Text;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.Template
{
    class AdoNetSamples : GFXDTest
    {
        public AdoNetSamples(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            try
            {

            }
            catch (Exception e)
            {
                Log(e);
            }
            finally
            {
                base.Run(context);
            }
        }

//        private void OpenConnection()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                // Initialize and open connection
//                connection = new GFXDClientConnection(connectionStr);
//                connection.Open();

//                ///
//                /// Peform works 
//                ///
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDCommand()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = "SELECT COUNT(*) FROM product";

//                connection.Open();

//                int prodCount = Convert.ToInt32(command.ExecuteScalar());
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDDataReader()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = "SELECT * FROM product";

//                connection.Open();

//                GFXDDataReader reader = command.ExecuteReader();
//                StringBuilder row = new StringBuilder();

//                while (reader.Read())
//                {
//                    for (int i = 0; i < reader.FieldCount; i++)
//                        row.AppendFormat("{0}, ", reader.GetString(i));

//                    Console.WriteLine(row.ToString());
//                }
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDDataAdapterWithDT()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = "SELECT * FROM product";

//                connection.Open();

//                // Create adapter and populate the DataTable object
//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("product");
//                adapter.Fill(table);

//                // Parse the DataTable object by rows
//                foreach (DataRow row in table.Rows)
//                {
//                    StringBuilder sb = new StringBuilder();

//                    for (int i = 0; i < row.Table.Columns.Count; i++)
//                        sb.AppendFormat("{0}, ", (row[i].ToString()));

//                    Console.WriteLine(sb.ToString());
//                }
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDDataAdapterWithDS()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                connection.Open();

//                // Create adapter and data set to hold multiple result sets
//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataSet dataset = new DataSet("CustomerOrder");

//                // Retrieve all customer records
//                command.CommandText = "SELECT * FROM customer";
//                adapter.Fill(dataset, "customer");

//                // Retrieve all order records
//                command.CommandText = "SELECT * FROM order";
//                adapter.Fill(dataset, "order");

//                // Retrieve all orderdetail records
//                command.CommandText = "SELECT * FROM orderdetail";
//                adapter.Fill(dataset, "orderdetail");


//                // Parse all tables and rows in the data set
//                foreach (DataTable table in dataset.Tables)
//                {
//                    foreach (DataRow row in table.Rows)
//                    {
//                        StringBuilder sb = new StringBuilder();

//                        for (int i = 0; i < row.Table.Columns.Count; i++)
//                            sb.AppendFormat("{0}, ", (row[i].ToString()));

//                        Console.WriteLine(sb.ToString());
//                    }
//                }
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDDataAdapterUpdate()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = @"SELECT unit_cost, retail_price
//                                       FROM product WHERE product_id=<id>";

//                connection.Open();

//                // Create adapter and populate the DataTable object
//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("product");
//                adapter.Fill(table);

//                // Generate update command
//                GFXDCommandBuilder builder = new GFXDCommandBuilder(adapter);
//                adapter.UpdateCommand = builder.GetUpdateCommand();

//                // Modify product pricing
//                table.Rows[0]["unit_cost"] = 99.99;
//                table.Rows[0]["retail_price"] = 199.99;

//                // Update the underlying table
//                adapter.Update(table);
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDDataAdapterInsert()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = @"SELECT * FROM product";

//                connection.Open();

//                // Create adapter and populate the DataTable object
//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("product");
//                adapter.Fill(table);

//                // Generate update command
//                GFXDCommandBuilder builder = new GFXDCommandBuilder(adapter);
//                adapter.InsertCommand = builder.GetInsertCommand();

//                // Create new product row
//                DataRow row = table.NewRow();
//                row[0] = <product_id>;
//                row[1] = <...>;
//                row[2] = <...>;
//                ...

//                // Update the underlying table
//                adapter.Update(table);
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDTransaction()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                // Open connection, disable auto-commit, and start transaction
//                connection = new GFXDClientConnection(connectionStr);
//                connection.AutoCommit = false;
//                connection.BeginGFXDTransaction(IsolationLevel.ReadCommitted);

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;

//                connection.Open();

//                // Get product info
//                command.CommandText = "SELECT * FROM product WHERE product_id=?";
//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("product");
//                adapter.Fill(table);

//                // Create new order
//                command.CommandText = "INSERT INTO order VALUES(?, ?, ?, ?, ?)";
//                command.ExecuteNonQuery();

//                // Create new order detail
//                command.CommandText = "INSERT INTO orderdetail VALUES(?, ?, ?, ?, ?)";
//                command.ExecuteNonQuery();

//                // Update product quantity
//                command.CommandText = "UPDATE product SET quantity=? WHERE product_id=?";
//                command.ExecuteNonQuery();

//                // Commit transaction
//                connection.Commit();
//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseGFXDParameter()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                // Initialize and open connection
//                connection = new GFXDClientConnection(connectionStr);
//                connection.Open();

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = @"SELECT * FROM order WHERE order_date = ?
//                                       AND subtotal > ?";

//                // Insert order_date value
//                GFXDParameter param1 = Command.CreateParameter();
//                param1.DbType = DbType.Date;
//                param1.Value = DateTime.Today;
//                Command.Parameters.Add(param1);

//                // Insert subtotal value 
//                GFXDParameter param2 = Command.CreateParameter();
//                param2.DbType = DbType.Decimal;
//                param2.Value = 999.99;
//                Command.Parameters.Add(param2);

//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("ORDER");
//                adapter.Fill(table);

//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseObjectsAsParameters()
//        {
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                // Initialize and open connection
//                connection = new GFXDClientConnection(connectionStr);
//                connection.Open();

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = @"SELECT * FROM order WHERE order_date = ?
//                                       AND subtotal > ?";

//                // Create an object array containing parameters' value
//                object[] parameters = new object[] { DateTime.Today, 999.99 };

//                // Add object array as parameters (range)
//                command.Parameters.AddRange(parameters);

//                GFXDDataAdapter adapter = command.CreateDataAdapter();
//                DataTable table = new DataTable("ORDER");
//                adapter.Fill(table);

//            }
//            catch (Exception e)
//            {
//                ///
//                /// Log or re-throw exception
//                /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }

//        private void UseBatchOperation()
//        {
//            int numRecords = 100;   
//            int batchSize = 10;     // Limit number of statements per batch execution
//            string gfxdHost = "localhost";
//            int gfxdPort = 1527;
//            string connectionStr = string.Format(@"server={0}:{1}", gfxdHost, gfxdPort);

//            GFXDClientConnection connection = null;

//            try
//            {
//                connection = new GFXDClientConnection(connectionStr);
//                connection.Open();

//                GFXDCommand command = connection.CreateCommand();
//                command.CommandType = CommandType.Text;
//                command.CommandText = @"INSERT INTO ORDER(order_id, order_date, ship_date, 
//                                      customer_id, subtotal) VALUES(?, ?, ?, ?, ?)";

//                // Prepare batch statement
//                command.Prepare();

//                int stCount = 0; // batch statements count

//                for (int i = 0; i < numRecords; i++)
//                {
//                    command.Parameters[0] = <order_id>;
//                    command.Parameters[1] = <order_date>;
//                    command.Parameters[2] = <ship_date>;
//                    command.Parameters[3] = <customer_id>;
//                    command.Parameters[4] = <subtotal>;

//                    // Add statement to command's batch
//                    command.AddBatch();

//                    // Execute Batch statements when batch size is reached and reset
//                    if ((++stCount) == batchSize)
//                    {
//                        command.ExecuteBatch();
//                        stCount = 0; 
//                    }
//                }

//                // Execute the remaining statements in the batch
//                command.ExecuteBatch();
//            }
//            catch (Exception e)
//            {
//            ///
//            /// Log or re-throw exception
//            /// 
//            }
//            finally
//            {
//                connection.Close();
//            }
//        }
    }
}
