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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using ado_net_gemfirexd_client.Properties;
using Pivotal.Data.GemFireXD;
using ado_net_sqlsvr_client.Properties;

namespace ado_net_gemfirexd_client.Models
{
    public class DataAccess
    {
        public DataAccess()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;            
        }

        public DataSet ExecuteQuery(string query)
        {
            var dt = new DataSet();

            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    conn.Open();

                    GFXDCommand cmd = conn.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = query;

                    var da = new GFXDDataAdapter(cmd);
                    da.Fill(dt);
                }
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }

            return dt;
        }

        public void ExcuteTransaction(List<string> tables)
        {
            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    // Open connection, disable auto-commit, and start transaction
                    conn.Open();

                    conn.AutoCommit = false;
                    conn.BeginGFXDTransaction(IsolationLevel.ReadCommitted);
                    GFXDCommand cmd = conn.CreateCommand();
                    cmd.CommandType = CommandType.Text;


                    for (int i = 0; i < tables.Count; i++)
                    {
                        string[] words = tables[i].Split('.');
                        cmd.CommandText = "Select * from " + tables[i] + " order by " + words[1] + "ID"; ;

                        var adapter = cmd.CreateDataAdapter();
                        var table = new DataTable(tables[i]);
                        adapter.Fill(table);

                        int cnt = table.Rows.Count;
                        var idx = (int)table.Rows[cnt - 1].ItemArray[0];


                        var builder = new GFXDCommandBuilder(adapter);
                        adapter.InsertCommand = builder.GetInsertCommand();
                        // Create new product row
                        for (int ctx = 0; ctx < 10000; ctx++)
                        {
                            DataRow row = table.NewRow();
                            row[0] = ++idx;
                            for (int j = 1; j < (table.Rows[cnt - 1].ItemArray.Count()); j++)
                            {
                                row[j] = table.Rows[cnt - 1].ItemArray[j];
                            }
                            table.Rows.Add(row);
                        }
                        // Update the underlying table
                        adapter.Update(table);
                    }

                    // Commit transaction
                    conn.Commit();
                }  
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }
        }

        public void InsertBulkData(string tableName)
        {
            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    string[] words = tableName.Split('.');
                    GFXDCommand cmd = conn.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "Select * from " + tableName + " order by " + words[1] + "ID";

                    conn.Open();


                    GFXDDataAdapter adapter = cmd.CreateDataAdapter();
                    var table = new DataTable(tableName);
                    adapter.Fill(table);


                    int cnt = table.Rows.Count;
                    var idx = (int)table.Rows[cnt - 1].ItemArray[0];


                    var builder = new GFXDCommandBuilder(adapter);
                    adapter.InsertCommand = builder.GetInsertCommand();
                    // Create new product row
                    for (int ctx = 0; ctx < 10000; ctx++)
                    {
                        DataRow row = table.NewRow();
                        row[0] = ++idx;
                        for (int i = 1; i < (table.Rows[cnt - 1].ItemArray.Count()); i++)
                        {
                            row[i] = table.Rows[cnt - 1].ItemArray[i];
                        }
                        table.Rows.Add(row);
                    }
                    // Update the underlying table
                    adapter.Update(table);
                }
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }
        }

        public void DeleteBulkData(string tableName)
        {
            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    string[] words = tableName.Split('.');

                    GFXDCommand cmd = conn.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "Select MAX(" +  words[1] + "ID) from " + tableName;
                    
                    conn.Open();

                    int prodCount = Convert.ToInt32(cmd.ExecuteScalar());

                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "Delete from " + tableName + " where " + words[1] + "ID between ? and ?";

                    // Insert order_date value and add to command’s Parameters collection
                    GFXDParameter param1 = cmd.CreateParameter();
                    param1.DbType = DbType.Int32;
                    param1.Value = (prodCount - 1000);
                    cmd.Parameters.Add(param1);
                    // Insert subtotal value add to command’s Parameters collection
                    GFXDParameter param2 = cmd.CreateParameter();
                    param2.DbType = DbType.Int32;
                    param2.Value = prodCount;
                    cmd.Parameters.Add(param2);

                    var da = new GFXDDataAdapter(cmd);
                    var dt = new DataTable(tableName);
                    da.Fill(dt);




                    //DataTable dt = new DataTable();
                    //var adapter = new GFXDDataAdapter();
                    //cmd.CommandText = "SELECT * FROM Sales.SalesReason";
                    //adapter.SelectCommand = cmd;
                    //DbCommandBuilder builder = new GFXDCommandBuilder(adapter);
                    //adapter.Fill(dt);

                    //for (int i = 500; i < 1000; i++)
                    //{
                    //    dt.Rows[i].Delete();
                    //}

                    //adapter.Update(dt);
                }
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }
        }

        public void UpdateBulkData(string tableName)
        {
            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    conn.Open();

                    //GFXDCommand cmd = conn.CreateCommand();
                    //cmd.CommandType = CommandType.Text;
                    //cmd.CommandText = "Delete from Sales.SalesReason where SalesReasonID between 17000 and 19485";

                    //var da = new GFXDDataAdapter(cmd);
                    //var dt = new DataTable(); 
                    //da.Fill(dt);


                    GFXDCommand cmd = conn.CreateCommand();

                    DataTable dt = new DataTable();
                    var adapter = new GFXDDataAdapter();
                    cmd.CommandText = "SELECT * FROM Sales.SalesReason";
                    adapter.SelectCommand = cmd;
                    DbCommandBuilder builder = new GFXDCommandBuilder(adapter);
                    adapter.Fill(dt);

                    for (int i = 500; i < 1000; i++)
                    {
                        dt.Rows[i].Delete();
                    }

                    adapter.Update(dt);
                }
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }
        }

        public void ExecuteNonQuery()
        {
            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    conn.Open();

                    // create a table
                    // using the base DbCommand class rather than GemFireXD specific class
                    DbCommand cmd = conn.CreateCommand();
                    cmd.CommandText = "create table dbo.TestData5 (TestData5ID int primary key, name varchar(50))";
                    //cmd.CommandText = "drop table dbo.TestData2";
                    cmd.ExecuteNonQuery();
                }
            }
            catch (GFXDException ex)
            {
                string err = ex.Message;
            }
            catch (Exception ex)
            {
                string err = ex.Message;
            }
        }

        private string _connectionString = string.Empty;
        public string ConnectionString
        {
            get { return _connectionString; }
            set
            {
                _connectionString = value;
            }
        }
    }
}
