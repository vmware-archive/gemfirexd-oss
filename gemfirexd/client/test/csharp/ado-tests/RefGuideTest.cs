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

using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Examples given in the reference guide.
  /// </summary>
  [TestFixture]
  public class RefGuideTest : TestBase
  {
    #region Private fields

    private static readonly int s_numInserts = 1000;

    #endregion

    #region Helper methods

    protected void VerifyInserts(GFXDConnection conn, int numInserts)
    {
      VerifyInserts(conn, "select * from t1", 0, numInserts - 1, "addr");
    }

    protected void VerifyInserts(GFXDConnection conn, string cmdText,
                                 int start, int end, string addrPrefix)
    {
      // check the inserts
      GFXDCommand cmd = new GFXDCommand(cmdText, conn);
      Dictionary<int, string> result =
        new Dictionary<int, string>(end - start + 1);
      int id;
      string addr;
      GFXDDataReader reader = cmd.ExecuteReader();
      for (int i = start; i <= end; i++) {
        Assert.IsTrue(reader.Read(), "failed in read for i=" + i);
        id = reader.GetInt32(0);
        addr = reader.GetString(1);
        Assert.IsFalse(result.ContainsKey(id),
                       "unexpected duplicate for id=" + id);
        Assert.AreEqual(addrPrefix + id, addr);
        result.Add(id, addr);
      }
      Assert.IsFalse(reader.Read());
    }

    protected string GetEscapedParameterName(string param)
    {
      return ":" + param;
    }

    #endregion

    [Test]
    public void ConnectDefaultsIncludingWithProperty()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // check a query
        Assert.AreEqual(1, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();
      }

      // Open a new connection to the server with streaming disabled
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        Dictionary<string, string> props = new Dictionary<string, string>();
        props.Add("disable-streaming", "true");
        conn.Open(props);

        // check a query
        Assert.AreEqual(1, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();
      }
    }

    [Test]
    public void PositionalParameters()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using positional parameters
          cmd = new GFXDCommand("insert into t1 (id, addr) values (?, ?)",
                                conn);
          cmd.Prepare();
          for (int i = 0; i < s_numInserts; i++) {
            cmd.Parameters.Clear();
            cmd.Parameters.Add(i);
            cmd.Parameters.Add("addr" + i);

            cmd.ExecuteNonQuery();
          }

          // check the inserts
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void NamedParameters()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using named parameters
          cmd = new GFXDCommand("insert into t1 values (:ID, :ADDR)", conn);
          cmd.Prepare();
          DbParameter prm;
          for (int i = 0; i < s_numInserts; i++) {
            // first the parameter for ID
            cmd.Parameters.Clear();
            prm = cmd.CreateParameter();
            prm.ParameterName = "ID";
            prm.DbType = DbType.Int32;
            prm.Value = i;
            cmd.Parameters.Add(prm);
            // then the parameter for ADDR
            prm = cmd.CreateParameter();
            prm.ParameterName = "ADDR";
            prm.DbType = DbType.String;
            prm.Value = "addr" + i;
            cmd.Parameters.Add(prm);

            cmd.ExecuteNonQuery();
          }

          // check the inserts
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void MixedParameters()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using named parameters
          cmd = new GFXDCommand("insert into t1 values (:ID, ?)", conn);
          cmd.Prepare();
          DbParameter prm;
          for (int i = 0; i < s_numInserts; i++) {
            // first the parameter for ID
            cmd.Parameters.Clear();
            prm = cmd.CreateParameter();
            prm.ParameterName = "ID";
            prm.DbType = DbType.Int32;
            prm.Value = i;
            cmd.Parameters.Add(prm);
            // then the parameter for ADDR
            prm = cmd.CreateParameter();
            prm.ParameterName = "ADDR";
            prm.Value = "addr" + i;
            cmd.Parameters.Add(prm);

            cmd.ExecuteNonQuery();
          }

          // check the inserts
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void PositionalParametersBatch()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using positional parameters
          cmd = new GFXDCommand("insert into t1 values (?, ?)", conn);
          cmd.Prepare();
          for (int i = 0; i < s_numInserts; i++) {
            cmd.Parameters.Add(i);
            cmd.Parameters.Add("addr" + i);
            cmd.AddBatch();
          }
          int[] results = cmd.ExecuteBatch();

          // check the inserts
          Assert.AreEqual(s_numInserts, results.Length);
          for (int i = 0; i < s_numInserts; i++) {
            Assert.AreEqual(1, results[i], "unexpected result=" + results[i] +
                            " for i=" + i);
          }
          // also check in the database
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void MultipleStringsBatch()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          string cmdStr;
          // insert into the table using different command strings
          for (int i = 0; i < s_numInserts; i++) {
            cmdStr = "insert into t1 values (" + i + ", 'addr" + i + "')";
            cmd.AddBatch(cmdStr);
          }
          int[] results = cmd.ExecuteBatch();

          // check the inserts
          Assert.AreEqual(s_numInserts, results.Length);
          for (int i = 0; i < s_numInserts; i++) {
            Assert.AreEqual(1, results[i], "unexpected result=" + results[i] +
                            " for i=" + i);
          }
          // also check in the database
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void DataAdapterBatch()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        // using the base DbCommand class rather than GemFireXD specific class
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table t1 (id int primary key," +
          " addr varchar(20))";
        cmd.ExecuteNonQuery();

        try {
          // populate DataTable from the underlying table
          cmd = conn.CreateCommand();
          cmd.CommandText = "select * from t1";
          DbDataAdapter adapter = new GFXDDataAdapter((GFXDCommand)cmd);
          adapter.SelectCommand = cmd;
          // associate a command builder with the DataAdapter
          new GFXDCommandBuilder((GFXDDataAdapter)adapter);
          // fill a DataTable using the above select SQL command
          // though there is no data to be populated yet, the schema will still be
          // set correctly in the DataTable even with no data which is required
          // before trying to make any updates
          DataTable table = new DataTable();
          adapter.Fill(table);
          // set batch size for best performance
          adapter.UpdateBatchSize = s_numInserts;
          // now perform the inserts in the DataTable
          for (int i = 0; i < s_numInserts; i++) {
            DataRow newRow = table.NewRow();
            newRow["ID"] = i;
            newRow["ADDR"] = "addr" + i;
            table.Rows.Add(newRow);
          }
          // apply the inserts to the underlying GemFireXD table
          adapter.Update(table);

          // check the inserts
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void DataReaderWithPositionalParameters()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using positional parameters
          cmd = new GFXDCommand("insert into t1 (id, addr) values (?, ?)",
                                conn);
          cmd.Prepare();
          for (int i = 1; i <= s_numInserts; i++) {
            cmd.Parameters.Clear();
            cmd.Parameters.Add(i);
            cmd.Parameters.Add("addr" + i);

            cmd.ExecuteNonQuery();
          }

          // now query the table using a DataReader
          cmd.Parameters.Clear();
          cmd.CommandText = "select * from t1";
          GFXDDataReader reader = cmd.ExecuteReader();
          int[] ids = new int[s_numInserts];
          int numResults = 0;
          while (reader.Read()) {
            int id = reader.GetInt32(0);
            string addr = reader.GetString(1);
            if (ids[id - 1] != 0) {
              throw new Exception("Duplicate value for ID=" + id +
                                  " addr=" + addr);
            }
            ids[id - 1] = id;
            numResults++;
          }
          reader.Close();
          if (numResults != s_numInserts) {
            throw new Exception("unexpected number of results " + numResults);
          }

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void TransactionTest()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        GFXDCommand cmd = new GFXDCommand("create table t1 (id int primary" +
                                          " key, addr varchar(20))", conn);
        cmd.ExecuteNonQuery();

        try {
          GFXDTransaction tran = conn.BeginTransaction(IsolationLevel
                                                       .ReadCommitted);
          cmd.Transaction = tran;
          // insert into the table using positional parameters
          cmd = new GFXDCommand("insert into t1 (id, addr) values (?, ?)",
                                conn);
          cmd.Prepare();
          for (int i = 0; i < s_numInserts; i++) {
            cmd.Parameters.Clear();
            cmd.Parameters.Add(i);
            cmd.Parameters.Add("addr" + i);

            cmd.ExecuteNonQuery();
          }
          tran.Commit();

          // check the inserts
          VerifyInserts(conn, s_numInserts);

          // fire some updates and if any unsuccessful then rollback the transaction
          cmd.CommandText = "update t1 set addr = ? where id = ?";
          tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);
          cmd.Transaction = tran;
          bool success = true;
          for (int i = 100; i < 200; i++) {
            cmd.Parameters.Clear();
            cmd.Parameters.Add("address" + i);
            cmd.Parameters.Add(i);
            if (cmd.ExecuteNonQuery() != 1) {
              // update failed; rolling back the entire transaction
              success = false;
              tran.Rollback();
              break;
            }
          }
          // command for verification
          string verifyText = "select * from t1 where id >= 100 and id < 200";
          if (success) {
            // verify the updates in transactional data
            VerifyInserts(conn, verifyText, 100, 199, "address");
            // all succeeded; commit the transaction
            tran.Commit();
            // verify the updates after commit
            VerifyInserts(conn, verifyText, 100, 199, "address");
          }
          else {
            // verify no updates
            VerifyInserts(conn, verifyText, 100, 199, "addr");
          }

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }

    [Test]
    public void GenericCoding()
    {
      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create a table
        // using the base DbCommand class rather than GemFireXD specific class
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table t1 (id int primary key, addr varchar(20))";
        cmd.ExecuteNonQuery();

        try {
          // insert into the table using named parameters
          // using an abstracted method that can deal with difference in the
          // conventions of named parameters in different drivers
          cmd = conn.CreateCommand();
          string idPrm = GetEscapedParameterName("ID");
          string addrPrm = GetEscapedParameterName("ADDR");
          cmd.CommandText = "insert into t1 values (" + idPrm + "," + addrPrm + ")";
          cmd.Prepare();
          // using the base DbParameter class
          DbParameter prm;
          for (int i = 0; i < 1000; i++) {
            // first the parameter for ID
            cmd.Parameters.Clear();
            prm = cmd.CreateParameter();
            prm.ParameterName = "ID";
            prm.DbType = DbType.Int32;
            prm.Value = i;
            cmd.Parameters.Add(prm);
            // next the parameter for ADDR
            prm = cmd.CreateParameter();
            prm.ParameterName = "ADDR";
            prm.DbType = DbType.String;
            prm.Value = "addr" + i;
            cmd.Parameters.Add(prm);

            cmd.ExecuteNonQuery();
          }

          // check the inserts
          VerifyInserts(conn, s_numInserts);

        } finally {
          // drop the table
          cmd = new GFXDCommand("drop table t1", conn);
          cmd.ExecuteNonQuery();

          conn.Close();
        }
      }
    }
  }
}
