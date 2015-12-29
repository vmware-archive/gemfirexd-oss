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
using System.IO;
using System.Threading;

using NUnit.Framework;
using System.Text;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Tests for function/Procedure/DAP invocation.
  /// </summary>
  [TestFixture]
  public class DDLFunctionTests : TestBase
  {
    private int m_locPort = -1;
    private int onTable = 0;
    private int onAll = 1;
    private int onServerGroups = 2;
    private int noOnClause = 3;
    private int onTableNoWhereClause = 5;

    #region Setup/TearDown methods

    private StartPeerDelegate m_peerStart = new StartPeerDelegate(
        StartGFXDPeer);

    [TestFixtureSetUp]
    public override void FixtureSetup()
    {
      InternalSetup(GetType());
      SetDefaultDriverType();

      // start one locator and default set of servers
      m_locPort = GetAvailableTCPPort();
      // starting a locator and three servers
      initClientPort();
      // first locator
      string locDir = s_testOutDir + "/gfxdlocator";
      if (Directory.Exists(locDir)) {
        Directory.Delete(locDir, true);
      }
      Directory.CreateDirectory(locDir);
      StartGFXDPeer(m_defaultDriverType, "locator", locDir, string.Empty,
                    s_clientPort, " -peer-discovery-address=localhost" +
                    " -peer-discovery-port=" + m_locPort);

      // then three servers
      IAsyncResult[] starts = new IAsyncResult[3];
      int clientPort = GetAvailableTCPPort();
      string serverDir = s_testOutDir + "/gfxdserver1";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[0] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort, null,
                                          null, null);

      clientPort = GetAvailableTCPPort();
      serverDir = s_testOutDir + "/gfxdserver2";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[1] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort, null,
                                          null, null);

      clientPort = GetAvailableTCPPort();
      serverDir = s_testOutDir + "/gfxdserver3";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[2] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort, null,
                                          null, null);

      // wait for servers to start
      foreach (IAsyncResult start in starts) {
        m_peerStart.EndInvoke(start);
      }
    }

    [TestFixtureTearDown]
    public override void FixtureTearDown()
    {
      // bring down the servers and locator
      /*
      if (m_locPort <= 0 || s_clientPort <= 0) {
        throw new NotSupportedException("No locator is running");
      }
      */
      //this doesn't work from ado.net
      //StopAllGFXDPeers(-1, m_locPort, "");
      string locDir = s_testOutDir + "/gfxdlocator";
      
      string serverDir = s_testOutDir + "/gfxdserver1";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir);
      serverDir = s_testOutDir + "/gfxdserver2";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir);
      serverDir = s_testOutDir + "/gfxdserver3";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir);

      StopGFXDPeer(m_defaultDriverType, "locator", locDir);
      m_locPort = -1;
      s_clientPort = -1;

      com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(null);
      if (s_logFileWriter != null) {
        s_logFileWriter.close();
        s_logFileWriter = null;
      }
    }

    protected void SetupCommonWithSG()
    {
      // start two servers with server-group SG2
      IAsyncResult[] starts = new IAsyncResult[2];
      int clientPort = GetAvailableTCPPort();
      string serverDir = s_testOutDir + "/gfxdserver1_sg";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[0] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort,
                                          " -server-groups=SG2", null, null);

      clientPort = GetAvailableTCPPort();
      serverDir = s_testOutDir + "/gfxdserver2_sg";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[1] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort,
                                          " -server-groups=SG2", null, null);

      // wait for servers to start
      foreach (IAsyncResult start in starts) {
        m_peerStart.EndInvoke(start);
      }
    }

    protected void TearDownCommonWithSG()
    {
      // stop the two servers with server-group SG2
      string serverDir2 = s_testOutDir + "/gfxdserver2_sg";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir2);
      string serverDir1 = s_testOutDir + "/gfxdserver1_sg";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir1);
    }

    protected override string[] GetProceduresToDrop()
    {
      return new string[] { "insertEmployee", "MY_ESCAPE_SELECT" };
    }

    protected override string[] GetFunctionsToDrop()
    {
      return new string[] { "times", "subsetRows" };
    }

    protected override string[] GetTablesToDrop()
    {
      return new string[] { "ORDERS", "EMPLOYEE", "EMP.PARTITIONTESTTABLE" };
    }

    protected override string[] GetSchemasToDrop()
    {
      return new string[] { "EMP" };
    }

    #endregion

    [Test]
    public void TestFunctionCreate()
    {
      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "partition by Primary Key redundancy 1 ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 100; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }

        Log("Insertion in table done.");

        cmd.CommandText = "CREATE FUNCTION times " +
          "(p1 INTEGER) " +
          "RETURNS INTEGER " +
          "LANGUAGE JAVA " +
          "EXTERNAL NAME 'tests.TestProcedures.times' " +
          "PARAMETER STYLE JAVA " +
          "NO SQL " +
          "RETURNS NULL ON NULL INPUT ";

        Log(cmd.CommandText);
        int result = cmd.ExecuteNonQuery();
        Log(result.ToString());

        cmd.CommandText = "SELECT times(ID) FROM ORDERS WHERE ID>0 and ID<15";
        cmd.CommandType = CommandType.Text;
        DbDataReader dbr = cmd.ExecuteReader();
        int rows = 0;
        while (dbr.Read()) {
          ++rows;
        }
        Log(rows.ToString());
        Assert.AreEqual(rows, 14, "#1");

        cmd.CommandText = "DROP FUNCTION times";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE FUNCTION subsetRows " +
         "(tableName VARCHAR(20), low INTEGER, high INTEGER) " +
         "RETURNS Table (ID INT, SECURITY_ID VARCHAR(10)) " +
         "LANGUAGE JAVA " +
         "EXTERNAL NAME 'tests.TestProcedures.subset' " +
         "PARAMETER STYLE DERBY_JDBC_RESULT_SET " +
         "READS SQL DATA " +
         "RETURNS NULL ON NULL INPUT ";

        Log(cmd.CommandText);
        result = cmd.ExecuteNonQuery();
        Log(result.ToString());

        cmd.CommandText = "SELECT s.* FROM Table(subsetRows('ORDERS',10, 15)) s";
        cmd.CommandType = CommandType.Text;
        DbDataReader dbr1 = cmd.ExecuteReader();
        rows = 0;
        while (dbr1.Read()) {
          ++rows;
        }
        Log(rows.ToString());

        Assert.AreEqual(rows, 4, "#2");

        cmd.CommandText = "DROP FUNCTION subsetRows";
        cmd.ExecuteNonQuery();

        conn.Close();
      }
    }

    [Test]
    public void TestProcedureCreate()
    {
      DbParameter param;
      DbDataReader dr = null;
      DbParameter idParam;
      DbParameter dojParam;

      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table employee (" +
          " id int PRIMARY KEY," +
          " fname varchar (50) NOT NULL," +
          " lname varchar (50) NULL," +
          " dob timestamp NOT NULL," +
          " doj timestamp NOT NULL," +
          " email varchar(50) NULL)";
        Log(cmd.CommandText);
        int result = cmd.ExecuteNonQuery();
        Assert.IsTrue(result == 0 || result == -1, "#S5");

        cmd.CommandText = "insert into employee values " +
          "(1, 'suresh', 'kumar', '1978-08-22 00:00:00'," +
          " '2001-03-12 00:00:00', 'suresh@gmail.com')," +
          "(2, 'ramesh', 'rajendran', '1977-02-15 00:00:00'," +
          " '2005-02-11 00:00:00', 'ramesh@yahoo.com')," +
          "(3, 'venkat', 'ramakrishnan', '1977-06-12 00:00:00'," +
          " '2003-12-11 00:00:00', 'ramesh@yahoo.com')," +
          "(4, 'ramu', 'dhasarath', '1977-02-15 00:00:00'," +
          " '2005-02-11 00:00:00', 'ramesh@yahoo.com')";
        Log(cmd.CommandText);
        result = cmd.ExecuteNonQuery();
        Assert.AreEqual(4, result, "#S6");

        Log("Insertion in table done.");

        cmd.CommandText = "create procedure insertEmployee" +
         " (fname varchar(20), dob timestamp, out doj timestamp, out id int)" +
         " language java parameter style java external name" +
        " 'tests.TestProcedures.insertEmployee'";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call insertEmployee(?, ?, ?, ?)";
        cmd.CommandType = CommandType.StoredProcedure;

        param = cmd.CreateParameter();
        param.ParameterName = "fname";
        param.DbType = DbType.String;
        param.Value = "testA";
        cmd.Parameters.Add(param);

        param = cmd.CreateParameter();
        param.ParameterName = "dob";
        param.DbType = DbType.DateTime;
        param.Value = new DateTime(2004, 8, 20);
        cmd.Parameters.Add(param);

        dojParam = cmd.CreateParameter();
        dojParam.ParameterName = "doj";
        dojParam.DbType = DbType.DateTime;
        dojParam.Direction = ParameterDirection.Output;
        cmd.Parameters.Add(dojParam);

        idParam = cmd.CreateParameter();
        idParam.ParameterName = "id";
        idParam.DbType = DbType.Int32;
        idParam.Direction = ParameterDirection.ReturnValue;
        cmd.Parameters.Add(idParam);
        Log(cmd.CommandText);
        Assert.AreEqual(-1, cmd.ExecuteNonQuery(), "#A1");

        cmd.Dispose();

        cmd = conn.CreateCommand();
        cmd.CommandText = "select fname, dob, doj from employee where id=?";
        Log(cmd.CommandText);
        param = cmd.CreateParameter();
        param.DbType = DbType.Int32;
        param.ParameterName = "@id";
        param.Value = idParam.Value;
        cmd.Parameters.Add(param);

        dr = cmd.ExecuteReader();
        Assert.IsTrue(dr.Read(), "#A2");
        Assert.AreEqual(typeof(string), dr.GetFieldType(0), "#A3");
        Assert.AreEqual("testA", dr.GetValue(0), "#A4");
        Assert.AreEqual(typeof(DateTime), dr.GetFieldType(1), "#A5");
        Assert.AreEqual(new DateTime(2004, 8, 20), dr.GetValue(1), "#A6");
        Assert.AreEqual(typeof(DateTime), dr.GetFieldType(2), "#A7");
        Assert.AreEqual(dojParam.Value, dr.GetValue(2), "#A8");
        Assert.IsFalse(dr.Read(), "#A9");

        cmd.Dispose();

        cmd = conn.CreateCommand();
        cmd.CommandText = "DROP PROCEDURE insertEmployee";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        cmd.Dispose();

        conn.Close();
      }
    }

    [Test]
    public void TestExecuteOnAllAndLocalAndGlobalEscape()
    {
      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table EMP.PARTITIONTESTTABLE (ID int not null,"
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + " PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "   VALUES BETWEEN 60 and 80 ) redundancy 2";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into EMP.partitiontesttable values" +
          " (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        Log("Insertion in table done.");

        cmd.CommandText = "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
          "'tests.TestProcedures.select_proc' " +
          "DYNAMIC RESULT SETS 4";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call MY_ESCAPE_SELECT(?) ON ALL";
        DbParameter p1 = cmd.CreateParameter();
        p1.ParameterName = "p1";
        p1.Direction = ParameterDirection.Input;
        p1.DbType = DbType.Int32;
        p1.Value = onAll; // onAll
        cmd.Parameters.Add(p1);

        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();

        int escSeq = 0;
        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());

          switch (escSeq) {
            case 0:
              Assert.AreEqual(12, rows, "#1 Multiple rows shud be returned");
              break;
            case 1:
              Assert.AreEqual(4, rows, "#1 Multiple rows shud be returned");
              break;
            case 2:
              Assert.AreEqual(12, rows, "#1 Multiple rows shud be returned");
              break;
            default:
              break;
          }
          ++results;
          rows = 0;
          escSeq++;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(4, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "DROP PROCEDURE MY_ESCAPE_SELECT";
        cmd.ExecuteNonQuery();
        conn.Close();
      }
    }

    [Test]
    public void TestExecuteOnServerGroupsAndLocalAndGlobalEscape()
    {
      // start the additional servers
      SetupCommonWithSG();

      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create schema EMP default server groups (SG2)";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into EMP.partitiontesttable values" +
          " (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        Log("Insertion in table done.");

        cmd.CommandText = "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
          "'tests.TestProcedures.select_proc' " +
          "DYNAMIC RESULT SETS 4";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call MY_ESCAPE_SELECT(?) ON server groups (sg2)";
        DbParameter p1 = cmd.CreateParameter();
        p1.ParameterName = "p1";
        p1.Direction = ParameterDirection.Input;
        p1.DbType = DbType.Int32;
        p1.Value = onServerGroups; // onServerGroups
        cmd.Parameters.Add(p1);

        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();

        int escSeq = 0;
        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());

          switch (escSeq) {
            case 0:
              Assert.AreEqual(8, rows, "#1 Multiple rows shud be returned");
              break;
            case 1:
              Assert.AreEqual(4, rows, "#1 Multiple rows shud be returned");
              break;
            case 2:
              Assert.AreEqual(8, rows, "#1 Multiple rows shud be returned");
              break;
            default:
              break;
          }
          ++results;
          rows = 0;
          escSeq++;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(4, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "DROP PROCEDURE MY_ESCAPE_SELECT";
        cmd.ExecuteNonQuery();
        conn.Close();
      }

      // stop the additional servers
      TearDownCommonWithSG();
    }

    [Test]
    public void TestExecuteOnTableWithWhereClauseAndLocalAndGlobalEscape()
    {
      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into EMP.partitiontesttable values" +
          " (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        Log("Insertion in table done.");

        cmd.CommandText = "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
          "'tests.TestProcedures.select_proc' " +
          "DYNAMIC RESULT SETS 4";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call MY_ESCAPE_SELECT(?) ON TABLE" +
          " EMP.PARTITIONTESTTABLE where ID >= 20 and ID < 40";
        DbParameter p1 = cmd.CreateParameter();
        p1.ParameterName = "p1";
        p1.Direction = ParameterDirection.Input;
        p1.DbType = DbType.Int32;
        p1.Value = onTable; // OnTable
        cmd.Parameters.Add(p1);

        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();

        int escSeq = 0;
        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());

          switch (escSeq) {
            case 0:
              Assert.AreEqual(2, rows, "#1 Multiple rows shud be returned");
              break;
            case 1:
              Assert.AreEqual(2, rows, "#1 Multiple rows shud be returned");
              break;
            case 2:
              Assert.AreEqual(2, rows, "#1 Multiple rows shud be returned");
              break;
            default:
              break;
          }
          ++results;
          rows = 0;
          escSeq++;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(4, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "DROP PROCEDURE MY_ESCAPE_SELECT";
        cmd.ExecuteNonQuery();
        conn.Close();
      }
    }

    [Test]
    public void TestExecuteOnTableWithNoWhereClauseAndLocalAndGlobalEscape()
    {
      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into EMP.partitiontesttable values" +
          " (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        Log("Insertion in table done.");

        cmd.CommandText = "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
          "'tests.TestProcedures.select_proc' " +
          "DYNAMIC RESULT SETS 4";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call MY_ESCAPE_SELECT(?) ON TABLE EMP.PARTITIONTESTTABLE";
        DbParameter p1 = cmd.CreateParameter();
        p1.ParameterName = "p1";
        p1.Direction = ParameterDirection.Input;
        p1.DbType = DbType.Int32;
        p1.Value = onTableNoWhereClause; // onTableNoWhereClause
        cmd.Parameters.Add(p1);

        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();

        int escSeq = 0;
        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());

          switch (escSeq) {
            case 0:
              Assert.AreEqual(12, rows, "#1 Multiple rows shud be returned");
              break;
            case 1:
              Assert.AreEqual(4, rows, "#1 Multiple rows shud be returned");
              break;
            case 2:
              Assert.AreEqual(12, rows, "#1 Multiple rows shud be returned");
              break;
            default:
              break;
          }
          ++results;
          rows = 0;
          escSeq++;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(4, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "DROP PROCEDURE MY_ESCAPE_SELECT";
        cmd.ExecuteNonQuery();
        conn.Close();
      }
    }

    [Test]
    public void TestExecuteAndLocalAndGlobalEscape()
    {
      // Open a new connection to the network server running on localhost
      using (DbConnection conn = OpenNewConnection()) {
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into EMP.partitiontesttable values" +
          " (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        Log("Insertion in table done.");

        cmd.CommandText = "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
          "'tests.TestProcedures.select_proc' " +
          "DYNAMIC RESULT SETS 4";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "call MY_ESCAPE_SELECT(?)";
        DbParameter p1 = cmd.CreateParameter();
        p1.ParameterName = "p1";
        p1.Direction = ParameterDirection.Input;
        p1.DbType = DbType.Int32;
        p1.Value = noOnClause; // noOnClause
        cmd.Parameters.Add(p1);

        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();

        int escSeq = 0;
        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());

          switch (escSeq) {
            case 0:
              Assert.AreEqual(4, rows, "#1 Multiple rows shud be returned");
              break;
            case 1:
              // local server can have 1 or 2 rows but should not have more
              // due to balanced buckets
              Assert.IsTrue(rows == 1 || rows == 2, "#2 One or two rows shud" +
                            " be returned but got " + rows);
              break;
            case 2:
              Assert.AreEqual(4, rows, "#3 Multiple rows shud be returned");
              break;
            default:
              break;
          }
          ++results;
          rows = 0;
          escSeq++;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(3, results, "#4 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "DROP PROCEDURE MY_ESCAPE_SELECT";
        cmd.ExecuteNonQuery();
        conn.Close();
      }
    }
  }
}
