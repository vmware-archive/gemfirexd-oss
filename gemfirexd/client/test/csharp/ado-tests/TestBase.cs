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
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;

//using IBM.Data.DB2;
using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// This class encapsulates some base common functionality to be used by
  /// unit tests.
  /// </summary>
  public abstract class TestBase
  {
    public enum DriverType
    {
      DB2,
      Mono,
      GFXD,
      GFXDPeer
    }

    internal delegate void StartPeerDelegate(DriverType driver,
                                             string peerType,
                                             string peerDir,
                                             string discoveryArg,
                                             int clientPort,
                                             string additionalArgs);

    internal static DriverType DefaultDriverType = DriverType.GFXD;
    protected static java.io.PrintWriter s_logFileWriter;
    protected static readonly Random s_rand;
    protected static readonly string s_launcherScript;
    protected static readonly string s_serverDir;
    protected static readonly string s_testOutDir;
    protected static int s_clientPort = -1;
    protected static int s_savedClientPortBeforeStopping = -1;
    protected static int s_clientPort1 = -1;
    protected static int s_clientPort2 = -1;
    protected static int s_clientPortLoc2 = -1;
    protected DriverType m_defaultDriverType = DefaultDriverType;

    protected static readonly DateTime Epoch = new DateTime(1970, 1, 1,
      0, 0, 0, DateTimeKind.Utc);
    protected const int MaxWaitMillis = 120000;

    static TestBase()
    {
      TimeSpan ts = DateTime.Now - Epoch;
      s_rand = new Random((int)ts.TotalSeconds);
      string prodDir = Environment.GetEnvironmentVariable("GEMFIREXD");
      if (prodDir == null || prodDir.Length == 0) {
          throw new AssertionException("GEMFIREXD environment variable should"
         + " be set to point to GemFireXD product installation directory");
      }
      s_testOutDir = Environment.GetEnvironmentVariable("GFXDADOOUTDIR");
      if (s_testOutDir == null || s_testOutDir.Length == 0) {
        throw new AssertionException("GFXDADOOUTDIR environment variable should"
         + " be set to point to the directory where the test output should go");
      }
      if (IsUnix) {
          s_launcherScript = prodDir + "/bin/gfxd";
      }
      else {
          s_launcherScript = prodDir + "/bin/gfxd.bat";
      }
      s_serverDir = s_testOutDir + "/gfxdserver";
      Directory.CreateDirectory(s_serverDir);
    }

    protected TestBase()
    {
    }

    protected static void Log(string str)
    {
      s_logFileWriter.println(str);
      s_logFileWriter.flush();
    }

    protected static void Log(string fmt, params object[] args)
    {
      s_logFileWriter.println(string.Format(fmt, args));
      s_logFileWriter.flush();
    }

    protected virtual void SetDefaultDriverType()
    {
      m_defaultDriverType = DefaultDriverType;
    }

    internal static bool isRunningWithPool()
    { 
      string rwp = Environment.GetEnvironmentVariable("RUN_WITH_POOL");
      if (rwp != null )
      {
        return Convert.ToBoolean(rwp);
      }
      return true; //default is pool
    }

    internal static void StartGFXDServer(DriverType driver,
                                         string additionalArgs)
    {
      int mcastPort = GetRandomPort();
      //int mcastPort = 0;
      //if (driver == DriverType.GFXDPeer) {
      //  mcastPort = GetRandomPort();
      //}
      initClientPort();
      StartGFXDPeer(driver, "server", s_serverDir, "-mcast-port=" +
                    mcastPort, s_clientPort, additionalArgs);
    }

    internal static void initClientPort()
    {
      if (s_clientPort > 0) {
        throw new NotSupportedException("An GFXD peer is running at port "
          + s_clientPort);
      }
      s_clientPort = GetAvailableTCPPort();
    }

    internal static void initClientPort1()
    {
      if (s_clientPort1 > 0) {
        throw new NotSupportedException("An GFXD peer is running at port "
          + s_clientPort1);
      }
      s_clientPort1 = GetAvailableTCPPort();
    }

    internal static void initClientPort2()
    {
      if (s_clientPort2 > 0) {
        throw new NotSupportedException("An GFXD peer is running at port "
          + s_clientPort2);
      }
      s_clientPort2 = GetAvailableTCPPort();
    }

    internal static void StartGFXDPeer(DriverType driver,
                                       string peerType,
                                       string peerDir,
                                       string discoveryArg,
                                       int clientPort,
                                       string additionalArgs)
    {
      string logLevel = Environment.GetEnvironmentVariable("GFXD_LOGLEVEL");
      string logLevelStr = "";
      if (logLevel != null && logLevel.Length > 0) {
        logLevelStr = " -log-level=" + logLevel;
      } else {
        logLevelStr = " -log-level=fine";
      }
      // set the proxy for license
      string proxyHost = Environment.GetEnvironmentVariable("HTTP_PROXY_HOST");
      string proxyPort = Environment.GetEnvironmentVariable("HTTP_PROXY_PORT");
      string proxyArgs = "";
      if (proxyHost != null && proxyHost.Length > 0) {
        proxyArgs = " -J-Dhttp.proxyHost=" + proxyHost;
        if (proxyPort != null && proxyPort.Length > 0) {
          proxyArgs += (" -J-Dhttp.proxyPort=" + proxyPort);
        }
      }
      string peerArgs = peerType + " start " + discoveryArg
        + " -J-ea -J-Dgemfirexd.drda.logConnections=true"
        + " -bind-address=localhost"
        + " -client-port=" + clientPort
        + additionalArgs + proxyArgs
          //+ " -J-Dgemfirexd.drda.debug=true -J-Dgemfirexd.drda.traceAll=true"
          //+ " -J-Dgemfirexd.debug.true=TraceIndex,TraceLock_*,TraceTran"
          //+ " -J-Dgemfirexd.no-statement-matching=true"
        + logLevelStr// + " -log-level=fine -J-Dgemfirexd.table-default-partitioned=true"
        + " -dir=" + peerDir;
      ProcessStartInfo pinfo = new ProcessStartInfo(s_launcherScript,
                                                    peerArgs);
      //pinfo.CreateNoWindow = true;
      pinfo.UseShellExecute = false;
      pinfo.RedirectStandardOutput = true;
      pinfo.RedirectStandardInput = false;
      pinfo.RedirectStandardError = false;
      pinfo.EnvironmentVariables["CLASSPATH"] = s_testOutDir;
      Process proc = new Process();
      proc.StartInfo = pinfo;
      Log("Starting GFXD " + peerType + " with script: " + s_launcherScript);
      Log("\tusing " + peerType + " args: " + peerArgs);
      Log("\textra CLASSPATH=" + s_testOutDir);
      Log("");
      if (!proc.Start()) {
        throw new Exception("Failed to start GFXD " + peerType);
      }
      StreamReader outSr = proc.StandardOutput;
      // Wait for GFXD peer to start
      bool started = proc.WaitForExit(MaxWaitMillis);
      int bufSize = 1024;
      char[] outBuf = new char[bufSize];
      int readChars = outSr.Read(outBuf, 0, bufSize);
      Log("Output from '{0} {1}':{2}{3}", s_launcherScript, peerArgs,
        Environment.NewLine, new string(outBuf, 0, readChars));
      outSr.Close();
      if (!started) {
        proc.Kill();
      }
      Assert.IsTrue(started, "Timed out waiting for GemFireXD " + peerType +
                    " to start.{0}Please check the " + peerType + " logs in " +
                    peerDir, Environment.NewLine);
    }

    internal static void StopGFXDServer(DriverType driver)
    {
      if (s_clientPort <= 0) {
        throw new NotSupportedException("No server is running");
      }
      StopGFXDPeer(driver, "server", s_serverDir);

      s_clientPort = -1;
    }

    internal static void StopGFXDPeer(DriverType driver, string peerType,
                                      string peerDir)
    {
      string peerArgs = peerType + " stop -dir=" + peerDir;
      ProcessStartInfo pinfo = new ProcessStartInfo(s_launcherScript, peerArgs);
      //pinfo.CreateNoWindow = true;
      pinfo.UseShellExecute = false;
      pinfo.RedirectStandardOutput = true;
      pinfo.RedirectStandardInput = false;
      pinfo.RedirectStandardError = false;
      Process proc = new Process();
      proc.StartInfo = pinfo;
      if (!proc.Start()) {
        throw new Exception("failed to stop GFXD " + peerType);
      }
      StreamReader outSr = proc.StandardOutput;
      // Wait for GFXD peer to stop
      bool started = proc.WaitForExit(MaxWaitMillis);
      int bufSize = 1024;
      char[] outBuf = new char[bufSize];
      int readChars = outSr.Read(outBuf, 0, bufSize);
      Log("Output from '{0} {1}':{2}{3}", s_launcherScript, peerArgs,
        Environment.NewLine, new string(outBuf, 0, readChars));
      outSr.Close();
      if (!started) {
        proc.Kill();
      }
      Assert.IsTrue(started, "Timed out waiting for GemFireXD " + peerType +
                    " to stop.{0}Please check the " + peerType + " logs in " +
                    peerDir, Environment.NewLine);
    }

    internal static void StopAllGFXDPeers(int mcastPort, int locPort,
                                          string additionalArgs)
    {
      string args = "shut-down-all";
      if (mcastPort > 0) {
        args += " -mcast-port=" + mcastPort + additionalArgs;
      }
      else {
        args += " -locators=localhost[" + locPort + ']' + additionalArgs;
      }
      ProcessStartInfo pinfo = new ProcessStartInfo(s_launcherScript, args);
      //pinfo.CreateNoWindow = true;
      pinfo.UseShellExecute = false;
      pinfo.RedirectStandardOutput = true;
      pinfo.RedirectStandardInput = false;
      pinfo.RedirectStandardError = false;
      Process proc = new Process();
      proc.StartInfo = pinfo;
      if (!proc.Start()) {
        throw new Exception("failed to stop all GFXD peers");
      }
      StreamReader outSr = proc.StandardOutput;
      // Wait for shut-down-all script to finish
      bool started = proc.WaitForExit(MaxWaitMillis);
      int bufSize = 1024;
      char[] outBuf = new char[bufSize];
      int readChars = outSr.Read(outBuf, 0, bufSize);
      Log("Output from '{0} {1}':{2}{3}", s_launcherScript, args,
        Environment.NewLine, new string(outBuf, 0, readChars));
      outSr.Close();
      if (!started) {
        proc.Kill();
      }
      Assert.IsTrue(started, "Timed out waiting for shut-down-all to finish.");
    }

    internal static int GetAvailableTCPPort()
    {
      int testPort;
      bool done = false;
      IPAddress localAddress = IPAddress.Loopback;
      do {
        testPort = GetRandomPort();
        // try to establish a listener on this port
        try {
          TcpListener tcpListener = new TcpListener(localAddress, testPort);
          tcpListener.Start();
          tcpListener.Stop();
          done = true;
        } catch (SocketException) {
          // could not start listener so try again
        }
      } while (!done);
      return testPort;
    }

    internal static int GetRandomPort()
    {
      int startPort = 1100;
      int endPort = 65000;
      return s_rand.Next(startPort, endPort);
    }

    internal static DbConnection CreateConnection(DriverType type, string param,
                                                  bool noServerString)
    {
      DbConnection connection = null;
      if (param != null && param.Length > 0) {
        param = ";" + param;
      }
      else {
        param = string.Empty;
      }
      //s_serverPort = 1527;
      //string localServer = "pc29.pune.gemstone.com";
      string localServer = "localhost";
      switch (type) {
        case DriverType.DB2:
          /*
          if (noServerString) {
            connection = new DB2Connection(param);
          }
          else {
            connection = new DB2Connection("Server=" + localServer + ':' +
              s_serverPort + ";Database=Sample;Authentication=server" +
              ";UserID=app;Password=app" + param);
          }
          */
          connection = null;
          break;
        case DriverType.Mono:
          if (noServerString) {
            //connection = new DB2Connection(param);
          }
          else {
            //connection = new DB2Connection("Database=gfxd;uid=app;pwd=app" +
            //                               param);
          }
          break;
        case DriverType.GFXD:
          if (noServerString) {
           // Console.WriteLine("True Trying to connect with string: " + param + " ...");
            connection = new GFXDClientConnection(param);
          }
          else {
            if (s_clientPortLoc2 == -1)
            {
                connection = new GFXDClientConnection("Server=" + localServer + ':'
                                                    + s_clientPort + param);
            }
            else
            {
              int cport = s_clientPort;
              if (cport == -1) 
              {
                cport = s_savedClientPortBeforeStopping;
              }
              string cs = "Server=" + localServer + ':' + cport + ";secondary-locators=" + localServer 
                          + ':' + s_clientPortLoc2 + param;
              Console.WriteLine("connecting with two locators " + cs);
              //two locators are there
              connection = new GFXDClientConnection(cs);
            }
          }
          break;
        case DriverType.GFXDPeer:
          if (noServerString) {
            //connection = new GFXDPeerConnection(param);
          }
          else {
            //connection = new GFXDPeerConnection("host-data=false;" +
            //  "mcast-port=" + m_serverMcastPort + param);
          }
          break;
      }
      return connection;
    }

    protected virtual DbConnection OpenNewConnection()
    {
      return OpenNewConnection(null, false, null);
    }

    protected DbConnection OpenNewConnection(string param, bool noServerString,
                                             Dictionary<string, string> props)
    {
      DbConnection conn = CreateConnection(m_defaultDriverType, param,
                                           noServerString);
      GFXDConnection gfxdConn = conn as GFXDConnection;
      if (gfxdConn != null) {
          gfxdConn.Open(props);
      }
      else {
        conn.Open();
      }
      return conn;
    }

    protected DbDataAdapter GetDataAdapter(DriverType type)
    {
      switch (type) {
        case DriverType.DB2:
          // return new DB2DataAdapter();
          return null;
        case DriverType.Mono:
          // return new DB2DataAdapter();
          return null;
        case DriverType.GFXD:
        case DriverType.GFXDPeer:
        default:
          return new GFXDDataAdapter();
      }
    }

    protected DbDataAdapter GetDataAdapter()
    {
      return GetDataAdapter(m_defaultDriverType);
    }

    protected DbCommand GetCommandForUpdate(DbConnection conn,
                                            DriverType type)
    {
      DbCommand cmd = conn.CreateCommand();
      switch (type) {
        case DriverType.DB2:
          // TODO: has DB2 driver anything for this?
          break;
        case DriverType.Mono:
          // TODO: has Mono driver anything for this?
          break;
        case DriverType.GFXD:
        case DriverType.GFXDPeer:
          ((GFXDCommand)cmd).ReaderLockForUpdate = true;
          break;
      }
      return cmd;
    }

    protected DbCommand GetCommandForUpdate(DbConnection conn)
    {
      return GetCommandForUpdate(conn, m_defaultDriverType);
    }

    protected DbCommandBuilder GetCommandBuilder(DbDataAdapter adapter,
                                                 DriverType type)
    {
      switch (type) {
        case DriverType.DB2:
          // return new DB2CommandBuilder((DB2DataAdapter)adapter);
          return null;
        case DriverType.Mono:
          // return new DB2CommandBuilder((DB2DataAdapter)adapter);
          return null;
        case DriverType.GFXD:
        case DriverType.GFXDPeer:
        default:
          return new GFXDCommandBuilder((GFXDDataAdapter)adapter);
      }
    }

    protected DbCommandBuilder GetCommandBuilder(DbDataAdapter adapter)
    {
      return GetCommandBuilder(adapter, m_defaultDriverType);
    }

    /* TODO: disabled due to #43188
    protected void UpdateValue(DbDataReader reader, int index,
                               object val, DriverType type)
    {
      switch (type) {
        case DriverType.DB2:
          // TODO: has DB2 driver anything for this?
          break;
        case DriverType.Mono:
          // TODO: has Mono driver anything for this?
          break;
        case DriverType.GFXD:
        case DriverType.GFXDPeer:
          ((GFXDDataReader)reader).UpdateValue(index, val);
          break;
      }
    }

    protected void UpdateValue(DbDataReader reader, int index, object val)
    {
      UpdateValue(reader, index, val, m_defaultDriverType);
    }

    protected void UpdateRow(DbDataReader reader, DriverType type)
    {
      switch (type) {
        case DriverType.DB2:
          // TODO: has DB2 driver anything for this?
          break;
        case DriverType.Mono:
          // TODO: has Mono driver anything for this?
          break;
        case DriverType.GFXD:
        case DriverType.GFXDPeer:
          ((GFXDDataReader)reader).UpdateRow();
          break;
      }
    }

    protected void UpdateRow(DbDataReader reader)
    {
      UpdateRow(reader, m_defaultDriverType);
    }
    */

    internal static bool IsUnix
    {
      get {
        int platformId = (int)Environment.OSVersion.Platform;
        return (platformId == 4 || platformId == 6 || platformId == 128);
      }
    }

    #region Setup and teardown methods

    protected DbConnection m_conn;
    protected DbCommand m_cmd;

    [TestFixtureSetUp]
    public virtual void FixtureSetup()
    {
      InternalSetup(GetType());
      SetDefaultDriverType();
      StartGFXDServer(m_defaultDriverType, "");
    }

    [TestFixtureTearDown]
    public virtual void FixtureTearDown()
    {
        StopGFXDServer(m_defaultDriverType);
      com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(null);
      if (s_logFileWriter != null) {
        s_logFileWriter.close();
        s_logFileWriter = null;
      }
    }

    internal static void InternalSetup(Type type)
    {
      string logFileName;
      if (s_testOutDir != null && s_testOutDir.Length > 0) {
        logFileName = s_testOutDir + '/' + type.FullName + ".txt";
      }
      else {
        logFileName = type.FullName + ".txt";
      }
      // use a java PrintWriter to also set the SanityManager output stream
      // to this file
      s_logFileWriter = new java.io.PrintWriter(logFileName);
      com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(s_logFileWriter);
    }

    [SetUp]
    public virtual void SetUp()
    {
      string[] commonTablesA = CommonTablesToCreate();
      if (commonTablesA.Length > 0) {
        m_conn = OpenNewConnection();
        CreateCommonTables(commonTablesA, m_conn);
      }
    }

    protected virtual string[] CommonTablesToCreate()
    {
      // nothing by default
      return new string[0];
    }

    [TearDown]
    public virtual void TearDown()
    {
      using (DbConnection conn = OpenNewConnection()) {
        DropDBObjects(GetSchemasToDrop(), GetTablesToDrop(),
                      GetFunctionsToDrop(), GetProceduresToDrop(), conn);
      }
      if (m_cmd != null) {
        m_cmd.Dispose();
        m_cmd = null;
      }
      if (m_conn != null && m_conn.State == ConnectionState.Open) {
        m_conn.Close();
        m_conn = null;
      }
      GFXDConnectionPoolManager.ClearAllPools();
    }

    protected virtual string[] GetProceduresToDrop()
    {
      // nothing by default
      return new string[0];
    }

    protected virtual string[] GetFunctionsToDrop()
    {
      // nothing by default
      return new string[0];
    }

    protected virtual string[] GetTablesToDrop()
    {
      return CommonTablesToCreate();
    }

    protected virtual string[] GetSchemasToDrop()
    {
      // nothing by default
      return new string[0];
    }

    internal static void CreateCommonTables(string[] commonTablesA,
                                            DbConnection conn)
    {
      int result;
      List<string> commonTables = new List<string>(commonTablesA);
      DbCommand cmd = conn.CreateCommand();

      if (commonTables.Contains("numeric_family")) {
        // numeric_family table and data
        cmd.CommandText = "create table numeric_family (id int" +
          " primary key, type_int int, type_varchar varchar(100))";
        result = cmd.ExecuteNonQuery();
        Assert.IsTrue(result == 0 || result == -1, "#S1");

        cmd.CommandText = "insert into numeric_family values" +
          " (1, 20000000, 'byte'), (2, 40000000, 'short')," +
          " (3, 60000000, 'int'), (4, 80000000, 'long')";
        result = cmd.ExecuteNonQuery();
        Assert.AreEqual(4, result, "#S2");
      }

      if (commonTables.Contains("datetime_family")) {
        // datetime_family table and data
        cmd.CommandText = "create table datetime_family (id int" +
          " primary key, type_date date, type_time time," +
          " type_datetime timestamp)";
        result = cmd.ExecuteNonQuery();
        Assert.IsTrue(result == 0 || result == -1, "#S3");

        cmd.CommandText = "insert into datetime_family values" +
          " (1, '2004-10-10', '10:10:10', '2004-10-10 10:10:10')," +
          " (2, '2005-10-10', NULL, '2005-10-10 10:10:10')";
        result = cmd.ExecuteNonQuery();
        Assert.AreEqual(2, result, "#S4");
      }

      if (commonTables.Contains("employee")) {
        // employee table and data
        cmd.CommandText = "create table employee (" +
          " id int PRIMARY KEY," +
          " fname varchar (50) NOT NULL," +
          " lname varchar (50) NULL," +
          " dob timestamp NOT NULL," +
          " doj timestamp NOT NULL," +
          " email varchar(50) NULL)";
        result = cmd.ExecuteNonQuery();
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
        result = cmd.ExecuteNonQuery();
        Assert.AreEqual(4, result, "#S6");
      }

      if (commonTables.Contains("emp.credit")) {
        // employee table and data
        cmd.CommandText = "create table emp.credit (" +
          " creditid int," +
          " employeeid int," +
          " credit int NULL," +
          " primary key(creditid)," +
          " constraint fk_ec foreign key(employeeid) references employee(id))";
        result = cmd.ExecuteNonQuery();
        Assert.IsTrue(result == 0 || result == -1, "#S7");
      }
    }

    protected void CleanEmployeeTable()
    {
      using (DbCommand cmd = m_conn.CreateCommand()) {
        cmd.CommandText = "delete from employee where id > 4";
        cmd.ExecuteNonQuery();
      }
    }

    internal static void DropDBObjects(string[] schemas, string[] tables,
      string[] funcs, string[] procs, DbConnection conn)
    {
      using (DbCommand cmd = conn.CreateCommand()) {
        foreach (string proc in procs) {
          cmd.CommandText = "drop procedure " + proc;
          try {
            cmd.ExecuteNonQuery();
          } catch (DbException ex) {
            if (!ex.Message.Contains("42Y55") &&
                !ex.Message.Contains("42Y07")) {
              throw;
            }
          }
        }
        foreach (string func in funcs) {
          cmd.CommandText = "drop function " + func;
          try {
            cmd.ExecuteNonQuery();
          } catch (DbException ex) {
            if (!ex.Message.Contains("42Y55") &&
                !ex.Message.Contains("42Y07")) {
              throw;
            }
          }
        }
        foreach (string table in tables) {
          cmd.CommandText = "drop table " + table;
          try {
            cmd.ExecuteNonQuery();
          } catch (DbException ex) {
            if (!ex.Message.Contains("42Y55") &&
                !ex.Message.Contains("42Y07")) {
              throw;
            }
          }
        }
        foreach (string schema in schemas) {
          cmd.CommandText = "drop schema " + schema + " restrict";
          try {
            cmd.ExecuteNonQuery();
          } catch (DbException ex) {
            if (!ex.Message.Contains("42Y55") &&
                !ex.Message.Contains("42Y07")) {
              throw;
            }
          }
        }
      }
    }

    #endregion
  }
}
