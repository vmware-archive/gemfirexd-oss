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
using System.Text;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Diagnostics;
using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Tests for ConnectionFailover with and without locator.
  /// </summary>
  [TestFixture]
  public class ConnectionFailoverTests : TestBase
  {
    private static int locPort = -1;
    private static int locPort2 = -1;
    private static int mCastPort = -1;
    private int numThreads = 5;

    #region Setup/TearDown methods

    [TestFixtureSetUp]
    public override void FixtureSetup()
    {
      InternalSetup(GetType());
      SetDefaultDriverType();
      // not starting any servers/locators here rather it will be per test
    }

    [TestFixtureTearDown]
    public override void FixtureTearDown()
    {
      // not stopping any servers/locators here rather it will be per test
        com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(null);
      if (s_logFileWriter != null) {
        s_logFileWriter.close();
        s_logFileWriter = null;
      }
    }

    [SetUp]
    public override void SetUp()
    {
      // nothing since we do not create/drop any DB object in this test
    }

    [TearDown]
    public override void TearDown()
    {
      StopServer("/gfxdserver1");
      StopServer("/gfxdserver2");
      StopServer("/gfxdserver3");
      StopServer("/gfxdserver4");
      StopLocator("/gfxdlocator", true);
      StopLocator2("/gfxdlocator2");
    }

    protected void StartServerWithLocatorPort(string args)
    {
      int clientPort = GetAvailableTCPPort();
      string serverDir = s_testOutDir + args;
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      StartGFXDPeer(m_defaultDriverType, "server", serverDir,
                    getlocatorString() ,
                    clientPort, "");
    }

    protected void StartServer(string args, int clientPort)
    {
      if (mCastPort == -1) {
        mCastPort = GetAvailableTCPPort();
      }
      string serverDir = s_testOutDir + args;
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);

      StartGFXDPeer(m_defaultDriverType, "server", serverDir, "-mcast-port=" +
              mCastPort, clientPort, "");
    }

    protected void StopServers()
    {
      StopServer("/gfxdserver2");
      Thread.Sleep(5000);
      StopServer("/gfxdserver3");
    }

    protected void StopServer(string args)
    {
      string serverDir = s_testOutDir + args;
      StopGFXDPeer(m_defaultDriverType, "server", serverDir);
    }

    protected void StartLocator(string args)
    {
      locPort = GetAvailableTCPPort();
      // starting a locator
      initClientPort();
      string locDir = s_testOutDir + args;
      if (Directory.Exists(locDir)) {
        Directory.Delete(locDir, true);
      }
      Directory.CreateDirectory(locDir);
      StartGFXDPeer(m_defaultDriverType, "locator", locDir, string.Empty,
                    s_clientPort, " -peer-discovery-address=localhost" +
                    " -peer-discovery-port=" + locPort + "");
    }

    protected void StartTwoLocator(string args)
    {
      locPort = GetAvailableTCPPort();
      locPort2 = GetAvailableTCPPort();
      // starting a locator
      initClientPort();

      string locDir = s_testOutDir + args;
      if (Directory.Exists(locDir))
      {
        Directory.Delete(locDir, true);
      }
      Directory.CreateDirectory(locDir);
      StartGFXDPeer(m_defaultDriverType, "locator", locDir, string.Empty,
                    s_clientPort, " -peer-discovery-address=localhost" +
                    getlocatorString() +
                    " -peer-discovery-port=" + locPort + "");

      string locDir2 = s_testOutDir + args + "2";
      if (Directory.Exists(locDir2))
      {
        Directory.Delete(locDir2, true);
      }
      Directory.CreateDirectory(locDir2);
      s_clientPortLoc2 = GetAvailableTCPPort();
      StartGFXDPeer(m_defaultDriverType, "locator", locDir2, string.Empty,
                    s_clientPortLoc2, " -peer-discovery-address=localhost" +
                    getlocatorString() +
                    " -peer-discovery-port=" + locPort2 + "");
    }

    private string getlocatorString()
    {
      string locString = "";
      if (locPort != -1) {
        locString = " -locators=localhost[" + locPort + ']';
        if (locPort2 != -1) {
          locString += "," + "localhost[" + locPort2 + ']';
        }
      }
      Console.WriteLine("getlocatorString: " + locString);
      return locString;
    }

    protected void StopLocator(string args)
    {
      StopLocator(args, false);      
    }

    protected void StopLocator(string args, bool force)
    {
      if (!force && s_clientPort <= 0) {
        throw new NotSupportedException("No locator is running");
      }
      string locDir = s_testOutDir + args;
      StopGFXDPeer(m_defaultDriverType, "locator", locDir);
      s_savedClientPortBeforeStopping = s_clientPort;
      s_clientPort = -1;
    }

    protected void StopLocator2(string args)
    {
      string locDir = s_testOutDir + args ;
      StopGFXDPeer(m_defaultDriverType, "locator", locDir);
      locPort2 = -1;
      s_clientPortLoc2 = -1;
    }

    protected void InitializeClientPort()
    {
      initClientPort();
      initClientPort1();
      initClientPort2();
    }

    private void UpdateTable(object connection)
    {      
      DbConnection conn = (DbConnection)connection;
      DbCommand cmd = conn.CreateCommand();

      for (int i = 0; i < 1000; i++) {
        cmd.CommandText = "UPDATE ORDERS SET VOL=? WHERE ID = ?";
        Log(cmd.CommandText);
        cmd.Parameters.Clear();
        cmd.Parameters.Add(i + 1001);
        cmd.Parameters.Add(i);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
      }
      cmd.Dispose();
      Log("Updation in table done.");
    }

    private void UpdateTable2()
    {     
      for (int i = 0; i < 1000; i++)
      {
        DbConnection conn = (DbConnection)OpenNewConnection("Maximum Pool Size=10", false, null);
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE ORDERS SET VOL=? WHERE ID = ?";
        Log(cmd.CommandText);
        cmd.Parameters.Clear();
        cmd.Parameters.Add(i + 1001);
        cmd.Parameters.Add(i);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
        conn.Close();
      }
      Log("Updation in table done.");
    }

    private void UpdateTable3()
    {
      try
      {
        DbConnection conn = (DbConnection)OpenNewConnection();
        for (int i = 0; i < 1000; i++)
        {
          DbCommand cmd = conn.CreateCommand();
          cmd.CommandText = "UPDATE ORDERS SET VOL=? WHERE ID = ?";
          Log(cmd.CommandText);
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i + 1001);
          cmd.Parameters.Add(i);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
          cmd.Dispose();
        }
        conn.Close();
      }
      catch (Exception ex) {
        Console.WriteLine("hitesh got " + ex);
      }
      Log("Updation in table done.");
    }

    #endregion

    /// <summary>
    /// two pool test
    /// </summary>
    [Test]
    public void TestTwoPool()
    {
      if(!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
     Console.WriteLine("Pool test TestTwoPool");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      DbConnection conn1 = OpenNewConnection();
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection("secondary-locators=localserver:96897", false, null);
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());

      
      //that will force new connection
      DbConnection conn3 = OpenNewConnection("secondary-locators=localserver:96897", false, null);
      for (int i = 10; i < 100; i++)
      {
        DbCommand cmd = conn3.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str2 = "Char " + i;
        cmd.Parameters.Add(str2);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
      }


      conn2.Close();
      conn3.Close();
      
      //pool1 should have one connection n=only
      Assert.AreEqual(1, ((GFXDClientConnection)conn1).PoolConnectionCount);

      //pool2 should  have one connection
      Assert.AreEqual(2, ((GFXDClientConnection)conn2).PoolConnectionCount);

      StopServer("/gfxdserver1");
      StopLocator2("/gfxdlocator2");
    }

    /// <summary>
    /// stops first locator, it return connection to pool
    /// </summary>
    [Test]
    public void TestLocatorStop1()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestLocatorStop1");
      // start the locator and 1 server first and let client connect with this server only.      

      StartTwoLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());

      conn2.Close();

      //stopped first locator
      StopLocator("/gfxdlocator");

      //that will force new connection
      DbConnection conn3 = OpenNewConnection();
      for (int i = 10; i < 100; i++)
      {
        DbCommand cmd = conn3.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str2 = "Char " + i;
        cmd.Parameters.Add(str2);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
      }


      conn2.Close();
      conn3.Close();

      //pool should  have one connection
      Assert.AreEqual(1, ((GFXDClientConnection)conn2).PoolConnectionCount);

      StopServer("/gfxdserver1");
      StopLocator2("/gfxdlocator2");
    }

    /// <summary>
    /// stops first locator. But it didn't return connection(conn2) to pool
    /// </summary>
    [Test]
    public void TestLocatorStop2()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Console.WriteLine("Pool test TestLocatorStop2");
      StartTwoLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());

      //stopped first locator
      StopLocator("/gfxdlocator");

      //that will force new connection
      DbConnection conn3 = OpenNewConnection();
      for (int i = 10; i < 100; i++)
      {
        DbCommand cmd = conn3.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str2 = "Char " + i;
        cmd.Parameters.Add(str2);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
      }
      

      conn2.Close();
      conn3.Close();

      //pool should  have two connection
      Assert.AreEqual(2, ((GFXDClientConnection)conn2).PoolConnectionCount);

      StopServer("/gfxdserver1");
      StopLocator2("/gfxdlocator2");
    }

    /// <summary>
    /// test server fail over
    /// </summary>
    [Test]
    public void TestServerFailed()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Console.WriteLine("Pool test TestServerFailed");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());


      StopServer("/gfxdserver1");
      Console.WriteLine("server closed");
      Thread.Sleep(2000);
      bool gotException = false;
      try
      {
        for (int i = 10; i < 100; i++)
        {
          DbCommand cmd = conn2.CreateCommand();

          cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str2 = "Char " + i;
          cmd.Parameters.Add(str2);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
      }
      catch (GFXDException ex)
      {
        Console.WriteLine("got exception " + ex.Message);
        gotException = true;
      }

      Assert.AreEqual(true, gotException);
      conn2.Close();
      
      //pool should not have any single connection
      Assert.AreEqual(0, ((GFXDClientConnection)conn2).PoolConnectionCount);
      Assert.AreEqual(1, ((GFXDClientConnection)conn2).GCReferenceCount, "We should have released GC reference 1 (in driver).");
      StopLocator("/gfxdlocator");
    }

    /// <summary>
    /// test server fail over
    /// </summary>
    [Test]
    public void TestServerFailover()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Console.WriteLine("Pool test TestServerFailover");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      StartServerWithLocatorPort("/gfxdserver2");

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());
      

      StopServer("/gfxdserver1");

      Thread.Sleep(2000);

      for (int i = 10; i < 100; i++)
      {
        DbCommand cmd = conn2.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str2 = "Char " + i;
        cmd.Parameters.Add(str2);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());        
      }

      Assert.AreEqual(1, ((GFXDClientConnection)conn2).PoolConnectionCount);
      conn2.Close();

      // Stop the server where client-connection was established for failover.


      StopServer("/gfxdserver2");

      StopLocator("/gfxdlocator");
    }

    /// <summary>
    /// test connection idle time
    /// </summary>
    [Test]
    public void TestConnectionIdleTime()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Console.WriteLine("Pool test TestConnectionIdleTime");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());
      //conn2.Close() not closing the connection

      object storeConnectionObject = ((GFXDClientConnection)conn2).RealPooledConnection;

      Assert.AreEqual(2, ((GFXDClientConnection)conn2).GCReferenceCount, "We should have GC reference count 2(one in pool and one in driver).");

      conn2.Close();

      Assert.AreEqual(1, ((GFXDClientConnection)conn2).GCReferenceCount, "We should have GC reference count 1(one in pool).");

      //180 second is default
      Thread.Sleep(200000);

      Assert.AreEqual( 0, ((GFXDClientConnection)conn2).PoolConnectionCount, "Connection count should be zero");
      Assert.AreEqual(0, ((GFXDClientConnection)conn2).GCReferenceCount , "We should have released GC reference count.");

      DbConnection conn3 = OpenNewConnection();
      
      //now there should be new connection
      Assert.AreNotEqual(storeConnectionObject, ((GFXDClientConnection)conn3).RealPooledConnection);
      conn3.Close();
    

      // Stop the server where client-connection was established for failover.
      StopServer("/gfxdserver1");

      StopLocator("/gfxdlocator");
    }


    /// <summary>
    /// test connection idle time which modified
    /// </summary>
    [Test]
    public void TestConnectionIdleTimeM()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Environment.SetEnvironmentVariable("IDLE_TIMEOUT", "100");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection("Connection Lifetime=10", false, null))
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection("Connection Lifetime=10", false, null);
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());
      //conn2.Close() not closing the connection

      object storeConnectionObject = ((GFXDClientConnection)conn2).RealPooledConnection;

      conn2.Close();

      //10 second is set
      Thread.Sleep(31000);

      Assert.IsTrue(((GFXDClientConnection)conn2).PoolConnectionCount == 0);


      DbConnection conn3 = OpenNewConnection("Connection Lifetime=10", false, null);

      //now there should be new connection
      Assert.AreNotEqual(storeConnectionObject, ((GFXDClientConnection)conn3).RealPooledConnection);
      conn3.Close();


      // Stop the server where client-connection was established for failover.
      StopServer("/gfxdserver1");

      StopLocator("/gfxdlocator");

      Environment.SetEnvironmentVariable("IDLE_TIMEOUT", null);
    }

    /// <summary>
    /// test pool max connection limit
    /// 
    /// </summary>
    [Test]
    public void TestPoolMaxConnections()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolMaxConnections");
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", "1");
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      Console.WriteLine("test starts");
      
      using (DbConnection conn1 = OpenNewConnection())
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());
      //conn2.Close() not closing the connection

      bool gotexception = false;
      long st = DateTime.Now.Ticks;
      try
      {
        DbConnection conn3 = OpenNewConnection();
      }
      catch (GFXDException sx)
      {
        Console.WriteLine("got exception " + sx.Message);
        gotexception = true;
      }

      Assert.IsTrue(gotexception);
      long end = DateTime.Now.Ticks;
      //default
      Assert.GreaterOrEqual(end, st + 15 * TimeSpan.TicksPerSecond);
      // Stop the server where client-connection was established for failover.
      StopServer("/gfxdserver1");

      StopLocator("/gfxdlocator");
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", null);
    }

    /// <summary>
    /// test pool max connection limit
    /// 
    /// </summary>
    [Test]
    public void TestPoolMaxConnections2()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolMaxConnections");
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", "1");
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/sqlflocator");

      StartServerWithLocatorPort("/sqlfserver1");

      Console.WriteLine("test starts");

      using (DbConnection conn1 = OpenNewConnection("Connect Timeout=30", false, null))
      {
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      DbConnection conn2 = OpenNewConnection("Connect Timeout=30", false, null);
      DbCommand cmd2 = conn2.CreateCommand();

      cmd2.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

      cmd2.Parameters.Add(1);
      cmd2.Parameters.Add(1);
      string str = "Char " + 1;
      cmd2.Parameters.Add(str);
      Assert.AreEqual(1, cmd2.ExecuteNonQuery());
      //conn2.Close() not closing the connection

      bool gotexception = false;
      long st = DateTime.Now.Ticks;
      try
      {
        DbConnection conn3 = OpenNewConnection("Connect Timeout=30", false, null);
      }
      catch (GFXDException sx)
      {
        Console.WriteLine("got exception " + sx.Message);
        gotexception = true;
      }

      Assert.IsTrue(gotexception);

      long end = DateTime.Now.Ticks;

      Console.WriteLine("hitesh gote " + end + " st " + (st + 30 * TimeSpan.TicksPerSecond));

      Assert.GreaterOrEqual(end, st + 30 * TimeSpan.TicksPerSecond);

      // Stop the server where client-connection was established for failover.
      StopServer("/sqlfserver1");

      StopLocator("/sqlflocator");
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", null);
    }

    [Test]
    public void TestPoolConnectionsGC()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolConnections");
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      object storeConnectionObject = null;

      Dictionary<object, bool> conns = new Dictionary<object, bool>();
      Console.WriteLine("test starts");
      using (DbConnection conn1 = OpenNewConnection()) 
      {
        GFXDClientConnection scc = (GFXDClientConnection)conn1;
        storeConnectionObject = scc.RealPooledConnection;
        conns[scc.RealPooledConnection] = true;
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }
      //GC check after connection close
      for (int i = 0; i < 1000; i++)
      {
        DbConnection conn = OpenNewConnection();
        
        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str = "Char " + i;
        cmd.Parameters.Add(str);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
        conn.Close();
      }

      bool gotExpectedException = false;
      string excepMsg = "";
      //GC check after without connection close
      try
      {
        for (int i = 1001; i < 5000; i++)
        {
          DbConnection conn = OpenNewConnection();

          DbCommand cmd = conn.CreateCommand();

          cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
      }
      catch (Exception ex) {
        excepMsg = ex.Message;
        if(ex.Message.IndexOf("All connections are used")  > 0)
          gotExpectedException = true;
      }

      Assert.IsTrue(gotExpectedException, "didn't get expected exception " + excepMsg); 
      
      // Stop the server where client-connection was established for failover.
      StopServer("/sqlfserver1");

      StopLocator("/sqlflocator");
    }

    public void TestPoolConnections()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolConnections");
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/sqlflocator");

      StartServerWithLocatorPort("/sqlfserver1");

      object storeConnectionObject = null;

      Dictionary<object, bool> conns = new Dictionary<object, bool>();
      Console.WriteLine("test starts");
      using (DbConnection conn1 = OpenNewConnection())
      {
        GFXDClientConnection scc = (GFXDClientConnection)conn1;
        storeConnectionObject = scc.RealPooledConnection;
        conns[scc.RealPooledConnection] = true;
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      for (int i = 0; i < 10; i++)
      {
        DbConnection conn = OpenNewConnection();

        GFXDClientConnection scc = (GFXDClientConnection)conn;
        Log("Conenctions " + scc.PoolConnectionCount + " stored one " + storeConnectionObject.GetHashCode() + " newone:" + scc.RealPooledConnection.GetHashCode());
        conns[scc.RealPooledConnection] = true;

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";

        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str = "Char " + i;
        cmd.Parameters.Add(str);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
        conn.Close();
        conn.Close();//extra close
      }

      using (DbConnection c2 = OpenNewConnection())
      {
        GFXDClientConnection scc = (GFXDClientConnection)c2;
        Assert.AreEqual(1, scc.PoolConnectionCount);
        Assert.AreEqual(conns.Count, scc.PoolConnectionCount);
        c2.Close();
      }

      // Stop the server where client-connection was established for failover.
      StopServer("/gfxdserver1");

      StopLocator("/gfxdlocator");
    }

    [Test]
    public void TestPoolConnectionsWithTx()
    {
      if (!isRunningWithPool())
        return;
      // start the locator and 1 server first and let client connect with this server only.      
      Console.WriteLine("Pool test TestPoolConnectionsWithTx");
      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      object storeConnectionObject = null;

      Dictionary<object, bool> conns = new Dictionary<object, bool>();
      Console.WriteLine("test starts");
      using (DbConnection conn1 = OpenNewConnection())
      {
        GFXDClientConnection scc = (GFXDClientConnection)conn1;
        storeConnectionObject = scc.RealPooledConnection;
        conns[scc.RealPooledConnection] = true;
        //create table
        DbCommand cmd = conn1.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();
        conn1.Close();
      }

      for (int i = 0; i < 10; i++)
      {
        DbConnection conn = OpenNewConnection();

        GFXDClientConnection scc = (GFXDClientConnection)conn;
        Log("Conenctions " + scc.PoolConnectionCount + " stored one " + storeConnectionObject.GetHashCode() + " newone:" + scc.RealPooledConnection.GetHashCode());
        conns[scc.RealPooledConnection] = true;

        GFXDTransaction tran = (GFXDTransaction)conn.BeginTransaction(IsolationLevel
                                                       .ReadCommitted);

        DbCommand cmd = new GFXDCommand("INSERT INTO ORDERS VALUES (?, ?, ?)" , (GFXDConnection)conn);
        cmd.Prepare();
        
        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i);
        string str = "Char " + i;
        cmd.Parameters.Add(str);
        Assert.AreEqual(1, cmd.ExecuteNonQuery());
        tran.Commit();
        conn.Close();
      }

      using (DbConnection c2 = OpenNewConnection())
      {
        GFXDClientConnection scc = (GFXDClientConnection)c2;
        Assert.AreEqual(1, scc.PoolConnectionCount);
        Assert.AreEqual(conns.Count, scc.PoolConnectionCount);
        c2.Close();
      }

      // Stop the server where client-connection was established for failover.
      StopServer("/gfxdserver1");

      StopLocator("/gfxdlocator");
    }

    [Test]
    public void TestPoolConnectionsWithMultipleThreads()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolConnectionsWithMultipleThreads");
     // Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", "10");
      // start the locator and servers and let client connect with the first server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      StartServerWithLocatorPort("/gfxdserver2");

      StartServerWithLocatorPort("/gfxdserver3");

      StartServerWithLocatorPort("/gfxdserver4");

      using (DbConnection conn = OpenNewConnection("Maximum Pool Size=10", false, null))
      {
        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++)
        {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");
        Stopwatch sw = new Stopwatch();
        sw.Start();
        int nthreads = 25;
        Thread[] ts = new Thread[nthreads];

        for (int i = 0; i < nthreads; i++)
        {
          ThreadStart tsm = new ThreadStart(UpdateTable2);
          ts[i] = new Thread(tsm);

          Log(String.Format("Start order table update thread. TID: [{0}]", ts[i].ManagedThreadId));
          ts[i].Start();
        }

        for (int i = 0; i < nthreads; i++)
          ts[i].Join();
        Console.WriteLine("test took time " + sw.ElapsedMilliseconds);
        //total connection should be less than 10, POOL_MAX_CONNECTION limit
        Assert.IsTrue(((GFXDClientConnection)conn).PoolConnectionCount <= 10);

        Thread ts1 = new Thread(new ThreadStart(StopServers));
        ts1.Start();
        Thread.Sleep(10);


        cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 0 and VOL < 1000";
        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();
        int rows = 0;
        do
        {
          while (reader.Read())
          {
            ++rows;
          }
          Assert.AreEqual(0, rows, "#1 No rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());
        reader.Close();

        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 1000 and VOL < 2000";
        Log(cmd.CommandText);
        reader = cmd.ExecuteReader();
        rows = 0;
        do
        {
          while (reader.Read())
          {
            ++rows;
          }
          Assert.AreEqual(999, rows, "#1 Multiple rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());

        conn.Close();

        StopServer("/gfxdserver4");

        StopLocator("/gfxdlocator");
      }
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", null);
    }

    //hitesh need to look
    [Test]
    public void TestPoolConnectionsWithMultipleThreads2()
    {
      if (!isRunningWithPool())
        return;
      Console.WriteLine("Pool test TestPoolConnectionsWithMultipleThreads");
     // Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", "10");
      // start the locator and servers and let client connect with the first server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      StartServerWithLocatorPort("/gfxdserver2");

      StartServerWithLocatorPort("/gfxdserver3");

      StartServerWithLocatorPort("/gfxdserver4");

      using (DbConnection conn = OpenNewConnection())
      {
        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++)
        {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");
        Stopwatch sw = new Stopwatch();
        sw.Start();
        int nthreads = 25;
        Thread[] ts = new Thread[nthreads];

        for (int i = 0; i < nthreads; i++)
        {
          ThreadStart tsm = new ThreadStart(UpdateTable3);
          ts[i] = new Thread(tsm);

          Log(String.Format("Start order table update thread. TID: [{0}]", ts[i].ManagedThreadId));
          ts[i].Start();
        }

        for (int i = 0; i < nthreads; i++)
        {
          Console.WriteLine("thread returned " + i);
          ts[i].Join();
        }
        Console.WriteLine("test took time " + sw.ElapsedMilliseconds);
        //total connection should be less than 10, POOL_MAX_CONNECTION limit
        Assert.AreEqual(((GFXDClientConnection)conn).PoolConnectionCount, nthreads +1);

        Thread ts1 = new Thread(new ThreadStart(StopServers));
        ts1.Start();
        Thread.Sleep(10);


        cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 0 and VOL < 1000";
        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();
        int rows = 0;
        do
        {
          while (reader.Read())
          {
            ++rows;
          }
          Assert.AreEqual(0, rows, "#1 No rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());
        reader.Close();

        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 1000 and VOL < 2000";
        Log(cmd.CommandText);
        reader = cmd.ExecuteReader();
        rows = 0;
        do
        {
          while (reader.Read())
          {
            ++rows;
          }
          Assert.AreEqual(999, rows, "#1 Multiple rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());

        conn.Close();

        StopServer("/gfxdserver4");

        StopLocator("/gfxdlocator");
      }
      Environment.SetEnvironmentVariable("POOL_MAX_CONNECTION", null);
    }

    /// <summary>
    /// Test with locator and replicated table.
    /// </summary>
    [Test]
    public void TestConnectionFailover1_WithLocator()
    {
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      // Open a new connection to the network server
      using (DbConnection conn = OpenNewConnection()) {
        // After client-server connection is established start the other server.
        StartServerWithLocatorPort("/gfxdserver2");        

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");

        // Stop the server where client-connection was established for failover.
        StopServer("/gfxdserver1");        

        cmd.CommandText = "SELECT * from ORDERS";
        Log(cmd.CommandText);

        DbDataReader reader = cmd.ExecuteReader();

        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());
          Assert.AreEqual(1000, rows, "#1 rows shud be 1000");
          ++results;
          rows = 0;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(1, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "Drop Table ORDERS ";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        conn.Close();

        //Close the remaining server and locator.
        StopServer("/gfxdserver2");

        StopLocator("/gfxdlocator");
      }
    }

    /// <summary>
    /// Test with locator and partitioned table.
    /// </summary>
    [Test]
    public void TestConnectionFailover2_WithLocator()
    {
      // start the locator and 1 server first and let client connect with this server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      // Open a new connection to the network server
      using (DbConnection conn = OpenNewConnection()) {
        // After client-server connection is established start the other servers.
        StartServerWithLocatorPort("/gfxdserver2");

        StartServerWithLocatorPort("/gfxdserver3");

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "partition by Primary Key redundancy 1 ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");

        // Stop the server where client-connection was established for failover.
        StopServer("/gfxdserver1");        

        cmd.CommandText = "SELECT * from ORDERS";
        Log(cmd.CommandText);

        DbDataReader reader = cmd.ExecuteReader();

        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());
          Assert.AreEqual(1000, rows, "#1 rows shud be 1000");
          ++results;
          rows = 0;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(1, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "Drop Table ORDERS ";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        conn.Close();

        //Close the remaining servers and locator.

        StopServer("/gfxdserver2");

        StopServer("/gfxdserver3");

        StopLocator("/gfxdlocator");
      }
    }

    /// <summary>
    /// Test without locator and partitioned table.
    /// </summary>
    [Test]
    public void TestConnectionFailover2_WithoutLocator()
    {
      InitializeClientPort();

      // start first 2 servers and let client connect with the first one only.

      StartServer("/gfxdserver1", s_clientPort);

      StartServer("/gfxdserver2", s_clientPort1);

      // Open a new connection to the network server
      using (DbConnection conn = OpenNewConnection()) {
        // After client-server connection is established start the other server.
        StartServer("/gfxdserver3", s_clientPort2);

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "partition by Primary Key redundancy 1 ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");

        // Stop the server where client-connection was established for failover.
        StopServer("/gfxdserver1");

        cmd.CommandText = "SELECT * from ORDERS";
        Log(cmd.CommandText);

        DbDataReader reader = cmd.ExecuteReader();

        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());
          Assert.AreEqual(1000, rows, "#1 rows shud be 1000");
          ++results;
          rows = 0;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(1, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "Drop Table ORDERS ";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        conn.Close();

        //Close the remaining servers.
        StopServer("/gfxdserver2");

        StopServer("/gfxdserver3");

        s_clientPort = -1;
        s_clientPort1 = -1;
        s_clientPort2 = -1;
      }
    }

    /// <summary>
    /// Test without locator and replcated table.
    /// </summary>
    [Test]
    public void TestConnectionFailover1_WithoutLocator()
    {
      InitializeClientPort();

      // start first 2 servers and let client connect with the first one only.

      StartServer("/gfxdserver1", s_clientPort);

      StartServer("/gfxdserver2", s_clientPort1);

      // Open a new connection to the network server
      using (DbConnection conn = OpenNewConnection()) {
        // After client-server connection is established start the other server.
        StartServer("/gfxdserver3", s_clientPort2);

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");

        // Stop the server where client-connection was established for failover.        
        StopServer("/gfxdserver1");        

        cmd.CommandText = "SELECT * from ORDERS";
        Log(cmd.CommandText);

        DbDataReader reader = cmd.ExecuteReader();

        int rows = 0;
        int results = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Log(rows.ToString());
          Assert.AreEqual(1000, rows, "#1 rows shud be 1000");
          ++results;
          rows = 0;
        } while (reader.NextResult());
        Log(results.ToString());
        Assert.AreEqual(1, results, "#2 result sets shud be returned");
        reader.Close();

        cmd.CommandText = "Drop Table ORDERS ";
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        conn.Close();

        //Close the remaining server.
        StopServer("/gfxdserver2");

        StopServer("/gfxdserver3");

        s_clientPort = -1;
        s_clientPort1 = -1;
        s_clientPort2 = -1;
      }
    }

    /// <summary>
    /// Test connection failover with threads.
    /// </summary>
    [Test]
    public void TestConnectionFailover_WithMultipleThreads()
    {
      // start the locator and servers and let client connect with the first server only.      

      StartLocator("/gfxdlocator");

      StartServerWithLocatorPort("/gfxdserver1");

      StartServerWithLocatorPort("/gfxdserver2");

      StartServerWithLocatorPort("/gfxdserver3");

      using (DbConnection conn = OpenNewConnection()) {
        // After client-server connection is established start the other server.
        StartServerWithLocatorPort("/gfxdserver4");

        DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = "create table ORDERS (ID int primary key, " +
          "VOL int NOT NULL unique, SECURITY_ID varchar(10)) " +
          "REPLICATE ";

        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ORDERS VALUES (?, ?, ?)";
        for (int i = 0; i < 1000; i++) {
          cmd.Parameters.Clear();
          cmd.Parameters.Add(i);
          cmd.Parameters.Add(i);
          string str = "Char " + i;
          cmd.Parameters.Add(str);
          Log(cmd.CommandText);
          Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }
        Log("Insertion in table done.");

        // Stop the server where client-connection was established.
        StopServer("/gfxdserver1");

        Thread[] ts = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
          ts[i] = new Thread(new ParameterizedThreadStart(UpdateTable));

          Log(String.Format("Start order table update thread. TID: [{0}]", ts[i].ManagedThreadId));
          ts[i].Start(conn);
          Thread.Sleep(10);
        }
        for (int i = 0; i < numThreads; i++)
          ts[i].Join();

        Thread ts1 = new Thread(new ThreadStart(StopServers));
        ts1.Start();        
        Thread.Sleep(10);

        cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 0 and VOL < 1000";
        Log(cmd.CommandText);
        DbDataReader reader = cmd.ExecuteReader();
        int rows = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Assert.AreEqual(0, rows, "#1 No rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());
        reader.Close();

        cmd.CommandText = "SELECT * from ORDERS WHERE VOL > 1000 and VOL < 2000";
        Log(cmd.CommandText);
        reader = cmd.ExecuteReader();
        rows = 0;
        do {
          while (reader.Read()) {
            ++rows;
          }
          Assert.AreEqual(999, rows, "#1 Multiple rows shud be returned");
          Log(rows.ToString());
        } while (reader.NextResult());

        conn.Close();

        StopServer("/gfxdserver4");

        StopLocator("/gfxdlocator");
      }
    }

  }
}

