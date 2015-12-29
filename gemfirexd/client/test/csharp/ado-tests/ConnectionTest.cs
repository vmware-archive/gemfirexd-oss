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

using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Ported from Mono's SqlConnectionTest for System.Data.SqlClient.
  /// </summary>
  [TestFixture]
  public class ConnectionTest : TestBase
  {
    [Test]
    public void Open()
    {
      m_conn = OpenNewConnection();
      Assert.AreEqual(ConnectionState.Open, m_conn.State, "#1");
      try {
        m_conn.Open();
        Assert.Fail("#2");
      } catch (DbException ex) {
        Assert.IsNull(ex.InnerException, "#3");
        Assert.IsNotNull(ex.Message, "#4");
        Assert.IsTrue(ex.Message.Contains("08001"), "#5");
      }
    }

    [Test]
    public void Open_ConnectionString_DatabaseInvalid()
    {
      try {
        m_conn = OpenNewConnection("database=invalidDB", false, null);
        Assert.Fail("#1");
      } catch (DbException ex) {
        // Cannot open database "invalidDB" requested
        // by the login. The login failed
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("invalidDB"), "#4: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("XJ028"), "#5: " + ex.Message);
      }
    }

    // needs to be in a separate AuthenticationTest suite that will start
    // the server with proper users etc.
    //[Test]
    public void Open_ConnectionString_LoginInvalid()
    {
      // login invalid
      try {
        m_conn = OpenNewConnection("user=invalidLogin", false, null);
        Assert.Fail("#1");
      } catch (DbException ex) {
        // Login failed for user 'invalidLogin'
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("'invalidLogin'"),
                      "#4: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("08004"), "#5");
      }
    }

    // needs to be in a separate AuthenticationTest suite that will start
    // the server with proper users etc.
    //[Test]
    public void Open_ConnectionString_PasswordInvalid()
    {
      // password invalid
      try {
        m_conn = OpenNewConnection("password=invalidPassword", false, null);
        Assert.Fail("#1");
      } catch (DbException ex) {
        // Login failed for user '...'
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("08004"), "#4: " + ex.Message);
      }
    }

    [Test]
    public void Open_ConnectionString_ServerInvalid()
    {
      // server invalid
      try {
        m_conn = OpenNewConnection("server=invalidServerName:1000", true, null);
        Assert.Fail("#1");
      } catch (DbException ex) {
        // An error has occurred while establishing a
        // connection to the server...
        Assert.IsNotNull(ex.Message, "#2");
        Assert.IsTrue(ex.Message.Contains("08001") || ex.Message
                      .Contains("08006"), "#3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("invalidServerName") && ex.Message
                      .Contains("1000"), "#4: " + ex.Message);
      }
    }

    [Test]
    public void Close()
    {
      m_conn = OpenNewConnection();
      m_conn.Close();

      Assert.AreEqual(ConnectionState.Closed, m_conn.State, "#1");

      m_conn.Close();
    }

    [Test]
    public void ChangeDatabase_DatabaseName_DoesNotExist()
    {
      m_conn = OpenNewConnection();
      string database = m_conn.Database;
      try {
        m_conn.ChangeDatabase("doesnotexist");
        Assert.Fail("#1");
      } catch (DbException ex) {
        // Could not locate entry in sysdatabases for
        // database 'doesnotexist'. No entry found with
        // that name. Make sure that the name is entered
        // correctly.
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("doesnotexist"), "#4");
        Assert.IsTrue(ex.Message.Contains("XJ028"), "#5");
        Assert.AreEqual(database, m_conn.Database, "#6");
      }
    }

    [Test]
    public void InterfaceTransactionTest()
    {
      m_conn = OpenNewConnection();
      IDbCommand command = m_conn.CreateCommand();
      Assert.AreEqual(m_conn, command.Connection, "unexpected connection");
      command.Connection = null;
      Assert.AreEqual(null, command.Connection, "Connection should be null");
      Assert.AreEqual(null, command.Transaction, "Transaction should be null");
    }

    [Test]
    public void BeginTransaction()
    {
      m_conn = OpenNewConnection();

      IDbTransaction trans = m_conn.BeginTransaction();
      Assert.AreSame(m_conn, trans.Connection, "#A1");
      Assert.AreEqual(IsolationLevel.ReadCommitted, trans.IsolationLevel, "#A2");
      trans.Rollback();

      trans = m_conn.BeginTransaction();
      try {
        m_conn.BeginTransaction();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // no support for parallel transactions
        Assert.IsNull(ex.InnerException, "#B2");
        Assert.IsNotNull(ex.Message, "#B3");
        Assert.IsTrue(ex.Message.Contains("XSTA2"), "#B4");
      } finally {
        trans.Rollback();
      }

      try {
        trans = m_conn.BeginTransaction();
        trans.Rollback();
        trans = m_conn.BeginTransaction();
        trans.Commit();
        trans = m_conn.BeginTransaction();
      } finally {
        trans.Rollback();
      }
    }

    [Test]
    public void ConnectionString_Connection_Open()
    {
      m_conn = OpenNewConnection();
      string connectionString = m_conn.ConnectionString;
      try {
        m_conn.ConnectionString = "server=localhost;database=tmp;";
        Assert.Fail("#1");
      } catch (DbException ex) {
        // Not allowed to change the 'ConnectionString'
        // property. The connection's current state is open
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("08001"), "#4");
        Assert.AreEqual(connectionString, m_conn.ConnectionString, "#5");
      }
    }

    [Test]
    public void ServerVersionTest()
    {
      m_conn = OpenNewConnection();
      m_conn.Close();
      // Test exception is thrown if Connection is CLOSED
      try {
        string s = ((DbConnection)m_conn).ServerVersion;
        Assert.Fail("#A1:" + s);
      } catch (DbException ex) {
        Assert.IsNull(ex.InnerException, "#A2");
        Assert.IsNotNull(ex.Message, "#A3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#A4");
      }

      // Test if Release Version is as per specification.
      m_conn.Open();
      String[] version = ((DbConnection)m_conn).ServerVersion.Split('.');
      Assert.AreEqual(2, version[0].Length,
                      "#B1 The Major release shud be exactly 2 characters");
      Assert.AreEqual(2, version[1].Length,
                      "#B2 The Minor release shud be exactly 2 characters");
      Assert.AreEqual(4, version[2].Length,
                      "#B3 The Release version should be exactly 4 digits");
    }

    [Test]
    public void ConnDispose()
    {
      m_conn = OpenNewConnection();
      m_conn.Dispose();
      Assert.AreEqual(ConnectionState.Closed, m_conn.State, "#A1");
      m_conn.Dispose();
      Assert.AreEqual(ConnectionState.Closed, m_conn.State, "#A2");
    }

    [Test]
    public void AutoCommitTest()
    {
      m_conn = OpenNewConnection();
      GFXDConnection conn = m_conn as GFXDConnection;
      if (conn != null) {
        Assert.False(conn.AutoCommit, "#1");
        conn.AutoCommit = true;
        Assert.True(conn.AutoCommit, "#2");
        conn.Close();
        try {
          conn.AutoCommit = false;
        } catch (DbException ex) {
          // cannot change property 'AutoCommit' is connection is closed
          Assert.IsNull(ex.InnerException, "#3");
          Assert.IsNotNull(ex.Message, "#4");
          Assert.IsTrue(ex.Message.Contains("08003"), "#5");
        }
      }
    }
  }
}
