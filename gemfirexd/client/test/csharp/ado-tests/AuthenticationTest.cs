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
  /// Tests for checking builtin and LDAP authentication schemes.
  /// </summary>
  [TestFixture]
  public class AuthenticationTest : TestBase
  {
    #region Setup/TearDown methods

    [TestFixtureSetUp]
    public override void FixtureSetup()
    {
      InternalSetup(GetType());
      SetDefaultDriverType();
      int locPort = GetAvailableTCPPort();
      initClientPort();
      StartGFXDPeer(m_defaultDriverType, "server", s_serverDir, string.Empty,
                    s_clientPort, " -start-locator=localhost[" + locPort +
                    "] -auth-provider=BUILTIN -gemfirexd.user.gemfire1=" +
                    "gemfire1 -user=gemfire1 -password=gemfire1");
    }

    protected override DbConnection OpenNewConnection()
    {
      Dictionary<string, string> props = new Dictionary<string, string>();
      props.Add("user", "gemfire1");
      props.Add("password", "gemfire1");
      return base.OpenNewConnection(null, false, props);
    }

    protected DbConnection OpenNewNoAuthConnection1()
    {
      Dictionary<string, string> props = new Dictionary<string, string>();
      props.Add("user", "gemfire1");
      props.Add("password", "gemfire");
      return base.OpenNewConnection(null, false, props);
    }

    protected DbConnection OpenNewNoAuthConnection2()
    {
      Dictionary<string, string> props = new Dictionary<string, string>();
      props.Add("user", "gemfire2");
      props.Add("password", "gemfire");
      return base.OpenNewConnection(null, false, props);
    }

    protected DbConnection OpenNewNoAuthConnection3()
    {
      Dictionary<string, string> props = new Dictionary<string, string>();
      props.Add("user", "gemfire3");
      props.Add("password", "gemfire3");
      return base.OpenNewConnection(null, false, props);
    }

    protected override string[] CommonTablesToCreate()
    {
      return new string[] { "employee", "numeric_family", "datetime_family" };
    }

    protected override string[] GetProceduresToDrop()
    {
      return new string[] { "executeScalarInOutParams", "multipleResultSets",
        "longParams", "noParam", "Bug66630", "testSize", "decimalTest",
        "paramTest", "insertEmployee" };
    }

    #endregion

    [Test]
    /// <summary>
    /// check for various connection attributes in addition to auth
    /// (examples from reference guide)
    /// </summary>
    public void BasicAuthentication()
    {
      // First use the connection with system user to create a new user.
      string host = "localhost";
      int port = s_clientPort;
      string user = "gemfire1";
      string passwd = "gemfire1";
      string connStr = string.Format("server={0}:{1};user={2};password={3}",
                                     host, port, user, passwd);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();
        // fire a simple query to check the connection
        GFXDCommand cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.members";
        Assert.AreEqual(1, cmd.ExecuteScalar());
        // create new user
        cmd.CommandText = "call sys.create_user('gemfirexd.user.gem2', 'gem2')";
        Assert.AreEqual(-1, cmd.ExecuteNonQuery());
        conn.Close();
      }

      // Open a new connection to the locator having network server
      // with username and password in the connection string.
      user = "gem2";
      passwd = "gem2";
      connStr = string.Format("server={0}:{1};user={2};password={3}",
                              host, port, user, passwd);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();
        // fire a simple query to check the connection
        GFXDCommand cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.members";
        Assert.AreEqual(1, cmd.ExecuteScalar());
        conn.Close();
      }

      // Open a new connection to the locator having network server
      // with username and password passed as properties.
      connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        Dictionary<string, string> props = new Dictionary<string, string>();
        props.Add("user", user);
        props.Add("password", passwd);
        props.Add("disable-streaming", "true");
        props.Add("load-balance", "false");
        conn.Open(props);
        // fire a simple query to check the connection
        GFXDCommand cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.members";
        Assert.AreEqual(1, cmd.ExecuteScalar());
        conn.Close();
      }
    }

    [Test]
    public void ValidInvalidAuthentication()
    {
      // expect auth failure with incorrect credentials
      try {
        OpenNewNoAuthConnection1();
        Assert.Fail("Expected Authentication failure");
      } catch (GFXDException gfxde) {
        if (!"08004".Equals(gfxde.State) || gfxde.Severity != GFXDSeverity
            .Session || gfxde.ErrorCode != (int)GFXDSeverity.Session) {
          throw gfxde;
        }
      }
      try {
        OpenNewNoAuthConnection2();
        Assert.Fail("Expected Authentication failure");
      } catch (GFXDException gfxde) {
        if (!"08004".Equals(gfxde.State) || gfxde.Severity != GFXDSeverity
            .Session || gfxde.ErrorCode != (int)GFXDSeverity.Session) {
          throw gfxde;
        }
      }
      try {
        OpenNewNoAuthConnection3();
        Assert.Fail("Expected Authentication failure");
      } catch (GFXDException gfxde) {
        if (!"08004".Equals(gfxde.State) || gfxde.Severity != GFXDSeverity
            .Session || gfxde.ErrorCode != (int)GFXDSeverity.Session) {
          throw gfxde;
        }
      }

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select count(*) from SYS.SYSSCHEMAS" +
        " where SCHEMANAME like 'SYS%'";
      m_cmd.CommandTimeout = 10;

      // Check the Return value for a Correct Query
      object result = m_cmd.ExecuteScalar();
      Assert.AreEqual(8, result, "#A1 Query Result returned is incorrect");

      m_cmd.CommandText = "select SCHEMANAME, SCHEMAID from SYS.SYSSCHEMAS" +
        " where SCHEMANAME like 'SYS%' order by SCHEMANAME asc";
      result = m_cmd.ExecuteScalar();
      Assert.AreEqual("SYS", result, "#A2 ExecuteScalar expected 'SYS'");

      m_cmd.CommandText = "select SCHEMANAME from SYS.SYSSCHEMAS" +
        " where SCHEMANAME like 'NOTHING%'";
      result = m_cmd.ExecuteScalar();
      Assert.IsNull(result, "#A3 Null expected if result set is empty");

      // Check SqlException is thrown for Invalid Query
      m_cmd.CommandText = "select count* from SYS.SYSSCHEMAS";
      try {
        result = m_cmd.ExecuteScalar();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // Incorrect syntax near 'from'
        Assert.IsNotNull(ex.Message, "#B2");
        Assert.IsTrue(ex.Message.Contains("from"), "#B3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42X01"), "#B4: " + ex.Message);
      }

      // Parameterized stored procedure calls

      int int_value = 17;
      int int2_value = 7;
      string string_value = "output value changed";
      string return_value = "first column of first rowset";

      m_cmd.CommandText = "create procedure executeScalarInOutParams " +
        " (in p1 int, inout p2 int, out p3 varchar(200)) language java " +
        " parameter style java external name" +
        " 'tests.TestProcedures.inOutParams' dynamic result sets 1";
      m_cmd.CommandType = CommandType.Text;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "call executeScalarInOutParams(?, ?, ?)";
      m_cmd.CommandType = CommandType.StoredProcedure;
      DbParameter p1 = m_cmd.CreateParameter();
      p1.ParameterName = "p1";
      p1.Direction = ParameterDirection.Input;
      p1.DbType = DbType.Int32;
      p1.Value = int_value;
      m_cmd.Parameters.Add(p1);

      DbParameter p2 = m_cmd.CreateParameter();
      p2.ParameterName = "p2";
      p2.Direction = ParameterDirection.InputOutput;
      p2.DbType = DbType.Int32;
      p2.Value = int2_value;
      m_cmd.Parameters.Add(p2);

      DbParameter p3 = m_cmd.CreateParameter();
      p3.ParameterName = "p3";
      p3.Direction = ParameterDirection.Output;
      p3.DbType = DbType.String;
      p3.Size = 200;
      m_cmd.Parameters.Add(p3);

      result = m_cmd.ExecuteScalar();
      Assert.AreEqual(return_value, result, "#C1 ExecuteScalar should return" +
        " 'first column of first rowset'");
      Assert.AreEqual(int_value * int2_value, p2.Value, "#C2 ExecuteScalar " +
        " should fill the parameter collection with the output values");
      Assert.AreEqual(string_value, p3.Value, "#C3 ExecuteScalar should" +
        " fill the parameter collection with the output values");

      // check invalid size parameter
      try {
        p3.Size = -1;
        Assert.Fail("#D1 Parameter should throw exception due to size < 0");
      } catch (ArgumentOutOfRangeException ex) {
        // String[2]: the Size property has an invalid
        // size of 0
        Assert.IsNull(ex.InnerException, "#D2");
        Assert.IsNotNull(ex.Message, "#D3");
        Assert.AreEqual(-1, ex.ActualValue, "#D4");
        Assert.IsTrue(ex.ParamName.Contains("Size"), "#D5");
      }

      // try truncation of output value
      int newSize = 6;
      p2.Value = int2_value;
      p3.Size = newSize;
      p3.Value = null;
      result = m_cmd.ExecuteScalar();
      Assert.AreEqual(return_value, result, "#E1 ExecuteScalar should return" +
        " 'first column of first rowset'");
      Assert.AreEqual(int_value * int2_value, p2.Value, "#E2 ExecuteScalar " +
        " should fill the parameter collection with the output values");
      Assert.AreEqual(string_value.Substring(0, newSize), p3.Value,
        "#E3 ExecuteScalar should fill the parameter collection with the" +
        " output values");
    }
  }
}
