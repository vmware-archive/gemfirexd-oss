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
  /// Ported from Mono's SqlParameterTest for System.Data.SqlClient.
  /// </summary>
  [TestFixture]
  public class ParameterTest : TestBase
  {
    #region Setup/TearDown methods

    protected override string[] CommonTablesToCreate()
    {
      return new string[] { "datetime_family" };
    }

    protected override string[] GetProceduresToDrop()
    {
      return new string[] { "sp_bug382635", "sp_bug382539" };
    }

    protected override string[] GetTablesToDrop()
    {
      return new string[] { "text1", "Bug382635", "datetime_family",
        "Bug526794" };
    }

    #endregion

    #region Tests

    [Test]
    // bug #324840
    public void ParameterSizeTest()
    {
      string longstring = new string('x', 20480);
      DbParameter prm;

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create table text1 (ID int not null, Val1 clob)";
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "INSERT INTO text1 (ID, Val1) VALUES (:ID, ?)";
      prm = m_cmd.CreateParameter();
      prm.Value = longstring;
      prm.DbType = DbType.String;
      m_cmd.Parameters.Add(prm);
      prm = m_cmd.CreateParameter();
      prm.ParameterName = "ID";
      prm.Value = 1;
      m_cmd.Parameters.Add(prm);
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "select length(Val1) from text1";
      m_cmd.Parameters.Clear();
      Assert.AreEqual(20480, m_cmd.ExecuteScalar(), "#1");

      m_cmd.CommandText = "INSERT INTO text1 (ID, Val1) VALUES (?, :Val1)";
      prm.ParameterName = null;
      prm.Value = 1;
      m_cmd.Parameters.Add(prm);
      prm = m_cmd.CreateParameter();
      prm.ParameterName = "Val1";
      prm.Value = longstring;
      // no type set
      m_cmd.Parameters.Add(prm);
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "select length(Val1) from text1";
      m_cmd.Parameters.Clear();
      Assert.AreEqual(20480, m_cmd.ExecuteScalar(), "#2");

      m_cmd.CommandText = "INSERT INTO text1 (ID, Val1) VALUES (:ID, ?)";
      prm = m_cmd.CreateParameter();
      prm.ParameterName = "Val1";
      prm.Value = longstring;
      prm.DbType = DbType.AnsiString;
      m_cmd.Parameters.Add(prm);
      prm = m_cmd.CreateParameter();
      prm.ParameterName = "ID";
      prm.Value = 1;
      m_cmd.Parameters.Add(prm);
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "select length(Val1) from text1";
      m_cmd.Parameters.Clear();
      Assert.AreEqual(20480, m_cmd.ExecuteScalar(), "#3");
    }

    [Test]
    // bug #382635
    public void ParameterSize_compatibility_Test()
    {
      string longstring = "abcdefghijklmnopqrstuvwxyz";

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create table bug382635 (description varchar(20))";
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "CREATE PROCEDURE sp_bug382635 (descr varchar(20))" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.bug382635'";
      m_cmd.CommandType = CommandType.Text;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "INSERT INTO bug382635 (description)" +
        " VALUES ('Verifies bug #382635')";
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "call sp_bug382635(:Desc)";
      m_cmd.CommandType = CommandType.StoredProcedure;

      DbParameter p1 = m_cmd.CreateParameter();
      // first test for exception with a non-matching parameter name
      p1.ParameterName = "@Desc";
      p1.Value = longstring;
      p1.Size = 15;
      m_cmd.Parameters.Add(p1);
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#1");
      } catch (DbException ex) {
        Assert.IsTrue(ex.Message.Contains("42886"), "#2: " + ex.Message);
      }
      // next check successful run with correct parameter name
      p1.ParameterName = "Desc";
      m_cmd.Parameters.Clear();
      m_cmd.Parameters.Add(p1);
      m_cmd.ExecuteNonQuery();

      // cancel should not throw any unneeded exception (e.g. unimplemented)
      m_cmd.Cancel();

      // Test for truncation
      DbCommand selectCmd = m_conn.CreateCommand();
      selectCmd.CommandText =
        "SELECT LENGTH(description), description from bug382635";

      using (DbDataReader rdr = selectCmd.ExecuteReader()) {
        Assert.IsTrue(rdr.Read(), "#A1");
        Assert.AreEqual(15, rdr.GetValue(0), "#A2");
        Assert.AreEqual(longstring.Substring(0, 15), rdr.GetValue(1), "#A3");
        Assert.AreEqual(longstring, p1.Value, "#A4");
        rdr.Close();
      }

      // Test to ensure truncation is not done in the Value getter/setter
      p1.Size = 12;
      p1.Value = longstring.Substring(0, 22);
      p1.Size = 14;
      m_cmd.ExecuteNonQuery();

      using (DbDataReader rdr = selectCmd.ExecuteReader()) {
        Assert.IsTrue(rdr.Read(), "#B1");
        Assert.AreEqual(14, rdr.GetValue(0), "#B2");
        Assert.AreEqual(longstring.Substring(0, 14), rdr.GetValue(1), "#B3");
        Assert.AreEqual(longstring.Substring(0, 22), p1.Value, "#B4");
        rdr.Close();
      }

      // Size exceeds size of value
      p1.Size = 40;
      m_cmd.ExecuteNonQuery();

      using (DbDataReader rdr = selectCmd.ExecuteReader()) {
        Assert.IsTrue(rdr.Read(), "#C1");
        Assert.AreEqual(20, rdr.GetValue(0), "#C2");
        Assert.AreEqual(longstring.Substring(0, 20), rdr.GetValue(1), "#C3");
        rdr.Close();
      }
    }

    [Test]
    public void ConversionToSqlTypeInvalid()
    {
      string insert_data = "insert into datetime_family (id, type_datetime)" +
       " values (6000, ?)";
      string delete_data = "delete from datetime_family where id = 6000";

      m_cmd = m_conn.CreateCommand();
      object[] values = new object[] { 5, true, 40L, "invalid date" };
      try {
        for (int index = 0; index < values.Length; index++) {
          object val = values[index];

          m_cmd.CommandText = insert_data;
          DbParameter param = m_cmd.CreateParameter();
          param.DbType = DbType.DateTime2;
          param.Value = val;
          m_cmd.Parameters.Add(param);
          m_cmd.Prepare();

          try {
            m_cmd.ExecuteNonQuery();
            Assert.Fail("#1: " + index);
          } catch (InvalidCastException) {
            // expected
          } catch (FormatException) {
            // expected
          }
        }
      } finally {
        m_cmd.CommandText = delete_data;
        m_cmd.Parameters.Clear();
        m_cmd.ExecuteNonQuery();
      }
    }

    [Test]
    // bug #382589
    public void DecimalMinMaxAsParamValueTest()
    {
      string create_sp = "CREATE PROCEDURE sp_bug382539" +
       " (inout decval decimal(29, 0)) language java parameter style java" +
       " external name 'tests.TestProcedures.bug382539'";

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = create_sp;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "sp_bug382539";
      m_cmd.CommandType = CommandType.StoredProcedure;

      // check for Decimal.MaxValue
      DbParameter pValue = m_cmd.CreateParameter();
      pValue.Value = Decimal.MaxValue;
      pValue.Direction = ParameterDirection.InputOutput;
      m_cmd.Parameters.Add(pValue);

      Assert.AreEqual(Decimal.MaxValue, pValue.Value,
                      "Parameter initialization value mismatch");
      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(102m, pValue.Value, "Parameter value mismatch");

      // now check for Decimal.MinValue
      m_cmd.Parameters.Clear();
      pValue.Value = Decimal.MinValue;
      pValue.Direction = ParameterDirection.InputOutput;
      m_cmd.Parameters.Add(pValue);

      Assert.AreEqual(Decimal.MinValue, pValue.Value,
                      "Parameter initialization value mismatch");
      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(102m, pValue.Value, "Parameter value mismatch");
    }

    [Test]
    // bug #382589
    public void DecimalMinMaxAsParamValueExceptionTest()
    {
      string create_sp = "CREATE PROCEDURE sp_bug382539" +
       " (inout decval decimal(29,10)) language java parameter style java" +
       " external name 'tests.TestProcedures.bug382539'";

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = create_sp;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "sp_bug382539";
      m_cmd.CommandType = CommandType.StoredProcedure;

      // check exception for Decimal.MaxValue
      DbParameter pValue = m_cmd.CreateParameter();
      pValue.Value = Decimal.MaxValue;
      pValue.Direction = ParameterDirection.InputOutput;
      m_cmd.Parameters.Add(pValue);

      Assert.AreEqual(Decimal.MaxValue, pValue.Value,
                      "Parameter initialization value mismatch");
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#A1");
      } catch (DbException ex) {
        // Error converting data type numeric to decimal
        Assert.IsNotNull(ex.Message, "#A2");
        Assert.IsTrue(ex.Message.Contains("22003"), "#A3: " + ex.Message);
      }

      // now check for Decimal.MinValue
      m_cmd.Parameters.Clear();
      pValue.Value = Decimal.MinValue;
      pValue.Direction = ParameterDirection.InputOutput;
      m_cmd.Parameters.Add(pValue);
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // Error converting data type numeric to decimal
        Assert.IsNotNull(ex.Message, "#B2");
        Assert.IsTrue(ex.Message.Contains("22003"), "#B3: " + ex.Message);
      }
    }

    [Test]
    // bug #526794
    public void ZeroLengthString()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create table bug526794 (name varchar(20) NULL)";
      m_cmd.ExecuteNonQuery();

      DbParameter param = m_cmd.CreateParameter();
      param.DbType = DbType.AnsiString;
      param.Value = string.Empty;

      m_cmd.CommandText = "insert into bug526794 values (?)";
      m_cmd.Parameters.Add(param);
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "select * from bug526794";
      m_cmd.Parameters.Clear();
      using (DbDataReader rdr = m_cmd.ExecuteReader()) {
        Assert.IsTrue(rdr.Read(), "#A1");
        Assert.AreEqual(string.Empty, rdr.GetValue(0), "#A2");
        rdr.Close();
      }

      m_cmd.CommandText = "insert into bug526794 values (?)";
      param.DbType = DbType.Int32;
      param.Value = string.Empty;
      m_cmd.Parameters.Add(param);
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#B1");
      } catch (FormatException ex) {
        // Failed to convert parameter value from a String to a Int32
        Assert.IsNotNull(ex.Message, "#B2");
      }
    }

    [Test]
    public void CopyToParameterCollection()
    {
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = "SELECT fname FROM employee WHERE fname=:fname" +
        " AND lname=?";
      DbParameter p1Fname;
      DbParameter p1Lname;
      if (cmd is GFXDCommand) {
        GFXDCommand scmd = (GFXDCommand)cmd;
        p1Fname = scmd.Parameters.Add("fname", GFXDType.VarChar, 15);
      }
      else {
        p1Fname = cmd.CreateParameter();
        p1Fname.ParameterName = "fname";
        p1Fname.DbType = DbType.AnsiString;
        p1Fname.Value = 15;
        cmd.Parameters.Add(p1Fname);
      }
      p1Lname = cmd.CreateParameter();
      p1Lname.DbType = DbType.String;
      p1Lname.Value = 15;
      cmd.Parameters.Add(p1Lname);

      Assert.AreEqual(2, cmd.Parameters.Count, "#1 Initialization error," +
        " parameter collection must contain 2 elements");
      Assert.AreSame(p1Fname, cmd.Parameters["fname"], "#2 Should find the" +
        " same parameter as that added");
      Assert.AreSame(p1Fname, cmd.Parameters[0], "#3 Should find the" +
        " same parameter as that added");
      Assert.AreSame(p1Lname, cmd.Parameters[1], "#4 Should find the" +
        " same parameter as that added");
      Assert.IsNull(cmd.Parameters["lname"], "#5 Expected to find a null" +
        " parameter for non-existing name");

      DbParameter[] destinationArray = new DbParameter[4];
      cmd.Parameters.CopyTo(destinationArray, 1);
      Assert.AreEqual(4, destinationArray.Length, "#6 The length of" +
        " destination array should not change");
      Assert.AreEqual(null, destinationArray[0], "#7 The parameter collection" +
        " is copied at index 1, so the first element should not change");
      Assert.AreEqual(p1Fname, destinationArray[1], "#8 The parameter" +
        " at index 1 must be p1Fname");
      Assert.AreEqual(p1Lname, destinationArray[2], "#9 The parameter" +
        " at index 2 must be p1Lname");
      Assert.AreEqual(null, destinationArray[3], "#10 The parameter" +
        " at index 3 must not change");
    }

    #endregion
  }
}
