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
  /// Ported from Mono's SqlCommandTest for System.Data.SqlClient.
  /// </summary>
  [TestFixture]
  public class CommandTest : TestBase
  {
    #region Setup/TearDown methods

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

    #region Tests

    [Test]
    // ctor (String, SqlConnection, SqlTransaction)
    public void Constructor4()
    {
      string cmdText = "select * from SYS.SYSTABLES";

      DbTransaction trans = null;
      // transaction from same connection
      try {
        trans = m_conn.BeginTransaction();
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = cmdText;
        m_cmd.Transaction = trans;

        Assert.AreEqual(cmdText, m_cmd.CommandText, "#A1");
        Assert.AreEqual(-1, m_cmd.CommandTimeout, "#A2");
        Assert.AreEqual(CommandType.Text, m_cmd.CommandType, "#A3");
        Assert.AreSame(m_conn, m_cmd.Connection, "#A4");
        Assert.IsNull(m_cmd.Container, "#A5");
        Assert.IsTrue(m_cmd.DesignTimeVisible, "#A6");
        Assert.IsNotNull(m_cmd.Parameters, "#A7");
        Assert.AreEqual(0, m_cmd.Parameters.Count, "#A8");
        Assert.IsNull(m_cmd.Site, "#A9");
        Assert.AreSame(trans, m_cmd.Transaction, "#A10");
        Assert.AreEqual(UpdateRowSource.Both, m_cmd.UpdatedRowSource, "#A11");
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (m_conn != null) {
          m_conn.Close();
        }
      }

      DbConnection connA = null;
      // transaction from other connection
      try {
        m_conn = OpenNewConnection();
        connA = OpenNewConnection();

        trans = connA.BeginTransaction();
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = cmdText;
        m_cmd.Transaction = trans;
        m_cmd.CommandTimeout = 30;

        Assert.AreEqual(cmdText, m_cmd.CommandText, "#B1");
        Assert.AreEqual(30, m_cmd.CommandTimeout, "#B2");
        Assert.AreEqual(CommandType.Text, m_cmd.CommandType, "#B3");
        Assert.AreSame(m_conn, m_cmd.Connection, "#B4");
        Assert.IsNull(m_cmd.Container, "#B5");
        Assert.IsTrue(m_cmd.DesignTimeVisible, "#B6");
        Assert.IsNotNull(m_cmd.Parameters, "#B7");
        Assert.AreEqual(0, m_cmd.Parameters.Count, "#B8");
        Assert.IsNull(m_cmd.Site, "#B9");
        Assert.AreSame(trans, m_cmd.Transaction, "#B10");
        Assert.AreEqual(UpdateRowSource.Both, m_cmd.UpdatedRowSource, "#B11");
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    // bug #341743
    public void Dispose_Connection_Disposed()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select count(*) from SYS.SYSTABLES";
      m_cmd.ExecuteScalar();

      m_conn.Dispose();

      Assert.AreSame(m_conn, m_cmd.Connection, "#1");
      m_cmd.Dispose();
      Assert.AreSame(m_conn, m_cmd.Connection, "#2");
    }

    [Test]
    public void ExecuteScalar()
    {
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

    [Test]
    public void ExecuteScalar_CommandText_Empty()
    {
      m_cmd = m_conn.CreateCommand();
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#A1");
      } catch (DbException ex) {
        // ExecuteScalar: CommandText property
        // has not been initialized
        Assert.IsNotNull(ex.Message, "#A2");
        Assert.IsTrue(ex.Message.Contains("XJ067"), "#A3: " + ex.Message);
      }

      m_cmd.CommandText = string.Empty;
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // ExecuteScalar: CommandText property
        // has not been initialized
        Assert.IsNotNull(ex.Message, "#B2");
        Assert.IsTrue(ex.Message.Contains("42X01"), "#B3: " + ex.Message);
      }

      m_cmd.CommandText = null;
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#C1");
      } catch (DbException ex) {
        // ExecuteScalar: CommandText property
        // has not been initialized
        Assert.IsNotNull(ex.Message, "#C2");
        Assert.IsTrue(ex.Message.Contains("XJ067"), "#C3: " + ex.Message);
      }
    }

    [Test]
    public void ExecuteScalar_Query_Invalid()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "InvalidQuery";
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#1");
      } catch (DbException ex) {
        // syntax error for 'InvalidQuery'
        Assert.IsNotNull(ex.Message, "#2");
        Assert.IsTrue(ex.Message.Contains("InvalidQuery"),
                      "#3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42X01"), "#4: " + ex.Message);
      }
    }

    [Test]
    public void ExecuteScalar_Transaction_NotAssociated()
    {
      DbTransaction trans = null;
      DbConnection connA = null;
      try {
        connA = OpenNewConnection();
        trans = m_conn.BeginTransaction();

        m_cmd = connA.CreateCommand();
        m_cmd.CommandText = "select count(*) from SYS.SYSTABLES";
        m_cmd.Transaction = trans;
        try {
          m_cmd.ExecuteScalar();
          Assert.Fail("#A1");
        } catch (DbException ex) {
          // The transaction object is not associated
          // with the connection object
          Assert.IsNull(ex.InnerException, "#A2");
          Assert.IsNotNull(ex.Message, "#A3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#A4: " + ex.Message);
        }
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (m_cmd != null) {
          m_cmd.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void ExecuteScalar_Transaction_Only()
    {
      DbTransaction trans = m_conn.BeginTransaction();
      string queryStr = "select count(*) from SYS.SYSTABLES";
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = queryStr;
      m_cmd.Connection = null;
      m_cmd.Transaction = trans;
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#1");
      } catch (DbException ex) {
        // ExecuteScalar: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#4");
      } finally {
        trans.Dispose();
      }

      // now set the Connection property and try again
      m_cmd.Connection = m_conn;
      object result = m_cmd.ExecuteScalar();
      Assert.IsTrue(result.GetType() == typeof(int), "#5");
      Assert.Less(10, (int)result, "#6: " + result);
    }

    [Test]
    public void ExecuteNonQuery()
    {
      DbTransaction trans = m_conn.BeginTransaction();
      m_cmd = m_conn.CreateCommand();
      m_cmd.Transaction = trans;

      try {
        int result;
        m_cmd.CommandText = "Select id from numeric_family where id=1";
        result = m_cmd.ExecuteNonQuery();
        Assert.AreEqual(-1, result, "#A2");

        m_cmd.CommandText = "Insert into numeric_family (id, type_int)" +
          " values (100, 200)";
        result = m_cmd.ExecuteNonQuery();
        Assert.AreEqual(1, result, "#A3 One row shud be inserted");

        m_cmd.CommandText = "Insert into numeric_family (id, type_int)" +
          " values (200, 400)";
        result = m_cmd.ExecuteNonQuery();
        Assert.AreEqual(1, result, "#A4 One row shud be inserted");

        m_cmd.CommandText = "Update numeric_family set type_int=300" +
          " where id=100";
        result = m_cmd.ExecuteNonQuery();
        Assert.AreEqual(1, result, "#A5 One row shud be updated");

        // Test Batch Commands
        if (m_cmd is GFXDCommand) {
          GFXDCommand scmd = (GFXDCommand)m_cmd;
          scmd.AddBatch("update numeric_family set type_int=20" +
            " where id=200");
          scmd.AddBatch("update numeric_family set type_int=10" +
            " where id=100");
          result = scmd.ExecuteNonQuery();
          Assert.AreEqual(2, result, "#A6 Two rows shud be updated");
          // now try updating the same row twice
          scmd.AddBatch("update numeric_family set type_int=20" +
            " where id=100");
          scmd.AddBatch("update numeric_family set type_int=10" +
            " where id=100");
          result = scmd.ExecuteNonQuery();
          Assert.AreEqual(2, result, "#A7 Two rows shud be updated");
        }

        m_cmd.CommandText = "Delete from numeric_family where id in (100, 200)";
        result = m_cmd.ExecuteNonQuery();
        Assert.AreEqual(2, result, "#A8 Two rows shud be deleted");
      } finally {
        trans.Dispose();
      }

      // Parameterized stored procedure calls

      int int_value = 17;
      int int2_value = 7;
      string string_value = "output value changed";

      m_cmd.CommandText = "create procedure executeScalarInOutParams " +
        " (p1 int, inout p2 int, out p3 varchar(200)) language java " +
        " parameter style java external name" +
        " 'tests.TestProcedures.inOutParams' dynamic result sets 1";
      m_cmd.CommandType = CommandType.Text;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "executeScalarInOutParams";
      m_cmd.CommandType = CommandType.StoredProcedure;

      DbParameter p1 = m_cmd.CreateParameter();
      p1.ParameterName = "p1";
      p1.Direction = ParameterDirection.Input;
      p1.DbType = DbType.Int32;
      p1.Value = int_value;
      m_cmd.Parameters.Add(p1);

      DbParameter p2 = m_cmd.CreateParameter();
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

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(int_value * int2_value, p2.Value, "#B1");
      Assert.AreEqual(string_value, p3.Value, "#B2");

      // try truncation of output value
      int newSize = 6;
      p2.Value = int2_value;
      p3.Size = newSize;
      p3.Value = "none";
      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(int_value * int2_value, p2.Value, "#C1");
      Assert.AreEqual(string_value.Substring(0, newSize), p3.Value, "#C2");
    }

    [Test]
    public void ExecuteNonQuery_Query_Invalid()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select id1 from numeric_family";
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#A2");
      } catch (DbException ex) {
        // Invalid column name 'id1'
        Assert.IsNotNull(ex.Message, "#A3");
        Assert.IsTrue(ex.Message.Contains("'ID1'"), "#A4: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42X04"), "#A5: " + ex.Message);
      }

      // ensure connection is not closed after error

      int result;
      m_cmd.CommandText = "INSERT INTO numeric_family (id, type_int)" +
        " VALUES (6100, 200)";
      result = m_cmd.ExecuteNonQuery();
      Assert.AreEqual(1, result, "#B1");

      m_cmd.CommandText = "DELETE FROM numeric_family WHERE id = 6100";
      result = m_cmd.ExecuteNonQuery();
      Assert.AreEqual(1, result, "#C1");
    }

    [Test]
    public void ExecuteNonQuery_Transaction_NotAssociated()
    {
      DbTransaction trans = null;
      DbConnection connA = null;
      try {
        connA = OpenNewConnection();
        trans = m_conn.BeginTransaction();

        m_cmd = connA.CreateCommand();
        m_cmd.CommandText = "select count(*) from SYS.SYSTABLES";
        m_cmd.Transaction = trans;
        try {
          m_cmd.ExecuteNonQuery();
          Assert.Fail("#A1");
        } catch (DbException ex) {
          // The transaction object is not associated
          // with the connection object
          Assert.IsNull(ex.InnerException, "#A2");
          Assert.IsNotNull(ex.Message, "#A3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#A4: " + ex.Message);
        }
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (m_cmd != null) {
          m_cmd.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void ExecuteNonQuery_Transaction_Only()
    {
      DbTransaction trans = m_conn.BeginTransaction();
      string queryStr = "select count(*) from SYS.SYSTABLES";
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = queryStr;
      m_cmd.Connection = null;
      m_cmd.Transaction = trans;
      try {
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#1");
      } catch (DbException ex) {
        // ExecuteScalar: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#4");
      } finally {
        trans.Dispose();
      }

      // now set the Connection property and try again
      m_cmd.Connection = m_conn;
      int result = m_cmd.ExecuteNonQuery();
      Assert.AreEqual(-1, result, "#5: " + result);
    }

    [Test]
    // bug #412569
    public void ExecuteReader()
    {
      // Test for command behaviors
      DataTable schemaTable = null;
      DbDataReader reader = null;

      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select id from numeric_family where" +
        " id <= 4 order by id asc";

      // Test for default command behavior with single ResultSet
      reader = m_cmd.ExecuteReader();
      int rows = 0;
      int results = 0;
      do {
        while (reader.Read()) {
          ++rows;
          Assert.AreEqual(rows, reader.GetInt32(0), "#1");
        }
        Assert.AreEqual(4, rows, "#2 Multiple rows shud be returned");
        ++results;
        rows = 0;
      } while (reader.NextResult());
      Assert.AreEqual(1, results, "#3 Single result set shud be returned");
      reader.Close();

      // Test for default command behavior with multiple ResultSets
      m_cmd.CommandText = "create procedure multipleResultSets (maxId int)" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.multipleResultSets' dynamic result sets 2";
      Assert.AreEqual(0, m_cmd.ExecuteNonQuery(), "#4");

      m_cmd.CommandText = "call multipleResultSets(4)";
      m_cmd.CommandType = CommandType.StoredProcedure;
      reader = m_cmd.ExecuteReader();
      rows = 0;
      results = 0;
      do {
        while (reader.Read()) {
          ++rows;
          if (results > 0) {
            Assert.AreEqual(rows * 20000000, reader.GetInt32(0), "#5");
          }
          else {
            Assert.AreEqual(rows, reader.GetInt32(0), "#6");
          }
        }
        Assert.AreEqual(4, rows, "#7 Multiple rows shud be returned");
        ++results;
        rows = 0;
      } while (reader.NextResult());
      Assert.AreEqual(2, results, "#8 Multiple result sets shud be returned");
      reader.Close();

      // Test if closing reader, closes the connection
      reader = m_cmd.ExecuteReader(CommandBehavior.CloseConnection);
      reader.Close();
      Assert.AreEqual(ConnectionState.Closed, m_conn.State,
                      "#9 Command Behavior is not followed");
      m_conn.Open();

      // Test only column information is returned
      reader = m_cmd.ExecuteReader(CommandBehavior.SchemaOnly);
      schemaTable = reader.GetSchemaTable();
      Assert.AreEqual(DBNull.Value, schemaTable.Rows[0]["IsKey"],
                      "#11 Primary Key info shud not be returned");
      Assert.AreEqual("ID", schemaTable.Rows[0]["ColumnName"],
                      "#12 Schema Data is Incorrect");
      reader.Close();

      // Test if row info and primary Key info is returned
      reader = m_cmd.ExecuteReader(CommandBehavior.KeyInfo);
      schemaTable = reader.GetSchemaTable();
      Assert.IsTrue((bool)schemaTable.Rows[0]["IsKey"],
                    "#10 Primary Key info shud be returned");
      reader.Close();

      // Test only one result set (first) is returned
      reader = m_cmd.ExecuteReader(CommandBehavior.SingleResult);
      schemaTable = reader.GetSchemaTable();
      Assert.IsFalse(reader.NextResult(),
                     "#13 Only one result set shud be returned");
      Assert.AreEqual("ID", schemaTable.Rows[0]["ColumnName"], "#14" +
                      " The result set returned shud be the first result set");
      reader.Close();

      // Test only one row is returned with "fetch first"
      m_cmd.CommandText = "select id from numeric_family where" +
        " id <= 4 order by id asc fetch first 1 rows only";
      m_cmd.CommandType = CommandType.Text;
      reader = m_cmd.ExecuteReader();
      rows = 0;
      results = 0;
      do {
        while (reader.Read()) {
          ++rows;
          Assert.AreEqual(rows, reader.GetInt32(0), "#15");
        }
        Assert.AreEqual(1, rows, "#16 Only one row shud be returned");
        ++results;
        rows = 0;
      } while (reader.NextResult());
      Assert.AreEqual(1, results, "#17 Single result set shud be returned");
      reader.Close();
    }

    [Test]
    public void ExecuteReader_Query_Invalid()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "InvalidQuery";
      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#1");
      } catch (DbException ex) {
        // syntax error for 'InvalidQuery'
        Assert.IsNotNull(ex.Message, "#A2");
        Assert.IsTrue(ex.Message.Contains("InvalidQuery"),
                      "#A3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42X01"), "#A4: " + ex.Message);

        // connection is not closed
        Assert.AreEqual(ConnectionState.Open, m_conn.State, "#A5");
      }

      try {
        m_cmd.ExecuteReader(CommandBehavior.CloseConnection);
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // syntax error for 'InvalidQuery'
        Assert.IsNotNull(ex.Message, "#B2");
        Assert.IsTrue(ex.Message.Contains("InvalidQuery"),
                      "#B3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42X01"), "#B4: " + ex.Message);

        // connection is closed
        Assert.AreEqual(ConnectionState.Closed, m_conn.State, "#B5");
      }
    }

    [Test]
    public void ExecuteReader_Transaction_NotAssociated()
    {
      DbTransaction trans = null;
      DbConnection connA = null;
      try {
        connA = OpenNewConnection();
        trans = m_conn.BeginTransaction();

        m_cmd = connA.CreateCommand();
        m_cmd.CommandText = "select count(*) from SYS.SYSTABLES";
        m_cmd.Transaction = trans;
        try {
          m_cmd.ExecuteReader();
          Assert.Fail("#A1");
        } catch (DbException ex) {
          // The transaction object is not associated
          // with the connection object
          Assert.IsNull(ex.InnerException, "#A2");
          Assert.IsNotNull(ex.Message, "#A3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#A4: " + ex.Message);
        }
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (m_cmd != null) {
          m_cmd.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void ExecuteReader_Transaction_Only()
    {
      DbTransaction trans = m_conn.BeginTransaction();
      string queryStr = "select count(*) from SYS.SYSTABLES";
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = queryStr;
      m_cmd.Connection = null;
      m_cmd.Transaction = trans;
      try {
        m_cmd.ExecuteReader();
        Assert.Fail("#1");
      } catch (DbException ex) {
        // ExecuteScalar: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#2");
        Assert.IsNotNull(ex.Message, "#3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#4");
      } finally {
        trans.Dispose();
      }

      // now set the Connection property and try again
      m_cmd.Connection = m_conn;
      using (DbDataReader reader = m_cmd.ExecuteReader()) {
        Assert.IsTrue(reader.Read(), "#5");
        Assert.Less(10, reader.GetInt32(0), "#6");
        Assert.IsFalse(reader.Read(), "#7");
      }
    }

    [Test]
    public void PrepareTest_CheckValidStatement()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "Select id from numeric_family where id=?";

      // Test if Parameters are correctly populated 
      m_cmd.Parameters.Clear();
      DbParameter param = m_cmd.CreateParameter();
      param.ParameterName = "@ID";
      param.DbType = DbType.Byte;
      m_cmd.Parameters.Add(param);
      m_cmd.Parameters["@ID"].Value = 2;
      m_cmd.Prepare();
      Assert.AreEqual(2, m_cmd.ExecuteScalar(),
                      "#3 Prepared Statement not working");

      m_cmd.Parameters[0].Value = 3;
      Assert.AreEqual(3, m_cmd.ExecuteScalar(),
                      "#4 Prep Statement not working");
    }

    [Test]
    public void Prepare()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "Select id from numeric_family where id=?";

      // Test no exception is thrown if Parameter Type
      // is not explicitly set
      DbParameter param = m_cmd.CreateParameter();
      param.ParameterName = "@ID";
      param.Value = 2;
      m_cmd.Parameters.Add(param);
      m_cmd.Prepare();
      Assert.AreEqual(2, m_cmd.ExecuteScalar(), "#A1");

      // Test no exception is thrown for variable size data if
      // precision/scale/size is not set
      m_cmd.CommandText = "select type_varchar from numeric_family" +
        " where type_varchar=?";
      m_cmd.Parameters.Clear();
      param.ParameterName = "@p1";
      param.DbType = DbType.String;
      m_cmd.Parameters.Add(param);
      m_cmd.Parameters["@p1"].Value = "long";
      m_cmd.Prepare();
      using (DbDataReader dr = m_cmd.ExecuteReader()) {
        Assert.IsTrue(dr.Read(), "#A2");
        Assert.AreEqual("long", dr.GetString(0), "#A3");
        Assert.IsFalse(dr.Read(), "#A4");
      }
      Assert.AreEqual("long", m_cmd.ExecuteScalar(), "#A5");

      // Test exception thrown for unknown Stored Procs
      string procName = "ABFSDSFSF";
      m_cmd.CommandType = CommandType.StoredProcedure;
      m_cmd.CommandText = procName;
      try {
        m_cmd.Prepare();
        m_cmd.ExecuteNonQuery();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // 'ABFSDSFSF' is not recognized as a function or procedure
        Assert.IsNotNull(ex.Message, "#B2");
        Assert.IsTrue(ex.Message.Contains(procName), "#B3: " + ex.Message);
        Assert.IsTrue(ex.Message.Contains("42Y03"), "#B4: " + ex.Message);
      }

      m_cmd.CommandType = CommandType.Text;
    }

    [Test]
    public void Prepare_Transaction_NotAssociated()
    {
      DbTransaction trans = null;
      DbConnection connA = null;
      DbParameter param = null;
      try {
        connA = OpenNewConnection();
        trans = m_conn.BeginTransaction();

        // Text, without parameters
        m_cmd = connA.CreateCommand();
        m_cmd.CommandText = "values (10)";
        m_cmd.Transaction = trans;
        try {
          m_cmd.Prepare();
          Assert.Fail("#A1");
        } catch (DbException ex) {
          // The transaction is either not associated
          // with the current connection or has been
          // completed
          Assert.IsNull(ex.InnerException, "#A2");
          Assert.IsNotNull(ex.Message, "#A3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#A4: " + ex.Message);
        }

        // Text, with parameters
        m_cmd.CommandText = "values (?)";
        m_cmd.Transaction = trans;
        param = m_cmd.CreateParameter();
        param.ParameterName = "@TestPar1";
        param.DbType = DbType.Int32;
        m_cmd.Parameters.Add(param);
        try {
          m_cmd.Prepare();
          Assert.Fail("#B1");
        } catch (DbException ex) {
          // The transaction is either not associated
          // with the current connection or has been
          // completed
          Assert.IsNull(ex.InnerException, "#B2");
          Assert.IsNotNull(ex.Message, "#B3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#B4: " + ex.Message);
        }

        // Text, parameters cleared
        m_cmd.CommandText = "values (10)";
        m_cmd.Parameters.Clear();
        try {
          m_cmd.Prepare();
          Assert.Fail("#C1");
        } catch (DbException ex) {
          // The transaction is either not associated
          // with the current connection or has been
          // completed
          Assert.IsNull(ex.InnerException, "#C2");
          Assert.IsNotNull(ex.Message, "#C3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#C4: " + ex.Message);
        }

        // StoredProcedure, without parameters
        m_cmd.CommandText = "FindCustomer";
        m_cmd.CommandType = CommandType.StoredProcedure;
        try {
          m_cmd.Prepare();
          Assert.Fail("#D1");
        } catch (DbException ex) {
          // The transaction is either not associated
          // with the current connection or has been
          // completed
          Assert.IsNull(ex.InnerException, "#D2");
          Assert.IsNotNull(ex.Message, "#D3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#D4: " + ex.Message);
        }

        // StoredProcedure, with parameters
        m_cmd.CommandText = "FindCustomer";
        m_cmd.CommandType = CommandType.StoredProcedure;
        m_cmd.Parameters.Add(param);
        try {
          m_cmd.Prepare();
          Assert.Fail("#E1");
        } catch (DbException ex) {
          // The transaction is either not associated
          // with the current connection or has been
          // completed
          Assert.IsNull(ex.InnerException, "#E2");
          Assert.IsNotNull(ex.Message, "#E3");
          Assert.IsTrue(ex.Message.Contains("25001"), "#E4: " + ex.Message);
        }
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void Prepare_Transaction_Only()
    {
      DbTransaction trans = m_conn.BeginTransaction();
      DbParameter param = null;

      // Text, without parameters
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select count(*) from whatever";
      m_cmd.Connection = null;
      m_cmd.Transaction = trans;
      try {
        m_cmd.Prepare();
        Assert.Fail("#A1");
      } catch (DbException ex) {
        // Prepare: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#A2");
        Assert.IsNotNull(ex.Message, "#A3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#A4");
      }

      // Text, with parameters
      m_cmd.CommandText = "select count(*) from whatever";
      param = m_cmd.CreateParameter();
      param.ParameterName = "@TestPar1";
      param.DbType = DbType.Int32;
      m_cmd.Parameters.Add(param);
      m_cmd.Transaction = trans;
      try {
        m_cmd.Prepare();
        Assert.Fail("#B1");
      } catch (DbException ex) {
        // Prepare: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#B2");
        Assert.IsNotNull(ex.Message, "#B3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#B4");
      }

      // Text, parameters cleared
      m_cmd.CommandText = "select count(*) from whatever";
      m_cmd.Parameters.Clear();
      m_cmd.Transaction = trans;
      try {
        m_cmd.Prepare();
        Assert.Fail("#C1");
      } catch (DbException ex) {
        // Prepare: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#C2");
        Assert.IsNotNull(ex.Message, "#C3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#C4");
      }

      // StoredProcedure, without parameters
      m_cmd.CommandText = "FindCustomer";
      m_cmd.CommandType = CommandType.StoredProcedure;
      m_cmd.Transaction = trans;
      try {
        m_cmd.Prepare();
        Assert.Fail("#D1");
      } catch (DbException ex) {
        // Prepare: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#D2");
        Assert.IsNotNull(ex.Message, "#D3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#D4");
      }

      // StoredProcedure, with parameters
      m_cmd.CommandText = "FindCustomer";
      m_cmd.CommandType = CommandType.StoredProcedure;
      m_cmd.Parameters.Add(param);
      m_cmd.Transaction = trans;
      try {
        m_cmd.Prepare();
        Assert.Fail("#E1");
      } catch (DbException ex) {
        // Prepare: Connection property has not
        // been initialized
        Assert.IsNull(ex.InnerException, "#E2");
        Assert.IsNotNull(ex.Message, "#E3");
        Assert.IsTrue(ex.Message.Contains("08003"), "#E4");
      }
    }

    [Test]
    public void PreparedBatch()
    {
      m_cmd = m_conn.CreateCommand();
      if (m_cmd is GFXDCommand) {
        GFXDCommand scmd = (GFXDCommand)m_cmd;
        scmd.CommandText = "insert into employee(id, fname, dob, doj, email)" +
          " values (?, ?, ?, ?, ?)";
        scmd.Prepare();

        DateTime now = DateTime.Now;
        DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                    now.Hour, now.Minute, now.Second);
        DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                    now.Hour, now.Minute, now.Second);
        dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));
        int sumRowIds = 0;
        int batchSize = 7;
        int batchIter = 0;
        try {
          for (int rowId = 1000; rowId <= 2000; ++rowId) {
            sumRowIds += rowId;
            scmd.Parameters.Add(rowId);
            scmd.Parameters.Add("gfxd" + rowId);
            scmd.Parameters.Add(dob);
            scmd.Parameters.Add(doj);
            scmd.Parameters.Add("test" + rowId + "@vmware.com");
            scmd.AddBatch();
            if (++batchIter == batchSize) {
              scmd.ExecuteBatch();
              batchIter = 0;
            }
          }
          if (batchIter != 0) {
            scmd.ExecuteBatch();
          }

          // a rough check for successful inserts
          DbCommand cmd = m_conn.CreateCommand();
          cmd.CommandText = "select sum(id) from employee where id >= 1000";
          Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                          "#1 All inserts not done?");
        } finally {
          CleanEmployeeTable();
        }
      }
    }

    [Test]
    // bug #412576
    public void Connection()
    {
      DbConnection connA = null;
      DbTransaction trans = null;
      try {
        connA = OpenNewConnection();

        m_cmd = m_conn.CreateCommand();
        m_cmd.Connection = connA;
        Assert.AreSame(connA, m_cmd.Connection, "#A1");
        Assert.IsNull(m_cmd.Transaction, "#A2");
        m_cmd.Dispose();

        trans = m_conn.BeginTransaction();
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "values (10)";
        m_cmd.Transaction = trans;
        m_cmd.Connection = connA;
        Assert.AreSame(connA, m_cmd.Connection, "#B1");
        Assert.AreSame(trans, m_cmd.Transaction, "#B2");
        trans.Dispose();

        trans = m_conn.BeginTransaction();
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "values (10)";
        m_cmd.Transaction = trans;
        trans.Rollback();
        Assert.AreSame(m_conn, m_cmd.Connection, "#C1");
        Assert.IsNull(m_cmd.Transaction, "#C2");
        m_cmd.Connection = connA;
        Assert.AreSame(connA, m_cmd.Connection, "#C3");
        Assert.IsNull(m_cmd.Transaction, "#C4");

        trans = m_conn.BeginTransaction();
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "values (10)";
        m_cmd.Transaction = trans;
        m_cmd.Connection = null;
        Assert.IsNull(m_cmd.Connection, "#D1");
        Assert.AreSame(trans, m_cmd.Transaction, "#D2");
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void Connection_Reader_Open()
    {
      DbConnection connA = null;
      DbTransaction trans = null;
      try {
        connA = OpenNewConnection();

        trans = m_conn.BeginTransaction();

        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "values (10)";
        m_cmd.Transaction = trans;
        DbCommand cmdA = m_conn.CreateCommand();
        cmdA.CommandText = "values (20)";
        cmdA.Transaction = trans;
        using (DbDataReader reader = cmdA.ExecuteReader()) {
          m_cmd.Connection = m_conn;
          Assert.AreSame(m_conn, m_cmd.Connection, "#A1");
          Assert.AreSame(trans, m_cmd.Transaction, "#A2");

          m_cmd.Connection = connA;
          Assert.AreSame(connA, m_cmd.Connection, "#B1");
          Assert.AreSame(trans, m_cmd.Transaction, "#B2");

          m_cmd.Connection = null;
          Assert.IsNull(m_cmd.Connection, "#C1");
          Assert.AreSame(trans, m_cmd.Transaction, "#C2");
        }
      } finally {
        if (trans != null) {
          trans.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
      }
    }

    [Test]
    public void Transaction()
    {
      DbConnection connA = null;
      DbConnection connB = null;
      DbTransaction transA = null;
      DbTransaction transB = null;
      try {
        connA = OpenNewConnection();
        connB = OpenNewConnection();
        transA = connA.BeginTransaction();
        transB = connB.BeginTransaction();

        DbCommand cmd = connA.CreateCommand();
        cmd.CommandText = "values (10)";
        cmd.Transaction = transA;
        Assert.AreSame(connA, cmd.Connection, "#A1");
        Assert.AreSame(transA, cmd.Transaction, "#A2");
        cmd.Transaction = transB;
        Assert.AreSame(connA, cmd.Connection, "#B1");
        Assert.AreSame(transB, cmd.Transaction, "#B2");
        cmd.Transaction = null;
        Assert.AreSame(connA, cmd.Connection, "#C1");
        Assert.IsNull(cmd.Transaction, "#C2");
      } finally {
        if (transA != null) {
          transA.Dispose();
        }
        if (transB != null) {
          transA.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
        if (connB != null) {
          connB.Close();
        }
      }
    }

    [Test]
    // bug #412579
    public void Transaction_Reader_Open()
    {
      DbConnection connA = null;
      DbConnection connB = null;
      DbTransaction transA = null;
      DbTransaction transB = null;
      try {
        connA = OpenNewConnection();
        connB = OpenNewConnection();
        transA = connA.BeginTransaction();
        transB = connB.BeginTransaction();

        DbCommand cmdA = connA.CreateCommand();
        cmdA.CommandText = "select * from numeric_family";
        cmdA.Transaction = transA;

        DbCommand cmdB = connB.CreateCommand();
        cmdB.CommandText = "select * from numeric_family";
        cmdB.Transaction = transB;
        using (DbDataReader reader = cmdB.ExecuteReader()) {
          cmdA.Transaction = transA;
          Assert.AreSame(transA, cmdA.Transaction, "#A1");

          cmdA.Transaction = transB;
          Assert.AreSame(transB, cmdA.Transaction, "#B1");

          cmdA.Transaction = null;
          Assert.IsNull(cmdA.Transaction, "#C1");
        }
        cmdA.Transaction = transA;
        Assert.AreSame(transA, cmdA.Transaction, "#D1");
        cmdA.Transaction = transB;
        Assert.AreSame(transB, cmdA.Transaction, "#D2");
      } finally {
        if (transA != null) {
          transA.Dispose();
        }
        if (transB != null) {
          transB.Dispose();
        }
        if (connA != null) {
          connA.Close();
        }
        if (connB != null) {
          connB.Close();
        }
      }
    }

    [Test]
    public void ExecuteNonQuery_StoredProcedure()
    {
      DbParameter param;
      DbCommand m_cmd = null;
      DbDataReader dr = null;
      DbParameter idParam;
      DbParameter dojParam;

      try {
        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "create procedure insertEmployee" +
         " (fname varchar(20), dob timestamp, out doj timestamp, out id int)" +
         " language java parameter style java external name" +
        " 'tests.TestProcedures.insertEmployee'";
        m_cmd.ExecuteNonQuery();

        m_cmd.CommandText = "insertEmployee";
        m_cmd.CommandType = CommandType.StoredProcedure;

        GFXDCommand scmd = null;
        if (m_cmd is GFXDCommand) {
          scmd = (GFXDCommand)m_cmd;
          scmd.Parameters.Add("fname", GFXDType.VarChar, "testA");

          scmd.Parameters.Add("dob", GFXDType.TimeStamp,
                              new DateTime(2004, 8, 20));

          dojParam = new GFXDParameter("doj", GFXDType.TimeStamp);
          dojParam.Direction = ParameterDirection.Output;
          m_cmd.Parameters.Add(dojParam);

          idParam = new GFXDParameter("id", GFXDType.Integer);
          idParam.Direction = ParameterDirection.ReturnValue;
          m_cmd.Parameters.Add(idParam);
        }
        else {
          param = m_cmd.CreateParameter();
          param.ParameterName = "fname";
          param.DbType = DbType.String;
          param.Value = "testA";
          m_cmd.Parameters.Add(param);

          param = m_cmd.CreateParameter();
          param.ParameterName = "dob";
          param.DbType = DbType.DateTime;
          param.Value = new DateTime(2004, 8, 20);
          m_cmd.Parameters.Add(param);

          dojParam = m_cmd.CreateParameter();
          dojParam.ParameterName = "doj";
          dojParam.DbType = DbType.DateTime;
          dojParam.Direction = ParameterDirection.Output;
          m_cmd.Parameters.Add(dojParam);

          idParam = m_cmd.CreateParameter();
          idParam.ParameterName = "id";
          idParam.DbType = DbType.Int32;
          idParam.Direction = ParameterDirection.ReturnValue;
          m_cmd.Parameters.Add(idParam);
        }

        Assert.AreEqual(-1, m_cmd.ExecuteNonQuery(), "#A1");
        m_cmd.Dispose();

        m_cmd = m_conn.CreateCommand();
        m_cmd.CommandText = "select fname, dob, doj from employee where id=?";
        if (scmd != null) {
          param = new GFXDParameter();
          ((GFXDParameter)param).Type = GFXDType.Integer;
        }
        else {
          param = m_cmd.CreateParameter();
          param.DbType = DbType.Int32;
        }
        param.ParameterName = "@id";
        param.Value = idParam.Value;
        m_cmd.Parameters.Add(param);

        dr = m_cmd.ExecuteReader();
        Assert.IsTrue(dr.Read(), "#A2");
        Assert.AreEqual(typeof(string), dr.GetFieldType(0), "#A3");
        Assert.AreEqual("testA", dr.GetValue(0), "#A4");
        Assert.AreEqual(typeof(DateTime), dr.GetFieldType(1), "#A5");
        Assert.AreEqual(new DateTime(2004, 8, 20), dr.GetValue(1), "#A6");
        Assert.AreEqual(typeof(DateTime), dr.GetFieldType(2), "#A7");
        Assert.AreEqual(dojParam.Value, dr.GetValue(2), "#A8");
        Assert.IsFalse(dr.Read(), "#A9");
        m_cmd.Dispose();
        dr.Close();
      } finally {
        if (m_cmd != null) {
          m_cmd.Dispose();
        }
        if (dr != null) {
          dr.Close();
        }
      }
    }

    [Test]
    // bug #319598
    public void LongQueryTest()
    {
      DbCommand cmd = m_conn.CreateCommand();
      string val = new string('a', 10000);
      cmd.CommandText = string.Format("values ('{0}')", val);
      cmd.ExecuteNonQuery();
    }

    [Test]
    // bug #319598
    public void LongStoredProcTest()
    {
      DbParameter param = null;

      /*int size = conn.PacketSize;*/
      DbCommand cmd = m_conn.CreateCommand();
      // create a temp stored proc
      cmd.CommandText = "create procedure longParams (in p1 varchar(4000)," +
        " p2 varchar(4000), p3 varchar(4000), out p4 varchar(4000)," +
        " out p5 int) language java parameter style java external name" +
        " 'tests.TestProcedures.longParams'";
      cmd.ExecuteNonQuery();

      //execute the proc
      cmd.CommandType = CommandType.StoredProcedure;
      cmd.CommandText = "longParams";

      string val = new string('a', 4000);
      // also test passing raw values with GFXDCommand
      if (cmd is GFXDCommand) {
        cmd.Parameters.Add(val);
        cmd.Parameters.Add(val);
      }
      else {
        param = cmd.CreateParameter();
        param.ParameterName = "@p1";
        param.DbType = DbType.String;
        param.Size = 4000;
        param.Value = val;
        cmd.Parameters.Add(param);

        param = cmd.CreateParameter();
        param.ParameterName = "@p2";
        param.DbType = DbType.String;
        param.Size = 4000;
        param.Value = val;
        cmd.Parameters.Add(param);
      }
      param = cmd.CreateParameter();
      param.ParameterName = "@p3";
      param.DbType = DbType.String;
      param.Size = 4000;
      param.Value = val;
      cmd.Parameters.Add(param);

      DbParameter p4 = cmd.CreateParameter();
      p4.ParameterName = "@p4";
      p4.DbType = DbType.String;
      p4.Size = 4000;
      p4.Direction = ParameterDirection.Output;

      DbParameter p5 = cmd.CreateParameter();
      p5.DbType = DbType.Int32;
      p5.Direction = ParameterDirection.ReturnValue;

      cmd.Parameters.Add(p4);
      cmd.Parameters.Add(p5);

      cmd.ExecuteNonQuery();
      Assert.AreEqual("Hello", p4.Value, "#1");
      Assert.AreEqual(2, p5.Value, "#2");
    }

    [Test]
    public void DateTimeParameterTest()
    {
      DbCommand cmd = m_conn.CreateCommand();
      DbParameter param = cmd.CreateParameter();
      cmd.CommandText = "select * from datetime_family where type_datetime=?";
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2005-10-10 10:10:10 GMT").ToUniversalTime();
      cmd.Parameters.Add(param);
      // should'nt cause an exception
      DbDataReader rdr = cmd.ExecuteReader();
      Assert.IsTrue(rdr.Read(), "#1");
      Assert.AreEqual(2, rdr.GetInt32(0), "#2");
      Assert.IsFalse(rdr.Read(), "#3");
      rdr.Close();
      cmd.Parameters.Clear();

      // now check for DATE
      cmd.CommandText = "select * from datetime_family where type_date=?";
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2005-10-10 10:10:10");
      cmd.Parameters.Add(param);
      // should'nt cause an exception
      rdr = cmd.ExecuteReader();
      Assert.IsTrue(rdr.Read(), "#4");
      Assert.AreEqual(2, rdr.GetInt32(0), "#5");
      Assert.IsFalse(rdr.Read(), "#6");
      rdr.Close();
      cmd.Parameters.Clear();
      // check without creating parameter
      cmd.Parameters.Add("2004-10-10 10:10:10");
      // should'nt cause an exception
      rdr = cmd.ExecuteReader();
      Assert.IsTrue(rdr.Read(), "#7");
      Assert.AreEqual(1, rdr.GetInt32(0), "#8");
      Assert.IsFalse(rdr.Read(), "#9");
      rdr.Close();
      cmd.Parameters.Clear();

      // lastly for TIME
      cmd.CommandText = "select * from datetime_family where type_time=?";
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2005-10-10 10:10:10 GMT").ToUniversalTime();
      cmd.Parameters.Add(param);
      // should'nt cause an exception
      rdr = cmd.ExecuteReader();
      Assert.IsTrue(rdr.Read(), "#10");
      Assert.AreEqual(1, rdr.GetInt32(0), "#11");
      Assert.IsFalse(rdr.Read(), "#12");
      rdr.Close();
      cmd.Parameters.Clear();
    }

    /**
     * Verifies whether an enum value is converted to a numeric value when
     * used as value for a numeric parameter (bug #66630)
     */
    [Test]
    public void EnumParameterTest()
    {
      DbParameter param;
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = "create procedure Bug66630 (in Status smallint)" +
       " language java parameter style java external name" +
       " 'tests.TestProcedures.monoBug66630' dynamic result sets 1";
      cmd.ExecuteNonQuery();

      cmd.CommandText = "Bug66630";
      cmd.CommandType = CommandType.StoredProcedure;
      param = cmd.CreateParameter();
      param.ParameterName = "@Status";
      param.DbType = DbType.Int32;
      param.Value = Status.Error;
      cmd.Parameters.Add(param);

      using (DbDataReader dr = cmd.ExecuteReader()) {
        // one record should be returned
        Assert.IsTrue(dr.Read(), "EnumParameterTest#1");
        // we should get two field in the result
        Assert.AreEqual(2, dr.FieldCount, "EnumParameterTest#2");
        // field 1
        Assert.AreEqual("INTEGER", dr.GetDataTypeName(0),
                        "EnumParameterTest#3");
        Assert.AreEqual(5, dr.GetInt32(0), "EnumParameterTest#4");
        // field 2
        Assert.AreEqual("SMALLINT", dr.GetDataTypeName(1),
                        "EnumParameterTest#5");
        Assert.AreEqual((short)Status.Error, dr.GetInt16(1),
                        "EnumParameterTest#6");
        // only one record should be returned
        Assert.IsFalse(dr.Read(), "EnumParameterTest#7");
      }
    }

    [Test]
    public void CloneTest()
    {
      DbTransaction trans = m_conn.BeginTransaction();

      m_cmd = m_conn.CreateCommand();
      m_cmd.Transaction = trans;

      DbCommand clone = ((ICloneable)m_cmd).Clone() as DbCommand;
      Assert.AreSame(m_conn, clone.Connection);
      Assert.AreSame(trans, clone.Transaction);
    }

    [Test]
    public void StoredProc_NoParameterTest()
    {
      string query = "create procedure noParam () language java parameter" +
        " style java external name 'tests.TestProcedures.noParam'" +
        " dynamic result sets 1";
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = query;
      cmd.ExecuteNonQuery();

      cmd.CommandType = CommandType.StoredProcedure;
      cmd.CommandText = "noParam";
      using (DbDataReader reader = cmd.ExecuteReader()) {
        Assert.IsTrue(reader.Read(), "#1 Select shud return data");
        Assert.AreEqual("data", reader.GetString(0), "#2");
      }
    }

    [Test]
    public void StoredProc_ParameterTest()
    {
      string createQuery = "create procedure paramTest (type char(20)," +
        " param1 {0}, out param2 {0}, inout param3 {0}, out param4 int)" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.paramTest' dynamic result sets 1";

      DbCommand cmd = m_conn.CreateCommand();
      int label = 0;
      string error = string.Empty;
      string t;
      while (label != -1) {
        try {
          switch (label) {
            case 0:
              // Test BigInt Param
              t = "bigint";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Int64, t, 0, Int64.MaxValue,
                                Int64.MaxValue, Int64.MaxValue, Int64.MaxValue);
              RpcHelperFunction(cmd, DbType.Int64, t, 0, Int64.MinValue,
                                Int64.MinValue, Int64.MinValue, Int64.MinValue);
              break;
            case 1:
              // Testing Char
              t = "char(10)";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.StringFixedLength, t, 10,
                                "characters", "characters", "characters",
                                "characters");
              RpcHelperFunction(cmd, DbType.StringFixedLength, t, 3,
                                "characters", "cha       ", "cha", "cha");
              RpcHelperFunction(cmd, DbType.StringFixedLength, t, 10,
                                "cha", "cha       ",
                                "cha       ", "cha       ");
              RpcHelperFunction(cmd, DbType.StringFixedLength, t, 3,
                                string.Empty, "          ", "   ", "   ");
              RpcHelperFunction(cmd, DbType.StringFixedLength, t, 5,
                                DBNull.Value, DBNull.Value, DBNull.Value,
                                DBNull.Value);
              break;
            case 2:
              // Testing DateTime
              t = "timestamp";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.DateTime, t, 0,
                                "2079-06-06 23:59:00",
                                new DateTime(2079, 6, 6, 23, 59, 0),
                                new DateTime(2079, 6, 6, 23, 59, 0),
                                new DateTime(2079, 6, 6, 23, 59, 0));
              RpcHelperFunction(cmd, DbType.DateTime, t, 0,
                                "2009-04-12 10:39:45",
                                new DateTime(2009, 4, 12, 10, 39, 45),
                                new DateTime(2009, 4, 12, 10, 39, 45),
                                new DateTime(2009, 4, 12, 10, 39, 45));
              RpcHelperFunction(cmd, DbType.DateTime, t, 0, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);
              break;
            case 3:
              // Test Decimal Param
              t = "decimal(10, 2)";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, 10.665m, 10.66m,
                                10.66m, 10.66m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, 0m, 0m, 0m, 0m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, -5.657m, -5.65m,
                                -5.65m, -5.65m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);

              // conversion
              RpcHelperFunction(cmd, DbType.Decimal, t, 0,
                                AttributeTargets.Constructor, 32.0m, 32m, 32m);
              RpcHelperFunction(cmd, DbType.Decimal, t,  0, 4.325f, 4.32m,
                                4.32m, 4.32m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, 10.0, 10.00m,
                                10m, 10m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, 10.665, 10.66m,
                                10.66m, 10.66m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, -5.657, -5.65m,
                                -5.65m, -5.65m);
              RpcHelperFunction(cmd, DbType.Decimal, t, 0, 4, 4m, 4m, 4m);
              break;
            case 4:
              // Test Float Param
              t = "float";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10.0, 10.0f,
                                10.0f, 10.0f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10.54, 10.54f,
                                10.54f, 10.54f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 0, 0.0f, 0.0f, 0.0f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, -5.34, -5.34f,
                                -5.34f, -5.34f);
              break;
            case 5:
              // Test Integer Param
              t = "int";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Int32, t, 0, 10, 10, 10, 10);
              RpcHelperFunction(cmd, DbType.Int32, t, 0, 0, 0, 0, 0);
              RpcHelperFunction(cmd, DbType.Int32, t, 0, -5, -5, -5, -5);
              RpcHelperFunction(cmd, DbType.Int32, t, 0, int.MaxValue,
                                int.MaxValue, int.MaxValue, int.MaxValue);
              RpcHelperFunction(cmd, DbType.Int32, t, 0, int.MinValue,
                                int.MinValue, int.MinValue, int.MinValue);
              break;
            case 6:
              // Test Money Param
              // money is non-standard so using NUMERIC instead
              t = "numeric(19,4)";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 10m, 10m,
                                10m, 10m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 10.54, 10.54m,
                                10.54m, 10.54m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 0, 0m, 0m, 0m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -5.34, -5.34m,
                                -5.34m, -5.34m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 5.34, 5.34m,
                                5.34m, 5.34m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -10.1234m,
                                -10.1234m, -10.1234m, -10.1234m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 10.1234m,
                                10.1234m, 10.1234m, 10.1234m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -2000000000m,
                                -2000000000m, -2000000000m, -2000000000m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 2000000000m,
                                2000000000m, 2000000000m, 2000000000m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.2345m,
                                -200000000.2345m, -200000000.2345m,
                                -200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.2345m,
                                200000000.2345m, 200000000.2345m,
                                200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);

              // rounding tests
              // TODO: derby always uses ROUND_DOWN currently; change this once
              // #41925 is fixed
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234561m,
                                -200000000.2345m, -200000000.2345m,
                                -200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234551m,
                                -200000000.2345m, -200000000.2345m,
                                -200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234541m,
                                -200000000.2345m, -200000000.2345m,
                                -200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234561m,
                                200000000.2345m, 200000000.2345m,
                                200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234551m,
                                200000000.2345m, 200000000.2345m,
                                200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234541m,
                                200000000.2345m, 200000000.2345m,
                                200000000.2345m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234461m,
                                -200000000.2344m, -200000000.2344m,
                                -200000000.2344m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234451m,
                                -200000000.2344m, -200000000.2344m,
                                -200000000.2344m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, -200000000.234441m,
                                -200000000.2344m, -200000000.2344m,
                                -200000000.2344m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234461m,
                                200000000.2344m, 200000000.2344m,
                                200000000.2344m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234451m,
                                200000000.2344m, 200000000.2344m,
                                200000000.2344m);
              RpcHelperFunction(cmd, DbType.Currency, t, 0, 200000000.234441m,
                                200000000.2344m, 200000000.2344m,
                                200000000.2344m);
              break;
            case 7:
              // Test Real Param
              t = "real";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10m, 10f,
                                10f, 10f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10.0, 10f,
                                10f, 10f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 0, 0f, 0f, 0f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 3.54, 3.54f,
                                3.54f, 3.54f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10, 10f,
                                10f, 10f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 10.5f, 10.5f,
                                10.5f, 10.5f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 3.5, 3.5f,
                                3.5f, 3.5f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, 4.54m, 4.54f,
                                4.54f, 4.54f);
              RpcHelperFunction(cmd, DbType.Single, t, 0, -4.54m, -4.54f,
                                -4.54f, -4.54f);
              break;
            case 8:
              // Test Double Param
              t = "double";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Double, t, 0, 10m, 10d,
                                10d, 10d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 10.0, 10d,
                                10d, 10d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 0, 0d, 0d, 0d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 3.54, 3.54d,
                                3.54d, 3.54d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 10, 10d,
                                10d, 10d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 10.5d, 10.5d,
                                10.5d, 10.5d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 3.5, 3.5d,
                                3.5d, 3.5d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, 4.54m, 4.54d,
                                4.54d, 4.54d);
              RpcHelperFunction(cmd, DbType.Double, t, 0, -4.54m, -4.54d,
                                -4.54d, -4.54d);
              break;
            case 9:
              // Test SmallInt Param
              t = "smallint";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Int16, t, 0, 10, (short)10,
                                (short)10, (short)10);
              RpcHelperFunction(cmd, DbType.Int16, t, 0, -10, (short)-10,
                                (short)-10, (short)-10);
              RpcHelperFunction(cmd, DbType.Int16, t, 0, short.MaxValue,
                                short.MaxValue, short.MaxValue, short.MaxValue);
              RpcHelperFunction(cmd, DbType.Int16, t, 0, short.MinValue,
                                short.MinValue, short.MinValue, short.MinValue);
              break;
            case 10:
              // Test Varchar Param
              t = "varchar(10)";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.String, t, 7, "VarChar", "VarChar",
                                "VarChar", "VarChar");
              RpcHelperFunction(cmd, DbType.String, t, 5, "Var", "Var",
                                "Var", "Var");
              RpcHelperFunction(cmd, DbType.String, t, 3, "Varchar", "Var",
                                "Var", "Var");
              RpcHelperFunction(cmd, DbType.String, t, 10, string.Empty,
                                string.Empty, string.Empty, string.Empty);
              RpcHelperFunction(cmd, DbType.String, t, 10, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);
              break;
            case 11:
              // Testing Date
              t = "date";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Date, t, 0, "2079-06-06",
                                DateTime.Parse("2079-06-06"),
                                DateTime.Parse("2079-06-06"),
                                DateTime.Parse("2079-06-06"));
              RpcHelperFunction(cmd, DbType.Date, t, 0, "2009-04-12",
                                DateTime.Parse("2009-04-12"),
                                DateTime.Parse("2009-04-12"),
                                DateTime.Parse("2009-04-12"));
              RpcHelperFunction(cmd, DbType.Date, t, 0, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);
              break;
            case 12:
              // Testing Time
              t = "time";
              cmd.CommandText = string.Format(createQuery, t);
              cmd.ExecuteNonQuery();
              RpcHelperFunction(cmd, DbType.Time, t, 0, "23:59:00",
                                DateTime.Parse("1970-01-01 23:59:00"),
                                DateTime.Parse("1970-01-01 23:59:00"),
                                DateTime.Parse("1970-01-01 23:59:00"));
              RpcHelperFunction(cmd, DbType.Time, t, 0, "10:39:45",
                                DateTime.Parse("1970-01-01 10:39:45"),
                                DateTime.Parse("1970-01-01 10:39:45"),
                                DateTime.Parse("1970-01-01 10:39:45"));
              RpcHelperFunction(cmd, DbType.Time, t, 0, DBNull.Value,
                                DBNull.Value, DBNull.Value, DBNull.Value);
              break;
            default:
              label = -2;
              break;
          }
        } catch (AssertionException ex) {
          error += string.Format(" Case {0} INCORRECT VALUE : {1}\n",
                                 label, ex.ToString());
        } catch (DbException ex) {
          error += string.Format(" Case {0} NOT WORKING : {1}\n",
                                 label, ex.ToString());
        }

        ++label;
        if (label != -1) {
          try {
            cmd.CommandText = "drop procedure paramTest";
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.ExecuteNonQuery();
          } catch (DbException ex) {
            if (!ex.Message.Contains("42Y55")) {
              throw;
            }
          }
        }
      }

      if (error.Length != 0) {
        Assert.Fail(error);
      }
    }

    [Test]
    public void OutputParamSizeTest1()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create procedure testSize (inout p1 varchar(10))" +
       " language java parameter style java external name" +
       " 'tests.TestProcedures.testSize'";
      m_cmd.CommandType = CommandType.Text;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "testSize";
      m_cmd.CommandType = CommandType.StoredProcedure;

      string pVal = "01234567890123456789";
      DbParameter p1 = m_cmd.CreateParameter();
      p1.ParameterName = "@p1";
      p1.Direction = ParameterDirection.InputOutput;
      p1.DbType = DbType.String;
      p1.IsNullable = false;
      p1.Size = 15;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(pVal.Substring(0, 10), p1.Value, "#1: " + p1.Value);

      // also try with nullable as true
      m_cmd.Parameters.Clear();
      p1.IsNullable = true;
      p1.Size = 100;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(pVal.Substring(0, 10), p1.Value, "#2: " + p1.Value);

      m_cmd.Parameters.Clear();
      p1.Size = 5;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(pVal.Substring(0, 5), p1.Value, "#3: " + p1.Value);
    }

    [Test]
    public void OutputParamSizeTest2()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create procedure testSize (out p1 varchar(10))" +
       " language java parameter style java external name" +
       " 'tests.TestProcedures.testSize'";
      m_cmd.CommandType = CommandType.Text;
      m_cmd.ExecuteNonQuery();

      m_cmd.CommandText = "testSize";
      m_cmd.CommandType = CommandType.StoredProcedure;

      string pVal = "01234567890123456789";
      string expectVal = "1234567890";
      DbParameter p1 = m_cmd.CreateParameter();
      p1.ParameterName = "@p1";
      p1.Direction = ParameterDirection.Output;
      p1.DbType = DbType.String;
      p1.IsNullable = false;
      p1.Size = 15;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(expectVal, p1.Value, "#1: " + p1.Value);

      // also try with nullable as true
      m_cmd.Parameters.Clear();
      p1.IsNullable = true;
      p1.Size = 100;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(expectVal, p1.Value, "#2: " + p1.Value);

      m_cmd.Parameters.Clear();
      p1.Size = 5;
      p1.Value = pVal;
      m_cmd.Parameters.Add(p1);

      m_cmd.ExecuteNonQuery();
      Assert.AreEqual(expectVal.Substring(0, 5), p1.Value, "#3: " + p1.Value);
    }

    [Test]
    public void Decimal_Overflow_Max()
    {
      DbParameter param;
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create procedure decimalTest (in p1 decimal(5, 3)," +
       " out p2 decimal(5, 3), out p3 int) language java parameter style java" +
       " external name 'tests.TestProcedures.decimalTest'";
      m_cmd.ExecuteNonQuery();

      //decimal overflow = 214748.36471m;
      decimal overflow = 214748.3648m;

      m_cmd.CommandText = "decimalTest";
      m_cmd.CommandType = CommandType.StoredProcedure;
      if (m_cmd is GFXDCommand) {
        m_cmd.Parameters.Add(overflow);
      }
      else {
        param = m_cmd.CreateParameter();
        param.ParameterName = "p1";
        param.DbType = DbType.Decimal;
        param.Value = overflow;
        m_cmd.Parameters.Add(param);
      }
      param = m_cmd.CreateParameter();
      param.DbType = DbType.Decimal;
      param.Direction = ParameterDirection.Output;
      m_cmd.Parameters.Add(param);

      param = m_cmd.CreateParameter();
      param.DbType = DbType.Int32;
      param.Direction = ParameterDirection.Output;
      m_cmd.Parameters.Add(param);

      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#1");
      } catch (DbException ex) {
        Assert.IsNotNull(ex.Message, "#2");
        Assert.IsTrue(ex.Message.Contains("22003"), "#3: " + ex.Message);
      }
    }

    [Test]
    public void Decimal_Overflow_Min()
    {
      DbParameter param;
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "create procedure decimalTest (in p1 decimal(5, 3)," +
       " out p2 decimal(5, 3), out p3 int) language java parameter style java" +
       " external name 'tests.TestProcedures.decimalTest'";
      m_cmd.ExecuteNonQuery();

      //decimal overflow = -214748.36481m;
      decimal overflow = -214748.3649m;

      m_cmd.CommandText = "decimalTest";
      m_cmd.CommandType = CommandType.StoredProcedure;
      if (m_cmd is GFXDCommand) {
        m_cmd.Parameters.Add(overflow);
      }
      else {
        param = m_cmd.CreateParameter();
        param.ParameterName = "p1";
        param.DbType = DbType.Decimal;
        param.Value = overflow;
        m_cmd.Parameters.Add(param);
      }
      param = m_cmd.CreateParameter();
      param.DbType = DbType.Decimal;
      param.Direction = ParameterDirection.Output;
      m_cmd.Parameters.Add(param);

      param = m_cmd.CreateParameter();
      param.DbType = DbType.Int32;
      param.Direction = ParameterDirection.Output;
      m_cmd.Parameters.Add(param);

      try {
        m_cmd.ExecuteScalar();
        Assert.Fail("#1");
      } catch (DbException ex) {
        Assert.IsNotNull(ex.Message, "#2");
        Assert.IsTrue(ex.Message.Contains("22003"), "#3: " + ex.Message);
      }
    }

    [Test]
    public void CommandDisposeTest()
    {
      DbDataReader reader = null;
      try {
        DbCommand command = m_conn.CreateCommand();
        try {
          command.CommandText = "select * from numeric_family";
          reader = command.ExecuteReader();
        } finally {
          command.Dispose();
        }
        while (reader.Read());
      } finally {
        if (reader != null) {
          reader.Dispose();
        }
      }
    }

    #endregion

    #region Internal/Private utility methods

    private enum Status
    {
      OK = 0,
      Error = 3
    }

    private void RpcHelperFunction(DbCommand cmd, DbType type, string typeName,
                                   int size, object input, object expectedRead,
                                   object expectedOut, object expectedInOut)
    {
      cmd.Parameters.Clear();
      DbParameter typeParam, param1, param2, param3;

      typeParam = cmd.CreateParameter();
      typeParam.DbType = DbType.StringFixedLength;
      typeParam.Value = typeName;
      param1 = cmd.CreateParameter();
      param1.DbType = type;
      param2 = cmd.CreateParameter();
      param2.DbType = type;
      param3 = cmd.CreateParameter();
      param3.DbType = type;
      if (size != 0) {
        param1.Size = size;
        param2.Size = size;
        param3.Size = size;
      }

      DbParameter retval = cmd.CreateParameter();
      retval.DbType = DbType.Int32;

      param1.Value = input;
      param1.Direction = ParameterDirection.Input;
      param2.Direction = ParameterDirection.Output;
      param3.Direction = ParameterDirection.InputOutput;
      param3.Value = input;
      retval.Direction = ParameterDirection.ReturnValue;

      cmd.CommandText = "paramTest";
      cmd.CommandType = CommandType.StoredProcedure;

      cmd.Parameters.Add(typeParam);
      cmd.Parameters.Add(param1);
      cmd.Parameters.Add(param2);
      cmd.Parameters.Add(param3);
      cmd.Parameters.Add(retval);

      using (DbDataReader reader = cmd.ExecuteReader()) {
        Assert.IsTrue(reader.Read(), "#1");
        Assert.AreEqual(expectedRead, reader.GetValue(0), "#2");
        Assert.IsFalse(reader.Read(), "#3");
      }
      Assert.AreEqual(expectedOut, param2.Value, "#4");
      Assert.AreEqual(expectedInOut, param3.Value, "#5");
      Assert.AreEqual(5, retval.Value, "#6");

      // reset CommandType since this is reused
      cmd.CommandType = CommandType.Text;
    }

    #endregion
  }
}
