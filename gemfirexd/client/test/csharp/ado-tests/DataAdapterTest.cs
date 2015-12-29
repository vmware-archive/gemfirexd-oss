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
  /// Ported from Mono's SqlDataAdapterTest for System.Data.SqlClient.
  /// </summary>
  [TestFixture]
  public class DataAdapterTest : TestBase
  {
    DbDataAdapter m_adapter;
    DbDataReader m_reader;

    private static bool m_rowUpdated = false;
    private static bool m_rowUpdating = false;

    bool m_fillErrorContinue = false;

    #region SetUp/TearDown methods

    protected override string[] CommonTablesToCreate ()
    {
      return new string[] { "employee", "numeric_family" };
    }

    protected override string[] GetProceduresToDrop()
    {
      return new string[] { "multipleResultSets", "multipleNoColumnResults",
        "updateAndQuery", "multiplePKResultSets" };
    }

    protected override string[] GetTablesToDrop()
    {
      string[] tablesToDrop = { "tmpTestTable" };
      string[] commonTables = CommonTablesToCreate();
      string[] allTablesToDrop = new string[tablesToDrop.Length +
                                            commonTables.Length];
      int index = 0;
      foreach (string table in tablesToDrop) {
        allTablesToDrop[index++] = table;
      }
      foreach (string table in commonTables) {
        allTablesToDrop[index++] = table;
      }
      return allTablesToDrop;
    }

    [TearDown]
    public override void TearDown()
    {
      if (m_adapter != null) {
        m_adapter.Dispose();
        m_adapter = null;
      }
      if (m_reader != null) {
        m_reader.Close();
        m_reader = null;
      }
      base.TearDown();
    }

    #endregion

    #region Tests

    [Test]
    public void Update_DeleteRow()
    {
      m_cmd = m_conn.CreateCommand();

      DataTable dt = new DataTable();
      m_adapter = GetDataAdapter();
      m_cmd.CommandText = "SELECT * FROM employee";
      m_adapter.SelectCommand = m_cmd;
      DbCommandBuilder builder = GetCommandBuilder(m_adapter);
      m_adapter.Fill(dt);

      DateTime now = DateTime.Now;
      DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));

      try {
        DataRow newRow = dt.NewRow();
        newRow["id"] = 6002;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        DbCommand cmd = CheckNewEmployeeRow(6002, dob, doj);

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row.Delete();
          }
        }
        m_adapter.Update(dt);

        m_reader = cmd.ExecuteReader();
        Assert.IsFalse(m_reader.Read());
        m_reader.Close();

        // also try explicitly setting the DeleteCommand
        //m_adapter.DeleteCommand = builder.GetDeleteCommand();

        newRow = dt.NewRow();
        newRow["id"] = 6002;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        cmd = CheckNewEmployeeRow(6002, dob, doj);

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row.Delete();
          }
        }
        m_adapter.Update(dt);

        m_reader = cmd.ExecuteReader();
        Assert.IsFalse(m_reader.Read());
        m_reader.Close();

        // now with useColumnsForParameterNames as true
        //m_adapter.DeleteCommand = builder.GetDeleteCommand(true);

        newRow = dt.NewRow();
        newRow["id"] = 6002;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        cmd = CheckNewEmployeeRow(6002, dob, doj);

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row.Delete();
          }
        }
        m_adapter.Update(dt);

        m_reader = cmd.ExecuteReader();
        Assert.IsFalse(m_reader.Read());
        m_reader.Close();

        // now explicitly set the DeleteCommand using GFXDCommandBuilder
        if (builder is GFXDCommandBuilder) {
          m_adapter.DeleteCommand = ((GFXDCommandBuilder)builder)
            .GetDeleteCommand();

          newRow = dt.NewRow();
          newRow["id"] = 6002;
          newRow["fname"] = "boston";
          newRow["dob"] = dob;
          newRow["doj"] = doj;
          newRow["email"] = "mono@novell.com";
          dt.Rows.Add(newRow);
          m_adapter.Update(dt);

          // check row inserted
          cmd = CheckNewEmployeeRow(6002, dob, doj);

          foreach (DataRow row in dt.Rows) {
            if (((int)row["id"]) == 6002) {
              row.Delete();
            }
          }
          m_adapter.Update(dt);

          m_reader = cmd.ExecuteReader();
          Assert.IsFalse(m_reader.Read());
          m_reader.Close();

          // now with useColumnsForParameterNames as true
          m_adapter.DeleteCommand = ((GFXDCommandBuilder)builder)
            .GetDeleteCommand(true);

          newRow = dt.NewRow();
          newRow["id"] = 6002;
          newRow["fname"] = "boston";
          newRow["dob"] = dob;
          newRow["doj"] = doj;
          newRow["email"] = "mono@novell.com";
          dt.Rows.Add(newRow);
          m_adapter.Update(dt);

          // check row inserted
          cmd = CheckNewEmployeeRow(6002, dob, doj);

          foreach (DataRow row in dt.Rows) {
            if (((int)row["id"]) == 6002) {
              row.Delete();
            }
          }
          m_adapter.Update(dt);

          m_reader = cmd.ExecuteReader();
          Assert.IsFalse(m_reader.Read());
          m_reader.Close();
        }
      } finally {
        CleanEmployeeTable();
      }
    }

    [Test]
    public void Update_InsertRow()
    {
      m_cmd = m_conn.CreateCommand();

      DataTable dt = new DataTable();
      m_adapter = GetDataAdapter();
      m_cmd.CommandText = "SELECT * FROM employee";
      m_adapter.SelectCommand = m_cmd;
      DbCommandBuilder builder = GetCommandBuilder(m_adapter);
      m_adapter.Fill(dt);

      DateTime now = DateTime.Now;
      DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));

      try {
        DataRow newRow = dt.NewRow();
        newRow["id"] = 6002;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        CheckNewEmployeeRow(6002, dob, doj);

        // also try explicitly setting the InsertCommand
        //m_adapter.InsertCommand = builder.GetInsertCommand();

        newRow = dt.NewRow();
        newRow["id"] = 6003;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        CheckNewEmployeeRow(6003, dob, doj);

        // now with useColumnsForParameterNames as true
        //m_adapter.InsertCommand = builder.GetInsertCommand(true);

        newRow = dt.NewRow();
        newRow["id"] = 6004;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        // check row inserted
        CheckNewEmployeeRow(6004, dob, doj);

        // now explicitly set the InsertCommand using GFXDCommandBuilder
        if (builder is GFXDCommandBuilder) {
          m_adapter.InsertCommand = ((GFXDCommandBuilder)builder)
            .GetInsertCommand();

          newRow = dt.NewRow();
          newRow["id"] = 6005;
          newRow["fname"] = "boston";
          newRow["dob"] = dob;
          newRow["doj"] = doj;
          newRow["email"] = "mono@novell.com";
          dt.Rows.Add(newRow);
          m_adapter.Update(dt);

          // check row inserted
          CheckNewEmployeeRow(6005, dob, doj);

          // now with useColumnsForParameterNames as true
          m_adapter.InsertCommand = ((GFXDCommandBuilder)builder)
            .GetInsertCommand(true);

          newRow = dt.NewRow();
          newRow["id"] = 6006;
          newRow["fname"] = "boston";
          newRow["dob"] = dob;
          newRow["doj"] = doj;
          newRow["email"] = "mono@novell.com";
          dt.Rows.Add(newRow);
          m_adapter.Update(dt);

          // check row inserted
          CheckNewEmployeeRow(6006, dob, doj);
        }
      } finally {
        CleanEmployeeTable();
      }
    }

    [Test]
    public void Update_UpdateRow()
    {
      m_cmd = m_conn.CreateCommand();

      DataTable dt = new DataTable();
      m_adapter = GetDataAdapter();
      m_cmd.CommandText = "SELECT * FROM employee";
      m_adapter.SelectCommand = m_cmd;
      DbCommandBuilder builder = GetCommandBuilder(m_adapter);
      m_adapter.Fill(dt);

      DateTime now = DateTime.Now;
      DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));

      try {
        DataRow newRow = dt.NewRow();
        newRow["id"] = 6002;
        newRow["fname"] = "boston";
        newRow["dob"] = dob;
        newRow["doj"] = doj;
        newRow["email"] = "mono@novell.com";
        dt.Rows.Add(newRow);
        m_adapter.Update(dt);

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row["lname"] = "de Icaza";
          }
        }
        m_adapter.Update(dt);

        DbCommand cmd = m_conn.CreateCommand();
        cmd.CommandText = "SELECT id, fname, lname, dob, doj, email" +
          " FROM employee WHERE id = 6002";
        m_reader = cmd.ExecuteReader();
        Assert.IsTrue(m_reader.Read(), "#A1");
        Assert.AreEqual(6002, m_reader.GetValue(0), "#A2");
        Assert.AreEqual("boston", m_reader.GetValue(1), "#A3");
        Assert.AreEqual("de Icaza", m_reader.GetValue(2), "#A4");
        Assert.AreEqual(dob, m_reader.GetValue(3), "#A5");
        Assert.AreEqual(doj, m_reader.GetValue(4), "#A6");
        Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#A7");
        Assert.IsFalse(m_reader.Read(), "#A8");
        m_reader.Close();

        // also try explicitly setting the UpdateCommand
        //m_adapter.UpdateCommand = builder.GetUpdateCommand();

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row["lname"] = "none";
          }
        }
        m_adapter.Update(dt);

        m_reader = cmd.ExecuteReader();
        Assert.IsTrue(m_reader.Read(), "#B1");
        Assert.AreEqual(6002, m_reader.GetValue(0), "#B2");
        Assert.AreEqual("boston", m_reader.GetValue(1), "#B3");
        Assert.AreEqual("none", m_reader.GetValue(2), "#B4");
        Assert.AreEqual(dob, m_reader.GetValue(3), "#B5");
        Assert.AreEqual(doj, m_reader.GetValue(4), "#B6");
        Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#B7");
        Assert.IsFalse(m_reader.Read(), "#B8");
        m_reader.Close();

        // now with useColumnsForParameterNames as true
        //m_adapter.UpdateCommand = builder.GetUpdateCommand(true);

        foreach (DataRow row in dt.Rows) {
          if (((int)row["id"]) == 6002) {
            row["lname"] = "de Icaza";
          }
        }
        m_adapter.Update(dt);

        m_reader = cmd.ExecuteReader();
        Assert.IsTrue(m_reader.Read(), "#C1");
        Assert.AreEqual(6002, m_reader.GetValue(0), "#C2");
        Assert.AreEqual("boston", m_reader.GetValue(1), "#C3");
        Assert.AreEqual("de Icaza", m_reader.GetValue(2), "#C4");
        Assert.AreEqual(dob, m_reader.GetValue(3), "#C5");
        Assert.AreEqual(doj, m_reader.GetValue(4), "#C6");
        Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#C7");
        Assert.IsFalse(m_reader.Read(), "#C8");
        m_reader.Close();

        // now explicitly set the UpdateCommand using GFXDCommandBuilder
        if (builder is GFXDCommandBuilder) {
          m_adapter.UpdateCommand = ((GFXDCommandBuilder)builder)
            .GetUpdateCommand();

          foreach (DataRow row in dt.Rows) {
            if (((int)row["id"]) == 6002) {
              row["lname"] = "none";
            }
          }
          m_adapter.Update(dt);

          m_reader = cmd.ExecuteReader();
          Assert.IsTrue(m_reader.Read(), "#D1");
          Assert.AreEqual(6002, m_reader.GetValue(0), "#D2");
          Assert.AreEqual("boston", m_reader.GetValue(1), "#D3");
          Assert.AreEqual("none", m_reader.GetValue(2), "#D4");
          Assert.AreEqual(dob, m_reader.GetValue(3), "#D5");
          Assert.AreEqual(doj, m_reader.GetValue(4), "#D6");
          Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#D7");
          Assert.IsFalse(m_reader.Read(), "#D8");
          m_reader.Close();

          // now with useColumnsForParameterNames as true
          m_adapter.UpdateCommand = ((GFXDCommandBuilder)builder)
            .GetUpdateCommand(true);

          foreach (DataRow row in dt.Rows) {
            if (((int)row["id"]) == 6002) {
              row["lname"] = "de Icaza";
            }
          }
          m_adapter.Update(dt);

          m_reader = cmd.ExecuteReader();
          Assert.IsTrue(m_reader.Read(), "#E1");
          Assert.AreEqual(6002, m_reader.GetValue(0), "#E2");
          Assert.AreEqual("boston", m_reader.GetValue(1), "#E3");
          Assert.AreEqual("de Icaza", m_reader.GetValue(2), "#E4");
          Assert.AreEqual(dob, m_reader.GetValue(3), "#E5");
          Assert.AreEqual(doj, m_reader.GetValue(4), "#E6");
          Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#E7");
          Assert.IsFalse(m_reader.Read(), "#E8");
          m_reader.Close();
        }
      } finally {
        CleanEmployeeTable();
      }
    }

    [Test]
    public void RowUpdatedTest()
    {
      try {
        DataTable dt = null;
        DataSet ds = new DataSet();
        m_cmd = m_conn.CreateCommand();
        m_adapter = GetDataAdapter();
        if (m_adapter is GFXDDataAdapter) {
          GFXDDataAdapter da = (GFXDDataAdapter)m_adapter;
          new GFXDCommandBuilder(da);
          m_cmd.CommandText = "Select * from employee";
          da.SelectCommand = (GFXDCommand)m_cmd;

          m_rowUpdated = false;
          m_rowUpdating = false;
          da.RowUpdated += new GFXDRowUpdatedEventHandler(OnRowUpdatedTest);
          da.RowUpdating += new GFXDRowUpdatingEventHandler(OnRowUpdatingTest);
          da.Fill(ds);

          dt = ds.Tables[0];
          dt.Rows[0][1] = "test";
          da.Update(dt);
          Assert.IsTrue(m_rowUpdated, "RowUpdated");
          Assert.IsTrue(m_rowUpdating, "RowUpdating");

          m_rowUpdated = false;
          m_rowUpdating = false;
          dt.Rows[0][1] = "suresh";
          da.Update(dt);

          da.RowUpdated -= new GFXDRowUpdatedEventHandler(OnRowUpdatedTest);
          da.RowUpdating -= new GFXDRowUpdatingEventHandler(OnRowUpdatingTest);
          Assert.IsTrue(m_rowUpdated, "RowUpdated");
          Assert.IsTrue(m_rowUpdating, "RowUpdating");
        }
      } finally {
        CleanEmployeeTable();
      }
    }

    [Test]
    public void OverloadedConstructorsTest()
    {
      DbCommand selCmd = m_conn.CreateCommand();
      selCmd.CommandText = "Select * from numeric_family";
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = selCmd;
      Assert.AreEqual(MissingMappingAction.Passthrough,
                      m_adapter.MissingMappingAction,
                      "#1 Missing Mapping action default to Passthrough");
      Assert.AreEqual(MissingSchemaAction.Add, m_adapter.MissingSchemaAction,
                      "#2 Missing Schme action default to Add");
      Assert.AreSame(selCmd, m_adapter.SelectCommand,
                     "#3 Select Command shud be a ref to the arg passed");

      DbConnection conn = OpenNewConnection();
      if (conn is GFXDConnection) {
        GFXDConnection gfxdConn = (GFXDConnection)conn;
        String selStr = "Select * from numeric_family";
        m_adapter = new GFXDDataAdapter(selStr, gfxdConn);
        Assert.AreEqual(MissingMappingAction.Passthrough,
                        m_adapter.MissingMappingAction,
                        "#4 Missing Mapping action default to Passthrough");
        Assert.AreEqual(MissingSchemaAction.Add, m_adapter.MissingSchemaAction,
                        "#5 Missing Schme action default to Add");
        Assert.AreSame(selStr, m_adapter.SelectCommand.CommandText,
                       "#6 Select Command shud be a ref to the arg passed");
        Assert.AreSame(conn, m_adapter.SelectCommand.Connection,
                       "#7 cmd.connection shud be t ref to connection obj");

        GFXDCommand selCommand = new GFXDCommand(selStr, gfxdConn);
        m_adapter = new GFXDDataAdapter(selCommand);
        Assert.AreEqual(MissingMappingAction.Passthrough,
                        m_adapter.MissingMappingAction,
                        "#8 Missing Mapping action default to Passthrough");
        Assert.AreEqual(MissingSchemaAction.Add, m_adapter.MissingSchemaAction,
                        "#9 Missing Schema action shud default to Add");
        Assert.AreSame(selCommand, m_adapter.SelectCommand, "#10");
        Assert.AreSame(selStr, m_adapter.SelectCommand.CommandText, "#11");
        Assert.AreSame(gfxdConn, m_adapter.SelectCommand.Connection, "#12");
      }
      conn.Close();
    }

    //[Test]
    public void Fill_Test_ConnState()
    {
      // Check if Connection State is maintained correctly ..
      DataSet data = new DataSet("test1");
      DbConnection conn = CreateConnection(m_defaultDriverType, null, false);
      DbCommand cmd = conn.CreateCommand();
      cmd.CommandText = "select id from numeric_family where id=1";
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = cmd;

      Assert.AreEqual(ConnectionState.Closed, cmd.Connection.State,
                      "#1 Connection shud be in closed state");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#2 One table shud be populated");
      Assert.AreEqual(ConnectionState.Closed, cmd.Connection.State,
                      "#3 Connection shud be closed state");

      data = new DataSet("test2");
      conn.Open();
      Assert.AreEqual(ConnectionState.Open, cmd.Connection.State,
                      "#3 Connection shud be open");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#4 One table shud be populated");
      Assert.AreEqual(ConnectionState.Open, cmd.Connection.State,
                      "#5 Connection shud be open");
      cmd.Connection.Close();

      // Test if connection is closed when exception occurs
      cmd.CommandText = "select id1 from numeric_family";
      try {
        m_adapter.Fill(data);
      } catch {
        if (cmd.Connection.State == ConnectionState.Open) {
          cmd.Connection.Close();
          Assert.Fail("# Connection Shud be Closed");
        }
      }
    }

    [Test]
    public void Fill_Test_Data()
    {
      // Check if a table is created for each resultset
      string batchProc = "create procedure multipleResultSets ()" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.multipleResultSets' dynamic result sets 2";
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = batchProc;
      cmd.ExecuteNonQuery();
      m_adapter = GetDataAdapter("call multipleResultSets()");
      DataSet data = new DataSet("test1");
      m_adapter.Fill(data);
      Assert.AreEqual(2, data.Tables.Count, "#A1 2 Tables shud be created");
      Assert.AreEqual("Table", data.Tables[0].TableName, "#A2");
      Assert.AreEqual("Table1", data.Tables[1].TableName, "#A3");
      Assert.AreEqual("ID", data.Tables[0].Columns[0].ColumnName, "#A4");
      Assert.AreEqual("TYPE_INT", data.Tables[0].Columns[1].ColumnName, "#A5");
      Assert.AreEqual("TYPE_VARCHAR",
                      data.Tables[1].Columns[0].ColumnName, "#A6");

      // Check if Table and Col are named correctly for unnamed columns
      batchProc = "create procedure multipleNoColumnResults ()" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.multipleNoColumnResults' dynamic result sets 2";
      cmd.CommandText = batchProc;
      cmd.ExecuteNonQuery();
      cmd.CommandText = "multipleNoColumnResults";
      cmd.CommandType = CommandType.StoredProcedure;
      m_adapter.SelectCommand = cmd;
      data = new DataSet("test2");
      m_adapter.Fill(data);
      Assert.AreEqual(2, data.Tables.Count, "#B1 2 Tables shud be created");
      Assert.AreEqual("Table", data.Tables[0].TableName, "#B2");
      Assert.AreEqual("Table1", data.Tables[1].TableName, "#B3");
      Assert.AreEqual(2, data.Tables[0].Columns.Count,
                      "#B4 2 Columns shud be created");
      Assert.AreEqual(2, data.Tables[1].Columns.Count,
                      "#B5 2 Columns shud be created");
      Assert.AreEqual("1", data.Tables[0].Columns[0].ColumnName, "#B6");
      Assert.AreEqual("2", data.Tables[0].Columns[1].ColumnName, "#B7");
      Assert.AreEqual("1", data.Tables[1].Columns[0].ColumnName, "#B8");
      Assert.AreEqual("2", data.Tables[1].Columns[1].ColumnName, "#B9");

      // Check if dup columns are named correctly
      string query = "select A.id, B.id, C.id from numeric_family A, " +
        "numeric_family B , numeric_family C";
      cmd.CommandText = query;
      cmd.CommandType = CommandType.Text;
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = cmd;
      data = new DataSet("test3");
      m_adapter.Fill(data);

      // NOTE msdotnet contradicts documented behavior
      // as per documentation the column names should be 
      // id1,id2,id3 .. but msdotnet returns id,id1,id2
      Assert.AreEqual("ID", data.Tables[0].Columns[0].ColumnName, "#C1" +
                      " if colname is duplicated ,shud be col,col1,col2 etc");
      Assert.AreEqual("ID1", data.Tables[0].Columns[1].ColumnName, "#C2" +
                      " if colname is duplicated shud be col,col1,col2 etc");
      Assert.AreEqual("ID2", data.Tables[0].Columns[2].ColumnName, "#C3" +
                      " if colname is duplicated shud be col,col1,col2 etc");

      // Test if tables are created and named accordingly,
      // but only for those queries returning result sets
      batchProc = "create procedure updateAndQuery ()" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.updateAndQuery' dynamic result sets 2";
      cmd.CommandText = batchProc;
      cmd.ExecuteNonQuery();
      cmd.CommandText = "call updateAndQuery()";
      m_adapter.SelectCommand = cmd;
      data = new DataSet("test4");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#D1 Tables shud be named" +
                      " only for queries returning a resultset");
      Assert.AreEqual("Table", data.Tables[0].TableName,
                      "#D2 The first resutlset shud have 'Table' as its name");

      // Test behavior with an outerjoin
      cmd.CommandText = "select A.id, B.type_int from numeric_family A" +
        " LEFT OUTER JOIN numeric_family B on A.id = B.type_int";
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = cmd;
      data = new DataSet("test5");
      m_adapter.Fill(data);
      Assert.AreEqual(0, data.Tables[0].PrimaryKey.Length, "#E1 Primary Key" +
        " shudnt be set if an outer join is performed");
      Assert.AreEqual(0, data.Tables[0].Constraints.Count, "#E2 Constraints" +
        " shudnt be set if an outer join is performed");
      cmd.CommandText = "select id from numeric_family order by id";
      m_adapter.SelectCommand = cmd;
      data = new DataSet("test6");
      m_adapter.Fill(data, 1, 1, "numeric_family");
      Assert.AreEqual(1, data.Tables[0].Rows.Count, "#E3");
      // 2nd row (0 indexes in adapter.Fill) should be 2 in order
      Assert.AreEqual(2, data.Tables[0].Rows[0][0], "#E4");

      // only one test for DataTable.. DataSet tests covers others  
      cmd.CommandText = "select id from numeric_family";
      m_adapter.SelectCommand = cmd;
      DataTable table = new DataTable("table1");
      m_adapter.Fill(table);
      Assert.AreEqual(4, table.Rows.Count, "#F1");
    }

    [Test]
    public void Fill_Test_PriKey()
    {
      // Test if Primary Key & Constraints Collection is correct
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select id,type_int from numeric_family";
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = m_cmd;
      m_adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
      DataSet data = new DataSet("test1");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables[0].PrimaryKey.Length,
                      "#1 Primary Key shud be set");
      Assert.AreEqual(1, data.Tables[0].Constraints.Count,
                      "#2 Constraints shud be set");
      Assert.AreEqual(4, data.Tables[0].Rows.Count, "#3 No Of Rows shud be 4");

      // Test if data is correctly merged 
      m_adapter.Fill(data);
      Assert.AreEqual(4, data.Tables[0].Rows.Count,
                      "#4 No of Row shud still be 4");

      // Test if rows are appended  and not merged 
      // when primary key is not returned in the result-set
      string query = "Select type_varchar from numeric_family";
      m_adapter.SelectCommand.CommandText = query;
      data = new DataSet("test2");
      m_adapter.Fill(data);
      Assert.AreEqual(4, data.Tables[0].Rows.Count, "#5 No of Rows shud be 4");
      m_adapter.Fill(data);
      Assert.AreEqual(8, data.Tables[0].Rows.Count,
                      "#6 No of Rows shud double now");
    }

    //[Test]
    public void Fill_Test_Exceptions()
    {
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = "select * from numeric_family";
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = m_cmd;
      DataSet data = new DataSet("test1");
      try {
        m_adapter.Fill(data, -1, 0, "numeric_family");
        Assert.Fail("#1 Exception shud be thrown: Incorrect Arguments");
      } catch (AssertionException e) {
        throw e;
      } catch (Exception e) {
        Assert.AreEqual(typeof(ArgumentException), e.GetType(),
                        "#2 Incorrect Exception: " + e);
      }

      try {
        m_adapter.Fill(data, 0, -1, "numeric_family");
        Assert.Fail("#3 Exception shud be thrown: Incorrect Arguments");
      } catch (Exception e) {
        Assert.AreEqual(typeof(ArgumentException), e.GetType(),
                        "#4 Incorrect Exception: " + e);
      }

      /*
      // NOTE msdotnet contradicts documented behavior
      // InvalidOperationException is expected if table is not valid  
      try {
        adapter.Fill (data , 0 , 0 , "invalid_talbe_name");
      } catch (InvalidOperationException e) {
        ex = e;
      } catch (Exception e){
        Assert.Fail ("#5 Exception shud be thrown: incorrect arugments");
      }
      Assert.IsNotNull (ex , "#6 Exception shud be thrown: incorrect args");
      // tmp .. can be removed once the bug if fixed
      adapter.SelectCommand.Connection.Close();
      ex = null;
      */

      try {
        m_adapter.Fill(null, 0, 0, "numeric_family");
        Assert.Fail("#7 Exception shud be thrown: Invalid Dataset");
      } catch (Exception e) {
        Assert.AreEqual(typeof(ArgumentNullException), e.GetType(),
                        "#8 Incorrect Exception: " + e);
      }

      // close the connection
      m_adapter.SelectCommand.Connection.Close();
      try {
        m_adapter.Fill(data);
        Assert.Fail("#9 Exception shud be thrown: Invalid Connection");
      } catch (DbException ex) {
        Assert.IsNotNull(ex.Message, "#9");
        Assert.IsTrue(ex.Message.Contains("08003"), "#10: " + ex.Message);
      }

      // now with null connection
      m_adapter.SelectCommand.Connection = null;
      try {
        m_adapter.Fill(data);
        Assert.Fail("#9 Exception shud be thrown: Invalid Connection");
      } catch (DbException ex) {
        Assert.IsNotNull(ex.Message, "#11");
        Assert.IsTrue(ex.Message.Contains("08003"), "#12: " + ex.Message);
      }
    }

    [Test]
    public void GetFillParametersTest()
    {
      string query = "select id, type_bit from numeric_family where id > ?";
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = query;
      m_adapter = GetDataAdapter();
      m_adapter.SelectCommand = m_cmd;
      IDataParameter[] param = m_adapter.GetFillParameters();
      Assert.AreEqual(0, param.Length, "#1 size shud be 0");

      DbParameter param1 = m_cmd.CreateParameter();
      param1.ParameterName = "@param1";
      param1.Value = 2;
      m_adapter.SelectCommand.Parameters.Add(param1);

      param = m_adapter.GetFillParameters();
      Assert.AreEqual(1, param.Length, "#2 count shud be 1");
      Assert.AreEqual(param1, param[0], "#3 Params shud be equal");
    }

    [Test]
    public void FillSchemaTest()
    {
      string query = "select * from invalid_table";

      // Test if connection is closed if exception occurs during fill schema
      m_adapter = GetDataAdapter(query);
      DataSet data = new DataSet("test");
      try {
        m_adapter.FillSchema(data, SchemaType.Source);
        Assert.Fail("#A1 Expected to fail due to invalid table name");
      } catch (DbException ex) {
        Assert.IsNotNull(ex.Message, "#A2");
        Assert.IsTrue(ex.Message.Contains("42X05"), "#A3: " + ex.Message);
      }

      // Test Primary Key is set (since primary key column returned)
      query = "select id, type_int from numeric_family where id=1";
      m_adapter = GetDataAdapter(query);
      data = new DataSet("test1");
      m_adapter.FillSchema(data, SchemaType.Source);

      Assert.AreEqual(1, data.Tables[0].PrimaryKey.Length,
                      "#B1 Primary Key property must be set");

      // Test Primary Key is not set (since primary key column is returned)
      query = "select type_varchar, type_int from numeric_family where id=1";
      m_adapter = GetDataAdapter(query);
      data = new DataSet("test2");
      m_adapter.FillSchema(data, SchemaType.Source);
      Assert.AreEqual(0, data.Tables[0].PrimaryKey.Length,
                      "#B2 Primary Key property should not be set");

      // Test multiple tables are created for a batch query
      query = "create procedure multiplePKResultSets()" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.multiplePKResultSets' dynamic result sets 2";
      m_cmd = m_conn.CreateCommand();
      m_cmd.CommandText = query;
      m_cmd.ExecuteNonQuery();
      data = new DataSet("test3");
      query = "call multiplePKResultSets()";
      m_adapter = GetDataAdapter(query);
      m_adapter.FillSchema(data, SchemaType.Source);
      Assert.AreEqual(2, data.Tables.Count,
                      "#B3 A table shud be created for each Result Set");
      Assert.AreEqual(2, data.Tables[0].Columns.Count,
                      "#B4 should have 2 columns");
      Assert.AreEqual(3, data.Tables[1].Columns.Count,
                      "#B5 Should have 3 columns");
      Assert.AreEqual(0, data.Tables[0].PrimaryKey[0].Ordinal,
                      "#B6 Expected PK constraint at first column");
      Assert.AreEqual(1, data.Tables[1].PrimaryKey[0].Ordinal,
                      "#B7 Expected PK constraint at second column");

      // Test if table names and column names  are filled correctly
      query = "create procedure multipleNoColumnResults()" +
        " language java parameter style java external name" +
        " 'tests.TestProcedures.multipleNoColumnResults' dynamic result sets 2";
      m_cmd.CommandText = query;
      m_cmd.ExecuteNonQuery();
      query = "call multipleNoColumnResults ()";
      m_adapter = GetDataAdapter(query);
      data = new DataSet("test4");
      m_adapter.FillSchema(data, SchemaType.Source);
      Assert.AreEqual("Table", data.Tables[0].TableName);
      Assert.AreEqual("Table1", data.Tables[1].TableName);
      Assert.AreEqual("1", data.Tables[0].Columns[0].ColumnName,
                      "#C1 Unnamed col shud be named as 'ColumnN'");
      Assert.AreEqual("2", data.Tables[0].Columns[1].ColumnName,
                      "#C2 Unnamed col shud be named as 'ColumnN'");
      Assert.AreEqual("1", data.Tables[1].Columns[0].ColumnName,
                      "#C3 Unnamed col shud be named as 'ColumnN'");
      Assert.AreEqual("2", data.Tables[1].Columns[1].ColumnName,
                      "#C4 Unnamed col shud be named as 'ColumnN'");
    }

    [Test]
    public void MissingSchemaActionTest()
    {
      m_adapter = GetDataAdapter("select id, type_varchar, type_int" +
                                 " from numeric_family where id<=4");
      DataSet data = new DataSet();
      Assert.AreEqual(MissingSchemaAction.Add, m_adapter.MissingSchemaAction,
                      "#1 Default Value");

      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#1 One table shud be populated");
      Assert.AreEqual(3, data.Tables[0].Columns.Count,
                      "#2 Missing cols are added");
      Assert.AreEqual(0, data.Tables[0].PrimaryKey.Length,
                      "#3 Default Value");

      m_adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
      data.Reset();
      m_adapter.Fill(data);
      Assert.AreEqual(3, data.Tables[0].Columns.Count,
                      "#4 Missing cols are added");
      Assert.AreEqual(1, data.Tables[0].PrimaryKey.Length,
                      "#5 Default Value");

      m_adapter.MissingSchemaAction = MissingSchemaAction.Ignore;
      data.Reset();
      m_adapter.Fill(data);
      Assert.AreEqual(0, data.Tables.Count, "#6 Data shud be ignored");

      m_adapter.MissingSchemaAction = MissingSchemaAction.Error;
      data.Reset();
      try {
        m_adapter.Fill(data);
        Assert.Fail("#8 Exception shud be thrown: Schema Mismatch");
      } catch (InvalidOperationException ex) {
        Assert.AreEqual(typeof(InvalidOperationException), ex.GetType(), "#9");
      }

      // Test for invalid MissingSchema Value
      try {
        m_adapter.MissingSchemaAction = (MissingSchemaAction)(-5000);
        Assert.Fail("#10 Exception shud be thrown: Invalid Value");
      } catch (ArgumentOutOfRangeException ex) {
        Assert.AreEqual(typeof(ArgumentOutOfRangeException), ex.GetType(), "#11");
      }

      // Tests if Data is filled correctly if schema is defined
      // manually and MissingSchemaAction.Error is set
      m_adapter.MissingSchemaAction = MissingSchemaAction.Error;
      data.Reset();
      DataTable table = data.Tables.Add("Table");
      table.Columns.Add("id");
      table.Columns.Add("type_varchar");
      table.Columns.Add("type_int");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#12");
      Assert.AreEqual(4, data.Tables[0].Rows.Count, "#13");
    }

    [Test]
    public void MissingMappingActionTest()
    {
      m_adapter = GetDataAdapter("select id, type_int" +
                                 " from numeric_family where id=1");
      DataSet data = new DataSet();
      Assert.AreEqual(m_adapter.MissingMappingAction,
                      MissingMappingAction.Passthrough, "#1 Default Value");
      m_adapter.Fill(data);
      Assert.AreEqual(1, data.Tables.Count, "#2 One Table shud be created");
      Assert.AreEqual(2, data.Tables[0].Columns.Count,
                      "#3 Two Cols shud be created");

      m_adapter.MissingMappingAction = MissingMappingAction.Ignore;
      data.Reset();
      m_adapter.Fill(data);
      Assert.AreEqual(0, data.Tables.Count, "#4 No table shud be created");

      m_adapter.MissingMappingAction = MissingMappingAction.Error;
      data.Reset();
      try {
        m_adapter.Fill(data);
        Assert.Fail("#5 Exception shud be thrown : Mapping is missing");
      } catch (InvalidOperationException ex) {
        Assert.AreEqual(typeof(InvalidOperationException), ex.GetType(), "#6");
      }

      try {
        m_adapter.MissingMappingAction = (MissingMappingAction)(-5000);
        Assert.Fail("#7 Exception shud be thrown : Invalid Value");
      } catch (ArgumentOutOfRangeException ex) {
        Assert.AreEqual(typeof(ArgumentOutOfRangeException), ex.GetType(), "#8");
      }

      // Test if mapping the column and table names works correctly
      m_adapter.MissingMappingAction = MissingMappingAction.Error;
      data.Reset();
      DataTable table = data.Tables.Add("numeric_family_1");
      table.Columns.Add("id_1");
      table.Columns.Add("type_int_1");
      table.Columns.Add("type_varchar_1");
      DataTableMapping tableMap = m_adapter.TableMappings.Add(
        "numeric_family", "numeric_family_1");
      tableMap.ColumnMappings.Add("ID", "ID_1");
      tableMap.ColumnMappings.Add("TYPE_VARCHAR", "TYPE_VARCHAR_1");
      tableMap.ColumnMappings.Add("TYPE_INT", "TYPE_INT_1");
      m_adapter.Fill(data, "numeric_family");
      Assert.AreEqual(1, data.Tables.Count,
                      "#9 The DataTable shud be correctly mapped");
      Assert.AreEqual(3, data.Tables[0].Columns.Count,
                      "#10 The DataColumns shud be corectly mapped");
      Assert.AreEqual(1, data.Tables[0].Rows.Count,
                      "#11 Data shud be populated if mapping is correct");
    }

    [Test]
    // bug #76433
    public void FillSchema_ValuesTest()
    {
      using (DbConnection conn = OpenNewConnection()) {
        IDbCommand command = conn.CreateCommand();

        // Create Temp Table
        string cmd = "Create Table tmpTestTable (";
        cmd += "Field1 DECIMAL (10) NOT NULL,";
        cmd += "Field2 DECIMAL(19))";
        command.CommandText = cmd;
        command.ExecuteNonQuery();

        DataSet dataSet = new DataSet();
        string selectString = "SELECT * FROM tmpTestTable";
        IDbDataAdapter dataAdapter = GetDataAdapter(selectString);
        dataAdapter.FillSchema(dataSet, SchemaType.Mapped);

        Assert.AreEqual(1, dataSet.Tables.Count, "#1");
        Assert.IsFalse(dataSet.Tables[0].Columns[0].AllowDBNull, "#2");
        Assert.IsTrue(dataSet.Tables[0].Columns[1].AllowDBNull, "#3");
      }
    }

    [Test]
    public void Fill_CheckSchema()
    {
      IDbCommand command = m_conn.CreateCommand();

      // Create Temp Table
      string cmd = "Create Table tmpTestTable (";
      cmd += "id int primary key,";
      cmd += "field int not null)";
      command.CommandText = cmd;
      command.ExecuteNonQuery();

      DataSet dataSet = new DataSet();
      string selectString = "SELECT * from tmpTestTable";
      IDbDataAdapter dataAdapter = GetDataAdapter(selectString);
      dataAdapter.Fill(dataSet);
      Assert.AreEqual(1, dataSet.Tables.Count, "#A1");
      Assert.AreEqual(2, dataSet.Tables[0].Columns.Count, "#A2");
      Assert.IsTrue(dataSet.Tables[0].Columns[1].AllowDBNull, "#A3");
      Assert.AreEqual(0, dataSet.Tables[0].PrimaryKey.Length, "#A4");

      dataSet.Reset();
      dataAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
      dataAdapter.Fill(dataSet);
      Assert.AreEqual(1, dataSet.Tables.Count, "#B1");
      Assert.AreEqual(2, dataSet.Tables[0].Columns.Count, "#B2");
      Assert.IsFalse(dataSet.Tables[0].Columns[1].AllowDBNull, "#B3");
      Assert.AreEqual(1, dataSet.Tables[0].PrimaryKey.Length, "#B4");
    }

    [Test]
    public void FillSchema_CheckSchema()
    {
      using (DbConnection conn = OpenNewConnection()) {
        IDbCommand command = conn.CreateCommand();

        // Create Temp Table
        string cmd = "Create Table tmpTestTable (";
        cmd += "id int primary key,";
        cmd += "field int not null)";
        command.CommandText = cmd;
        command.ExecuteNonQuery();

        DataSet dataSet = new DataSet();
        string selectString = "SELECT * from tmpTestTable";
        IDbDataAdapter dataAdapter = GetDataAdapter(selectString);

        dataAdapter.FillSchema(dataSet, SchemaType.Mapped);
        Assert.IsFalse(dataSet.Tables[0].Columns[1].AllowDBNull, "#1");

        dataSet.Reset();
        dataAdapter.MissingSchemaAction = MissingSchemaAction.Add;
        dataAdapter.FillSchema(dataSet, SchemaType.Mapped);
        Assert.IsFalse(dataSet.Tables[0].Columns[1].AllowDBNull, "#2");

        dataSet.Reset();
        dataAdapter.MissingSchemaAction = MissingSchemaAction.Ignore;
        dataAdapter.FillSchema(dataSet, SchemaType.Mapped);
        Assert.AreEqual(0, dataSet.Tables.Count, "#3");

        dataSet.Reset();
        dataAdapter.MissingSchemaAction = MissingSchemaAction.Error;
        try {
          dataAdapter.FillSchema(dataSet, SchemaType.Mapped);
          Assert.Fail("#4 Error should be thrown");
        } catch (InvalidOperationException ex) {
          Assert.AreEqual(typeof(InvalidOperationException),
                          ex.GetType(), "#4");
        }
      }
    }

    [Test]
    public void CreateViewSSPITest()
    {
      string sql = "create view MONO_TEST_VIEW as select * from Numeric_family";
      DbCommand dbcmd = m_conn.CreateCommand();
      dbcmd.CommandText = sql;
      dbcmd.ExecuteNonQuery();

      sql = "drop view MONO_TEST_VIEW";
      dbcmd = m_conn.CreateCommand();
      dbcmd.CommandText = sql;
      dbcmd.ExecuteNonQuery();
    }

    [Test]
    public void Fill_RelatedTables()
    {
      DbCommand command = m_conn.CreateCommand();

      DataSet dataSet = new DataSet();
      string selectString = "SELECT id,type_int from numeric_family where id<3";
      command.CommandText = selectString;
      DbDataAdapter dataAdapter = GetDataAdapter();
      dataAdapter.SelectCommand = command;

      DataTable table2 = dataSet.Tables.Add("table2");
      DataColumn ccol1 = table2.Columns.Add("id", typeof(int));
      table2.Columns.Add("type_int", typeof(int));

      DataTable table1 = dataSet.Tables.Add("table1");
      DataColumn pcol1 = table1.Columns.Add("id", typeof(int));
      table1.Columns.Add("type_int", typeof(int));

      table2.Constraints.Add("fk", pcol1, ccol1);
      //table1.Constraints.Add ("fk1", pcol2, ccol2);

      dataSet.EnforceConstraints = false;
      dataAdapter.Fill(dataSet, "table1");
      dataAdapter.Fill(dataSet, "table2");

      //Should not throw an exception
      dataSet.EnforceConstraints = true;

      Assert.AreEqual(2, table1.Rows.Count, "#1");
      Assert.AreEqual(2, table2.Rows.Count, "#2");
    }

    [Test]
    public void UpdateBatchSizeTest()
    {
      m_adapter = GetDataAdapter();
      Assert.AreEqual(1, m_adapter.UpdateBatchSize,
                      "#1 The default value should be 1");
      m_adapter.UpdateBatchSize = 3;
      Assert.AreEqual(3, m_adapter.UpdateBatchSize,
                      "#2 The value should be 3 after setting the property" +
                      "UpdateBatchSize to 3");
    }

    [Test]
    public void UpdateBatchSizeArgumentOutOfRangeTest()
    {
      m_adapter = GetDataAdapter();
      try {
        m_adapter.UpdateBatchSize = -2;
        Assert.Fail("#1");
      } catch (ArgumentOutOfRangeException ex) {
        // got expected exception
        Assert.IsNotNull(ex.Message, "#2");
      }
    }

    /// <summary>
    /// This test checks for simple set of batch inserts followed by
    /// batch updates and deletes using DataAdapter.
    /// </summary>
    [Test]
    public void BatchSizeInsertUpdateDeleteTest()
    {
      m_adapter = GetDataAdapter("select * from employee");
      GetCommandBuilder(m_adapter);
      Assert.AreEqual(1, m_adapter.UpdateBatchSize,
                      "#A1 The default value should be 1");
      m_adapter.UpdateBatchSize = 3;
      Assert.AreEqual(3, m_adapter.UpdateBatchSize,
                      "#A2 The value should be 3 after setting the property" +
                      " UpdateBatchSize to 3");

      // some inserts that should be sent in batches
      DataTable dt = new DataTable();
      m_adapter.Fill(dt);
      DateTime now = DateTime.Now;
      DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                  now.Hour, now.Minute, now.Second);
      dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));

      int startInsertId = 1000, endInsertId = 1500;
      int startUpdateId = 1273, endUpdateId = 1355;
      int startDeleteId = 1133, endDeleteId = 1437;
      int rowId;
      int sumRowIds = 0;
      try {
        for (rowId = startInsertId; rowId <= endInsertId; ++rowId) {
          DataRow newRow = dt.NewRow();
          sumRowIds += rowId;
          newRow["id"] = rowId;
          newRow["fname"] = "gfxd" + rowId;
          newRow["dob"] = dob;
          newRow["doj"] = doj;
          newRow["email"] = "test" + rowId + "@vmware.com";
          dt.Rows.Add(newRow);
        }
        m_adapter.Update(dt);

        // check that we have no parameters left after batch execution
        Assert.AreEqual(0, m_adapter.InsertCommand.Parameters.Count,
                        "#B1 Should have no parameters after batching");
        // a rough check for successful inserts
        DbCommand cmd = m_conn.CreateCommand();
        cmd.CommandText = "select sum(id) from employee where id >= " +
          startInsertId;
        Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                        "#B2 All inserts not done?");

        // now check for a set of updates
        int sumUpdatedRowIds = 0;
        for (int expectedId = startUpdateId; expectedId <= endUpdateId;
             ++expectedId) {
          sumUpdatedRowIds += expectedId;
          foreach (DataRow row in dt.Rows) {
            rowId = (int)row["id"];
            if (rowId == expectedId) {
              row["lname"] = "gem" + rowId;
              break;
            }
          }
        }
        m_adapter.Update(dt);

        // check that we have no parameters left after batch execution
        Assert.AreEqual(0, m_adapter.UpdateCommand.Parameters.Count,
                        "#C1 Should have no parameters after batching");
        // a rough check for successful updates
        cmd.CommandText = "select sum(id) from employee" +
          " where lname like 'gem%'";
        Assert.AreEqual(sumUpdatedRowIds, cmd.ExecuteScalar(),
                        "#C2 Updates not successful?");

        // lastly check for a set of deletes
        foreach (DataRow row in dt.Rows) {
          rowId = (int)row["id"];
          if (rowId >= startDeleteId && rowId <= endDeleteId) {
            row.Delete();
            sumRowIds -= rowId;
          }
        }
        m_adapter.Update(dt);

        // check that we have no parameters left after batch execution
        Assert.AreEqual(0, m_adapter.DeleteCommand.Parameters.Count,
                        "#D1 Should have no parameters after batching");
        // a rough check for successful deletes
        cmd.CommandText = "select sum(id) from employee where id > 4";
        Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                        "#D2 Some deletes not successful?");
      } finally {
        CleanEmployeeTable();
      }
    }

    /// <summary>
    /// This test checks for mixture of inserts/updates/deletes.
    /// </summary>
    [Test]
    public void BatchSizeMixedChangesTest()
    {
      m_adapter = GetDataAdapter("select * from employee");
      GetCommandBuilder(m_adapter);
      Assert.AreEqual(1, m_adapter.UpdateBatchSize,
                      "#A1 The default value should be 1");
      m_adapter.UpdateBatchSize = 7;
      Assert.AreEqual(7, m_adapter.UpdateBatchSize,
                      "#A2 The value should be 7 after setting the property" +
                      " UpdateBatchSize to 7");

      for (int times = 1; times <= 100; ++times) {
        DbCommand cmd = m_conn.CreateCommand();
        DataTable dt = new DataTable();
        m_adapter.Fill(dt);
        DateTime now = DateTime.Now;
        DateTime doj = new DateTime(now.Year, now.Month, now.Day,
                                    now.Hour, now.Minute, now.Second);
        DateTime dob = new DateTime(now.Year, now.Month, now.Day,
                                    now.Hour, now.Minute, now.Second);
        dob.Subtract(new TimeSpan(20 * 365, 0, 0, 0));

        TrackRandom rnd = new TrackRandom();
        int startInsertId = 1000, endInsertId = 1500;
        int startUpdateId = 1133, endUpdateId = 1437;
        int startDeleteId = 1251, endDeleteId = 1355;
        //int startInsertId = 100, endInsertId = 130;
        //int startUpdateId = 109, endUpdateId = 125;
        //int startDeleteId = 113, endDeleteId = 123;

        int rowId, endRowId;
        int insertRowId = startInsertId;
        int updateRowId = startUpdateId;
        int deleteRowId = startDeleteId;
        int sumRowIds;
        int currentBatch;
        bool changed;

        try {
          // the inserts/updates/deletes are performed in random batches of
          // sizes ranging from 3-15
          while (insertRowId <= endInsertId || updateRowId <= endUpdateId
                 || deleteRowId <= endDeleteId) {
            currentBatch = rnd.Next(3, 15);
            changed = false;
            endRowId = insertRowId + currentBatch;
            while (insertRowId <= endRowId && insertRowId <= endInsertId) {
              DataRow newRow = dt.NewRow();
              newRow["id"] = insertRowId;
              newRow["fname"] = "gfxd" + insertRowId;
              newRow["dob"] = dob;
              newRow["doj"] = doj;
              newRow["email"] = "test" + insertRowId + "@vmware.com";
              dt.Rows.Add(newRow);
              ++insertRowId;
              changed = true;
            }

            if (changed && rnd.Next(5) == 1) {
              m_adapter.Update(dt);
              // check that we have no parameters left after batch execution
              Assert.AreEqual(0, m_adapter.InsertCommand.Parameters.Count,
                              "#B1 Should have no parameters after batching");
              // a rough check for successful inserts
              sumRowIds = 0;
              for (rowId = startInsertId; rowId < insertRowId; ++rowId) {
                if (rowId < startDeleteId || rowId >= deleteRowId) {
                  sumRowIds += rowId;
                }
              }
              cmd.CommandText = "select sum(id) from employee where id >= " +
                startInsertId;
              Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                              "#B2 All inserts not done?");
            }

            // now check for a set of updates
            changed = false;
            endRowId = updateRowId + rnd.Next(0, currentBatch);
            while (updateRowId <= endRowId && updateRowId < insertRowId
                   && updateRowId <= endUpdateId) {
              foreach (DataRow row in dt.Rows) {
                if (row.RowState != DataRowState.Deleted) {
                  rowId = (int)row["id"];
                  if (rowId == updateRowId) {
                    row["lname"] = "gem" + rowId;
                    changed = true;
                    break;
                  }
                }
              }
              ++updateRowId;
            }

            if (changed && rnd.Next(5) == 2) {
              m_adapter.Update(dt);
              // check that we have no parameters left after batch execution
              Assert.IsTrue(m_adapter.UpdateCommand == null
                            || m_adapter.UpdateCommand.Parameters.Count == 0,
                              "#C1 Should have no parameters after batching");
              // a rough check for successful updates
              sumRowIds = 0;
              for (rowId = startUpdateId; rowId < updateRowId; ++rowId) {
                if (rowId < startDeleteId || rowId >= deleteRowId) {
                  sumRowIds += rowId;
                }
              }
              cmd.CommandText = "select sum(id) from employee" +
                " where lname like 'gem%'";
              Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                              "#C2 Updates not successful?");
            }

            // lastly check for a set of deletes
            changed = false;
            endRowId = deleteRowId + rnd.Next(0, currentBatch);
            DataRow[] rowArray = new DataRow[dt.Rows.Count];
            dt.Rows.CopyTo(rowArray, 0);
            foreach (DataRow row in rowArray) {
              if (row.RowState != DataRowState.Deleted) {
                rowId = (int)row["id"];
                if (rowId >= deleteRowId && rowId < insertRowId
                    && rowId <= endRowId && rowId <= endDeleteId) {
                  row.Delete();
                  changed = true;
                }
              }
            }
            if (changed) {
              if (endRowId >= insertRowId) {
                endRowId = insertRowId - 1;
              }
              if (endRowId > endDeleteId) {
                endRowId = endDeleteId;
              }
              deleteRowId = endRowId + 1;
            }

            if (changed && rnd.Next(5) == 0) {
              m_adapter.Update(dt);
              // check that we have no parameters left after batch execution
              Assert.IsTrue(m_adapter.DeleteCommand == null
                            || m_adapter.DeleteCommand.Parameters.Count == 0,
                              "#D1 Should have no parameters after batching");
              // a rough check for successful deletes
              sumRowIds = 0;
              for (rowId = startInsertId; rowId < insertRowId; ++rowId) {
                if (rowId < startDeleteId || rowId >= deleteRowId) {
                  sumRowIds += rowId;
                }
              }
              cmd.CommandText = "select sum(id) from employee where id > 4";
              Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                              "#D2 Some deletes not successful?");
            }
          }
          // check that everything is finished properly at the end
          m_adapter.Update(dt);
          // check that we have no parameters left after batch execution
          Assert.AreEqual(0, m_adapter.InsertCommand.Parameters.Count,
                          "#E1 Should have no parameters after batching");
          Assert.IsTrue(m_adapter.UpdateCommand == null
                        || m_adapter.UpdateCommand.Parameters.Count == 0,
                          "#E2 Should have no parameters after batching");
          Assert.IsTrue(m_adapter.DeleteCommand == null
                        || m_adapter.DeleteCommand.Parameters.Count == 0,
                          "#E3 Should have no parameters after batching");
          // a rough check for successful updates
          sumRowIds = 0;
          for (rowId = startUpdateId; rowId <= endUpdateId; ++rowId) {
            if (rowId < startDeleteId || rowId > endDeleteId) {
              sumRowIds += rowId;
            }
          }
          cmd.CommandText = "select sum(id) from employee" +
            " where lname like 'gem%'";
          Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                          "#E4 Updates not successful?");
          // a rough check for successful deletes
          sumRowIds = 0;
          for (rowId = startInsertId; rowId <= endInsertId; ++rowId) {
            if (rowId < startDeleteId || rowId > endDeleteId) {
              sumRowIds += rowId;
            }
          }
          cmd.CommandText = "select sum(id) from employee where id > 4";
          Assert.AreEqual(sumRowIds, cmd.ExecuteScalar(),
                          "#E5 Some deletes not successful?");
        } catch (Exception ex) {
          Console.WriteLine("Failed with exception: " + ex.ToString());
          Console.WriteLine("Failed for seed: " + rnd.Seed);
          throw;
        } finally {
          CleanEmployeeTable();
        }
      }
    }

    #endregion

    #region Internal/Private helper methods

    private DbCommand CheckNewEmployeeRow(int id, DateTime dob, DateTime doj)
    {
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = "SELECT id, fname, lname, dob, doj, email" +
        " FROM employee WHERE id = " + id;
      m_reader = cmd.ExecuteReader();
      try {
        Assert.IsTrue(m_reader.Read(), "#A1");
        Assert.AreEqual(id, m_reader.GetValue(0), "#A2");
        Assert.AreEqual("boston", m_reader.GetValue(1), "#A3");
        Assert.AreEqual(DBNull.Value, m_reader.GetValue(2), "#A4");
        Assert.AreEqual(dob, m_reader.GetValue(3), "#A5");
        Assert.AreEqual(doj, m_reader.GetValue(4), "#A6");
        Assert.AreEqual("mono@novell.com", m_reader.GetValue(5), "#A7");
        Assert.IsFalse(m_reader.Read(), "#A8");
      } finally {
        m_reader.Close();
      }
      return cmd;
    }

    private static void OnRowUpdatedTest(object sender,
                                         GFXDRowUpdatedEventArgs e)
    {
      if (e.StatementType == StatementType.Update) {
        m_rowUpdated = true;
      }
    }

    private static void OnRowUpdatingTest(object sender,
                                          GFXDRowUpdatingEventArgs e)
    {
      if (e.StatementType == StatementType.Update) {
        m_rowUpdating = true;
      }
    }

    private void ErrorHandler(object sender, FillErrorEventArgs args)
    {
      args.Continue = m_fillErrorContinue;
    }

    private DbDataAdapter GetDataAdapter(string query)
    {
      DbCommand cmd = m_conn.CreateCommand();
      cmd.CommandText = query;
      DbDataAdapter adapter = GetDataAdapter();
      adapter.SelectCommand = cmd;
      return adapter;
    }

    #endregion
  }

  class TrackRandom
  {
    private Random m_rnd;
    private int m_seed;

    internal TrackRandom()
    {
      m_seed = (int)(DateTime.Now.Ticks & 0x7FFFFFFF);
      m_rnd = new Random(m_seed);
    }

    internal TrackRandom(int seed)
    {
      m_seed = seed;
      m_rnd = new Random(seed);
    }

    internal int Next()
    {
      return m_rnd.Next();
    }

    internal int Next(int max)
    {
      return m_rnd.Next(max);
    }

    internal int Next(int min, int max)
    {
      return m_rnd.Next(min, max);
    }

    internal void NextBytes(byte[] buf)
    {
      m_rnd.NextBytes(buf);
    }

    internal double NextDouble()
    {
      return m_rnd.NextDouble();
    }

    internal int Seed
    {
      get {
        return m_seed;
      }
    }
  }

  [TestFixture]
  public class SqlDataAdapterInheritTest : DbDataAdapter
  {
    [TestFixtureSetUp]
    public void FixtureSetUp()
    {
      TestBase.InternalSetup(GetType());
      TestBase.StartGFXDServer(TestBase.DefaultDriverType, "");
    }

    [TestFixtureTearDown]
    public void FixtureTearDown()
    {
      TestBase.StopGFXDServer(TestBase.DefaultDriverType);
      com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(null);
    }

    [Test]
    public void FillDataAdapterTest()
    {
      DbConnection conn = TestBase.CreateConnection(TestBase.DefaultDriverType,
                                                    null, false);
      conn.Open();
      try {
        TestBase.CreateCommonTables(new string[] { "employee" }, conn);
        DataTable dt = new DataTable();
        DbCommand command = conn.CreateCommand();
        command.CommandText = "Select * from employee";
        SelectCommand = command;
        Fill(dt, command.ExecuteReader());
        Assert.AreEqual(4, dt.Rows.Count, "#1");
        Assert.AreEqual(6, dt.Columns.Count, "#2");
      } finally {
        TestBase.DropDBObjects(new string[0], new string[] { "employee" },
                               new string[0], new string[0], conn);
        conn.Close();
      }
    }
  }
}
