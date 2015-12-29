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
using System.IO;
using System.Text;

using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Some basic tests that exercise somewhat wide range of API taken
  /// from Mono's test suite.
  /// </summary>
  [TestFixture]
  public class BasicTests : TestBase
  {
    #region Tests

    /// <summary>
    /// This is the functional test from Mono test suite for Oracle client
    /// ported for GemFireXD driver.
    /// </summary>
    [Test]
    public void MonoTest()
    {
      Log("Current directory: " + Environment.CurrentDirectory);
      DbConnection conn = OpenNewConnection();
      DbCommand cmd = conn.CreateCommand();

      Log("  Creating table s.MONO_DB2_TEST...");
      cmd.CommandText = "CREATE TABLE s.MONO_DB2_TEST (varchar2_value " +
        "varchar(32), long_value long varchar, number_whole_value numeric(18), " +
        "number_scaled_value numeric(18,2), number_integer_value int, " +
        "float_value float, date_value date, char_value char, clob_value clob, " +
        "blob_value blob, clob_empty_value clob, blob_empty_value blob, " +
        "varchar2_null_value varchar(32), number_whole_null_value int, " +
        "number_scaled_null_value int, number_integer_null_value int, " +
        "float_null_value float, date_null_value date, char_null_value " +
        "char, clob_null_value clob, blob_null_value blob, " +
        "primary key (varchar2_value))";
      cmd.ExecuteNonQuery();

      Log("Begin Trans for table s.MONO_DB2_TEST...");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.Chaos);

      Log("  Inserting value into s.MONO_DB2_TEST...");
      cmd.Transaction = trans;
      // check for truncation error with non-blank characters
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, date_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, " +
        "'2004-12-31 14:14:14', 'US')";

      try {
        cmd.ExecuteNonQuery();
        Assert.Fail("expected a truncation error");
      } catch (DbException ex) {
        // expect a truncation error
        if (!ex.Message.ToLower().Contains("truncat")) {
          throw;
        }
      }
      // should pass now with blanks that can be truncated
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, date_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, " +
        "'2004-12-31', 'U ')";
      cmd.ExecuteNonQuery();
      trans.Commit();

      // update the blob and clob fields
      UpdateClobBlob(conn, "s.MONO_DB2_TEST", "Mono");

      // DbCommand.ExecuteReader of MONO_DB2_TEST table
      Log("  Read simple test for table s.MONO_DB2_TEST...");
      ReadSimpleTest(conn, "SELECT * FROM s.MONO_DB2_TEST");

      // verify the insert and update
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 123m,
                          DateTime.Parse("2004-12-31"), true);
    }

    /// <summary>
    /// This is the functional test from Mono test suite for Oracle client
    /// ported for GemFireXD driver with TX level READ_COMMITTED.
    /// </summary>
    [Test]
    public void MonoTestTransaction()
    {
      Log("Current directory: " + Environment.CurrentDirectory);
      DbConnection conn = OpenNewConnection();
      DbCommand cmd = conn.CreateCommand();

      Log("  Creating table s.MONO_DB2_TEST...");
      cmd.CommandText = "CREATE TABLE s.MONO_DB2_TEST (varchar2_value " +
        "varchar(32), long_value long varchar, number_whole_value numeric(18), " +
        "number_scaled_value numeric(18,2), number_integer_value int, " +
        "float_value float, char_value char, " +
        "varchar2_null_value varchar(32), number_whole_null_value int, " +
        "number_scaled_null_value int, number_integer_null_value int, " +
        "float_null_value float, date_null_value date, char_null_value " +
        "char, primary key (varchar2_value))";
      cmd.ExecuteNonQuery();

      Log("Begin Trans for table s.MONO_DB2_TEST...");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

      Log("  Inserting value into s.MONO_DB2_TEST...");
      cmd.Transaction = trans;
      // check for truncation error with non-blank characters
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, 'US')";

      try {
        cmd.ExecuteNonQuery();
        Assert.Fail("expected a truncation error");
      } catch (DbException ex) {
        // expect a truncation error
        if (!ex.Message.ToLower().Contains("truncat")) {
          throw;
        }
      }
      // should pass now with blanks that can be truncated
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, 'U ')";
      cmd.ExecuteNonQuery();
      trans.Commit();

      // DbCommand.ExecuteReader of MONO_DB2_TEST table
      Log("  Read simple test for table s.MONO_DB2_TEST...");
      ReadSimpleTest(conn, "SELECT * FROM s.MONO_DB2_TEST");

      // verify the insert and update
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 123m,
                          DateTime.MinValue, false);

      // now do an update with and without rollback
      trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
      cmd.CommandText = "UPDATE s.MONO_DB2_TEST set number_whole_value=234 " +
        "where varchar2_value='Mono'";
      cmd.ExecuteNonQuery();
      trans.Rollback();

      // expect old values only after rollback
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 123m,
                          DateTime.MinValue, false);

      trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
      cmd.ExecuteNonQuery();
      trans.Commit();
      // expect updated values after commit
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 234m,
                          DateTime.MinValue, false);
    }

    /// <summary>
    /// This is the functional test from Mono test suite for Oracle client
    /// ported for GemFireXD driver with TX level READ_UNCOMMITTED. GemFireXD
    /// upgrades it to READ_COMMITTED.
    /// </summary>
    [Test]
    public void MonoTestTransactionReadUncommitted()
    {
      Log("Current directory: " + Environment.CurrentDirectory);
      DbConnection conn = OpenNewConnection();
      DbCommand cmd = conn.CreateCommand();

      Log("  Creating table s.MONO_DB2_TEST...");
      cmd.CommandText = "CREATE TABLE s.MONO_DB2_TEST (varchar2_value " +
        "varchar(32), long_value long varchar, number_whole_value numeric(18), " +
        "number_scaled_value numeric(18,2), number_integer_value int, " +
        "float_value float, char_value char, " +
        "varchar2_null_value varchar(32), number_whole_null_value int, " +
        "number_scaled_null_value int, number_integer_null_value int, " +
        "float_null_value float, date_null_value date, char_null_value " +
        "char, primary key (varchar2_value))";
      cmd.ExecuteNonQuery();

      Log("Begin Trans for table s.MONO_DB2_TEST...");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.
                                                  ReadUncommitted);
      Assert.AreEqual(IsolationLevel.ReadCommitted, trans.IsolationLevel);

      Log("  Inserting value into s.MONO_DB2_TEST...");
      cmd.Transaction = trans;
      // check for truncation error with non-blank characters
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, 'US')";

      try {
        cmd.ExecuteNonQuery();
        Assert.Fail("expected a truncation error");
      } catch (DbException ex) {
        // expect a truncation error
        if (!ex.Message.ToLower().Contains("truncat")) {
          throw;
        }
      }
      // should pass now with blanks that can be truncated
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, 'U ')";
      cmd.ExecuteNonQuery();
      trans.Commit();

      // DbCommand.ExecuteReader of MONO_DB2_TEST table
      Log("  Read simple test for table s.MONO_DB2_TEST...");
      ReadSimpleTest(conn, "SELECT * FROM s.MONO_DB2_TEST");

      // verify the insert and update
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 123m,
                          DateTime.MinValue, false);

      // now do an update with and without rollback
      trans = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
      cmd.CommandText = "UPDATE s.MONO_DB2_TEST set number_whole_value=234 " +
        "where varchar2_value='Mono'";
      cmd.ExecuteNonQuery();
      trans.Rollback();

      // expect old values only after rollback
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 123m,
                          DateTime.MinValue, false);

      trans = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
      cmd.ExecuteNonQuery();
      trans.Commit();
      // expect updated values after commit
      VerifyCommonColumns(conn, "s.MONO_DB2_TEST", 234m,
                          DateTime.MinValue, false);
    }

    /// <summary>
    /// Test for proper locking in select for update in transactions.
    /// </summary>
    [Test]
    public void MonoTestTransactionSelectForUpdate()
    {
      DbConnection conn = OpenNewConnection();
      DbCommand cmd = GetCommandForUpdate(conn);

      Log("  Creating table s.MONO_DB2_TEST...");
      cmd.CommandText = "CREATE TABLE s.MONO_DB2_TEST (varchar2_value " +
        "varchar(32), long_value long varchar, number_whole_value numeric(18), " +
        "number_scaled_value numeric(18,2), number_integer_value int, " +
        "float_value float, char_value char, " +
        "varchar2_null_value varchar(32), number_whole_null_value int, " +
        "number_scaled_null_value int, number_integer_null_value int, " +
        "float_null_value float, date_null_value date, char_null_value " +
        "char, primary key (varchar2_value))";
      cmd.ExecuteNonQuery();

      Log("Begin Trans for table s.MONO_DB2_TEST...");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

      Log("  Inserting value into s.MONO_DB2_TEST...");
      cmd.Transaction = trans;

      // should pass now with blanks that can be truncated
      cmd.CommandText = "INSERT INTO s.MONO_DB2_TEST (varchar2_value, " +
        "long_value, number_whole_value, number_scaled_value," +
        "number_integer_value, float_value, char_value) VALUES(" +
        "'Mono', 'This is a LONG column', 123, 456.78, 8765, 235.2, 'U ')";
      cmd.ExecuteNonQuery();
      trans.Commit();

      trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
      cmd.CommandText = "SELECT * from s.MONO_DB2_TEST"
        + " where varchar2_value='Mono' FOR UPDATE OF char_value,"
        + " number_whole_value";
      DbDataReader reader = cmd.ExecuteReader();

      // now check conflict with another connection trying to update
      DbConnection conn2 = OpenNewConnection();
      DbCommand cmd2 = conn2.CreateCommand();
      DbTransaction trans2 = conn2.BeginTransaction();

      cmd2.CommandText = "UPDATE s.MONO_DB2_TEST set char_value = 'C'";
      try {
        cmd2.ExecuteNonQuery();
        Assert.Fail("#A0: expected a conflict exception");
      } catch (GFXDException gfxde) {
        if (!"X0Z02".Equals(gfxde.State)) {
          throw gfxde;
        }
      }

      // no conflict after commit of first transaction
      trans.Commit();

      Assert.AreEqual(1, cmd2.ExecuteNonQuery(), "#A1");
      trans2.Commit();

      reader = cmd.ExecuteReader();
      Assert.IsTrue(reader.Read(), "#A2");
      Assert.AreEqual("C", reader.GetString(6), "#A3");
      Assert.IsFalse(reader.Read(), "#A4");

      // now check for successful update from the same transaction
      trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
      reader = cmd.ExecuteReader();
      trans2 = conn2.BeginTransaction();
      try {
        cmd2.ExecuteNonQuery();
        Assert.Fail("expected a conflict exception");
      } catch (GFXDException gfxde) {
        if (!"X0Z02".Equals(gfxde.State)) {
          throw gfxde;
        }
      }
      cmd.CommandText = "UPDATE s.MONO_DB2_TEST set number_whole_value = 246";
      Assert.AreEqual(1, cmd.ExecuteNonQuery(), "#U1");
      cmd.CommandText = "SELECT * from s.MONO_DB2_TEST";
      reader = cmd.ExecuteReader();
      Assert.IsTrue(reader.Read(), "#A5");
      Assert.AreEqual("C", reader.GetString(6), "#A6");
      Assert.AreEqual(246, reader.GetInt32(2), "#A7");
      Assert.IsFalse(reader.Read(), "#A8");

      cmd2.CommandText = "SELECT * from s.MONO_DB2_TEST";
      DbDataReader reader2 = cmd2.ExecuteReader();
      Assert.IsTrue(reader2.Read(), "#A9");
      Assert.AreEqual("C", reader2.GetString(6), "#A10");
      Assert.AreEqual(123, reader2.GetInt32(2), "#A11");
      Assert.IsFalse(reader2.Read(), "#A12");

      trans.Commit();
      reader = cmd.ExecuteReader();
      Assert.IsTrue(reader.Read(), "#A5");
      Assert.AreEqual("C", reader.GetString(6), "#A6");
      Assert.AreEqual(246, reader.GetInt32(2), "#A7");
      Assert.IsFalse(reader.Read(), "#A8");

      reader2 = cmd2.ExecuteReader();
      Assert.IsTrue(reader2.Read(), "#A9");
      Assert.AreEqual("C", reader2.GetString(6), "#A10");
      Assert.AreEqual(246, reader2.GetInt32(2), "#A11");
      Assert.IsFalse(reader2.Read(), "#A12");
      trans2.Commit();
    }

    [Test]
    public void DataAdapterTest()
    {
      DbConnection conn = OpenNewConnection();
      Log("  Creating select command...");
      DbCommand command = conn.CreateCommand();
      command.CommandText = "SELECT * FROM SYS.SYSTABLES";

      Log("  Creating data adapter...");
      DbDataAdapter adapter = GetDataAdapter();
      adapter.SelectCommand = command;
      Log("  DataAdapter created!");

      Log("  Creating DataSet...");
      DataSet dataSet = new DataSet("EMP");
      Log("  DataSet created!");

      Log("  Filling DataSet via data adapter...");
      adapter.Fill(dataSet);
      Log("  DataSet filled successfully!");

      Log("  Getting DataTable...");
      DataTable table = dataSet.Tables[0];
      Log("  DataTable Obtained!");

      Log("  Displaying each row...");
      int rowCount = 0;
      int sysTablesCount = 0;
      foreach (DataRow row in table.Rows) {
        Log("    row {0}", rowCount + 1);
        for (int i = 0; i < table.Columns.Count; i += 1) {
          Log(" {0}: {1}", table.Columns[i].ColumnName, row[i]);
        }
        Log("");
        if ("SYSTABLES".Equals(row["TABLENAME"])) {
          ++sysTablesCount;
        }
        ++rowCount;
      }
      Assert.AreEqual(1, sysTablesCount,
        "failed to find SYSTABLES table itself in SYS.SYSTABLES");
      Assert.Greater(rowCount, 5, "expected at least 5 rows in SYSTABLES");
    }

    [Test]
    public void DataAdapterTest2()
    {
      DbConnection conn = OpenNewConnection();
      DataAdapterTest2_Setup(conn);
      ReadSimpleTest(conn, "SELECT * FROM mono_adapter_test");

      GetMetaData(conn, "SELECT * FROM mono_adapter_test");

      DataAdapterTest2_Insert(conn);
      ReadSimpleTest(conn, "SELECT * FROM mono_adapter_test");

      DataAdapterTest2_Update(conn);
      ReadSimpleTest(conn, "SELECT * FROM mono_adapter_test");

      DataAdapterTest2_Delete(conn);
      ReadSimpleTest(conn, "SELECT * FROM mono_adapter_test");
    }

    #endregion

    #region Helper methods

    protected object ReadScalar(DbConnection con, string selectSql)
    {
      DbCommand cmd = con.CreateCommand();
      cmd.CommandText = selectSql;

      object o = cmd.ExecuteScalar();
      string dataType = (o != null ? o.GetType().ToString() : "null");
      Log("       DataType: " + dataType);
      return o;
    }

    protected string ReadScalarString(DbConnection con, string selectSql)
    {
      DbCommand cmd = con.CreateCommand();
      cmd.CommandText = selectSql;

      string s = null;
      DbDataReader reader = cmd.ExecuteReader();
      try {
        if (reader.Read()) {
          s = reader.GetString(0);
        }
      } finally {
        reader.Close();
      }
      return s;
    }

    protected byte[] ReadScalarBytes(DbConnection con, string selectSql)
    {
      DbCommand cmd = con.CreateCommand();
      cmd.CommandText = selectSql;

      byte[] bytes = null;
      DbDataReader reader = cmd.ExecuteReader();
      try {
        if (reader.Read()) {
          MemoryStream ms = new MemoryStream();
          int bufSize = 1024;
          long offset = 0;
          long bytesRead;
          bytes = new byte[bufSize];
          while ((bytesRead = reader.GetBytes(0, offset, bytes,
                                              0, bufSize)) > 0) {
            ms.Write(bytes, 0, (int)bytesRead);
            offset += bytesRead;
          }
          bytes = ms.ToArray();
        }
      } finally {
        reader.Close();
      }
      return bytes;
    }

    protected void ReadSimpleTest(DbConnection con, string selectSql)
    {
      DbCommand cmd = con.CreateCommand();
      cmd = con.CreateCommand();
      cmd.CommandText = selectSql;
      DbDataReader reader = cmd.ExecuteReader();

      Log("  Results...");
      Log("    Schema");
      DataTable table;
      table = reader.GetSchemaTable();
      for (int c = 0; c < reader.FieldCount; c++) {
        Log("  Column " + c.ToString());
        DataRow row = table.Rows[c];

        string strColumnName = row["ColumnName"].ToString();
        string strBaseColumnName = row["BaseColumnName"].ToString();
        string strColumnSize = row["ColumnSize"].ToString();
        string strNumericScale = row["NumericScale"].ToString();
        string strNumericPrecision = row["NumericPrecision"].ToString();
        string strDataType = row["DataType"].ToString();

        Log("      ColumnName: " + strColumnName);
        Log("      BaseColumnName: " + strBaseColumnName);
        Log("      ColumnSize: " + strColumnSize);
        Log("      NumericScale: " + strNumericScale);
        Log("      NumericPrecision: " + strNumericPrecision);
        Log("      DataType: " + strDataType);
      }

      int r = 0;
      Log("    Data");
      while (reader.Read()) {
        r++;
        Log("       Row: " + r.ToString());
        for (int f = 0; f < reader.FieldCount; f++) {
          string sname = "";
          object ovalue = "";
          string svalue = "";
          string sDataType = "";
          string sFieldType = "";
          string sDataTypeName = "";

          sname = reader.GetName(f);

          if (reader.IsDBNull(f)) {
            ovalue = DBNull.Value;
            svalue = "";
            sDataType = "DBNull.Value";
          }
          else {
            sDataType = ovalue.GetType().ToString();
            switch (sDataType) {
              case "System.Byte[]":
                ovalue = reader.GetValue(f);
                ovalue = GetHexString((byte[])ovalue);
                break;
              default:
                ovalue = reader.GetValue(f);
                break;
            }
          }
          sFieldType = reader.GetFieldType(f).ToString();
          sDataTypeName = reader.GetDataTypeName(f);

          Log("           Field: " + f.ToString());
          Log("               Name: " + sname);
          Log("               Value: " + svalue);
          Log("               Data Type: " + sDataType);
          Log("               Field Type: " + sFieldType);
          Log("               Data Type Name: " + sDataTypeName);
        }
      }
      if (r == 0) {
        Log("  No data returned.");
      }
    }

    // use this function to read a byte array into a string
    // for easy display of binary data, such as, a BLOB value
    public static string GetHexString(byte[] bytes)
    {
      string bvalue = "";
      StringBuilder sb2 = new StringBuilder();
      for (int z = 0; z < bytes.Length; z++) {
        byte byt = bytes[z];
        if (byt < 0x10) {
          sb2.Append("0");
        }
        sb2.Append(byt.ToString("x"));
      }
      if (sb2.Length > 0) {
        bvalue = "0x" + sb2.ToString();
      }
      return bvalue;
    }

    public void GetMetaData(DbConnection conn, string sql)
    {
      DbCommand cmd = conn.CreateCommand();
      cmd.CommandText = sql;

      Log("Read Schema With KeyInfo");
      DbDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo |
                                           CommandBehavior.SchemaOnly);
      DataTable dt;
      dt = rdr.GetSchemaTable();
      foreach (DataRow schemaRow in dt.Rows) {
        foreach (DataColumn schemaCol in dt.Columns) {
          Log(schemaCol.ColumnName + " = " + schemaRow[schemaCol]);
          Log("---Type: " + schemaRow[schemaCol].GetType().ToString());
        }
        Log("");
      }
      Log("Read Schema with No KeyInfo");

      rdr = cmd.ExecuteReader();
      dt = rdr.GetSchemaTable();
      foreach (DataRow schemaRow in dt.Rows) {
        foreach (DataColumn schemaCol in dt.Columns) {
          Log(schemaCol.ColumnName + " = " + schemaRow[schemaCol]);
          Log("---Type: " + schemaRow[schemaCol].GetType().ToString());
          Log("");
        }
      }
      rdr.Close();
    }

    public void DataAdapterTest2_Insert(DbConnection conn)
    {
      Log("================================");
      Log("=== Adapter Insert =============");
      Log("================================");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.Chaos);

      Log("   Create adapter...");
      DbCommand selectCmd = conn.CreateCommand();
      selectCmd.Transaction = trans;
      selectCmd.CommandText = "SELECT * FROM mono_adapter_test";
      DbDataAdapter da = GetDataAdapter();
      da.SelectCommand = (DbCommand)selectCmd;

      // CommandBuilder to perform the insert
      DbCommandBuilder cmdBuilder = GetCommandBuilder(da);

      Log("   Create data set ...");
      DataSet ds = new DataSet();

      Log("   Fill data set via adapter...");
      da.Fill(ds, "mono_adapter_test");

      Log("   New Row...");
      DataRow myRow;
      myRow = ds.Tables["mono_adapter_test"].NewRow();

      byte[] bytes = new byte[] { 0x45,0x46,0x47,0x48,0x49,0x50 };

      Log("   Set values in the new DataRow...");
      myRow["varchar2_value"] = "OracleClient";
      myRow["number_whole_value"] = 22;
      myRow["number_scaled_value"] = 12.34;
      myRow["number_integer_value"] = 456;
      myRow["float_value"] = 98.76;
      myRow["date_value"] = new DateTime(2001, 07, 09);
      myRow["char_value"] = "Romeo";
      myRow["clob_value"] = "clobtest";
      myRow["blob_value"] = bytes;

      Log("    Add DataRow to DataTable...");
      ds.Tables["mono_adapter_test"].Rows.Add(myRow);

      Log("da.Update(ds...");
      da.Update(ds, "mono_adapter_test");

      trans.Commit();

      cmdBuilder.Dispose();
    }

    public void DataAdapterTest2_Update(DbConnection conn)
    {
      Log("================================");
      Log("=== Adapter Update =============");
      Log("================================");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.Chaos);

      Log("   Create adapter...");
      DbCommand selectCmd = GetCommandForUpdate(conn);
      selectCmd.Transaction = trans;
      selectCmd.CommandText = "SELECT * FROM mono_adapter_test";
      DbDataAdapter da = GetDataAdapter();
      da.SelectCommand = selectCmd;

      // CommandBuilder to perform the update
      DbCommandBuilder cmdBuilder = GetCommandBuilder(da);

      Log("   Create data set ...");
      DataSet ds = new DataSet();

      Log("  Fill data set via adapter...");
      da.Fill(ds, "mono_adapter_test");

      Log("Tables Count: " + ds.Tables.Count.ToString());

      DataTable table = ds.Tables["mono_adapter_test"];
      DataRowCollection rows;
      rows = table.Rows;
      Log("   Row Count: " + rows.Count.ToString());
      DataRow myRow = rows[0];

      byte[] bytes = new byte[] { 0x62, 0x63, 0x64, 0x65, 0x66, 0x67 };

      Log("   Set values in the new DataRow...");

      myRow["varchar2_value"] = "Super Power!";

      myRow["number_scaled_value"] = 12.35;
      myRow["number_integer_value"] = 457;
      myRow["float_value"] = 198.76;
      myRow["date_value"] = new DateTime(2002, 08, 09);
      myRow["char_value"] = "Juliet";
      myRow["clob_value"] = "this is a clob";
      myRow["blob_value"] = bytes;

      Log("da.Update(ds...");
      da.Update(ds, "mono_adapter_test");

      trans.Commit();

      cmdBuilder.Dispose();
    }

    public void DataAdapterTest2_Delete(DbConnection conn)
    {
      Log("================================");
      Log("=== Adapter Delete =============");
      Log("================================");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.Chaos);

      Log("   Create adapter...");
      DbCommand selectCmd = conn.CreateCommand();
      selectCmd.Transaction = trans;
      selectCmd.CommandText = "SELECT * FROM mono_adapter_test";
      DbDataAdapter da = GetDataAdapter();
      da.SelectCommand = (DbCommand)selectCmd;

      // CommandBuilder to perform the delete
      DbCommandBuilder cmdBuilder = GetCommandBuilder(da);

      Log("   Create data set ...");
      DataSet ds = new DataSet();

      Log("Fill data set via adapter...");
      da.Fill(ds, "mono_adapter_test");

      Log("delete row...");
      ds.Tables["mono_adapter_test"].Rows[0].Delete();

      Log("da.Update(table...");
      da.Update(ds, "mono_adapter_test");

      Log("Commit...");
      trans.Commit();

      cmdBuilder.Dispose();
    }

    readonly byte[] blob_bytes = new byte[6] { 0x31, 0x32, 0x33,
      0x34, 0x35, 0x36 };

    protected void UpdateClobBlob(DbConnection conn, string tableName,
                                  string varchar2_value)
    {
      DbCommand cmd = GetCommandForUpdate(conn);

      Log("  Begin Trans for table " + tableName + "...");
      DbTransaction trans = conn.BeginTransaction(IsolationLevel.Chaos);

      Log("  Select/Update CLOB columns on table " + tableName + "...");

      cmd.CommandText = "SELECT CLOB_VALUE, BLOB_VALUE FROM " + tableName +
        " WHERE varchar2_value='" + varchar2_value + "' FOR UPDATE";
      DbDataReader reader = cmd.ExecuteReader();
      if (!reader.Read()) {
         throw new Exception("ERROR: RECORD NOT FOUND");
      }

      // update clob_value
      Log("     Update CLOB column on table " + tableName + "...");
      char[] chars = "Mono is not so fun...".ToCharArray();
      //UpdateValue(reader, 0, chars);
      cmd.CommandText = "UPDATE " + tableName + " SET CLOB_VALUE = ? WHERE"
        + " varchar2_value='" + varchar2_value + "'";
      cmd.Parameters.Add(chars);
      Assert.AreEqual(1, cmd.ExecuteNonQuery(), "expected one row update");

      // update blob_value
      Log("     Update BLOB column on table " + tableName + "...");
      byte[] bytes = new byte[6] { 0x31, 0x32, 0x33, 0x34, 0x35, 0x036 };
      //UpdateValue(reader, 1, bytes);
      cmd.CommandText = "UPDATE " + tableName + " SET BLOB_VALUE = ? WHERE"
        + " varchar2_value='" + varchar2_value + "'";
      cmd.Parameters.Clear();
      cmd.Parameters.Add(bytes);
      Assert.AreEqual(1, cmd.ExecuteNonQuery(), "expected one row update");

      //Log("  Update row and commit trans for table " + tableName + "...");
      //UpdateRow(reader);
      trans.Commit();

      cmd.CommandText = "UPDATE " + tableName + " SET CLOB_VALUE = ?, " +
        " BLOB_VALUE = ? WHERE varchar2_value = ?";
      cmd.Parameters.Clear();

      // update clob_value
      Log("     Update CLOB column on table " + tableName + "...");
      chars = "Mono is fun!".ToCharArray();
      DbParameter param = cmd.CreateParameter();
      param.Value = chars;
      cmd.Parameters.Add(param);

      // update blob_value
      Log("     Update BLOB column on table " + tableName + "...");
      cmd.Parameters.Add(blob_bytes);

      // varchar2_value
      cmd.Parameters.Add(varchar2_value);

      Log("  Execute and commit trans for table " + tableName + "...");
      Assert.AreEqual(1, cmd.ExecuteNonQuery(), "expected one row update");
      trans.Commit();
    }

    public void VerifyCommonColumns(DbConnection conn, string tableName,
                                    decimal expected_dec_value,
                                    DateTime expectedDateTime, bool checkLobs)
    {
      // DbCommand.ExecuteScalar
      Log(" -ExecuteScalar tests...");
      string varchar2_value = (string)ReadScalar(conn,
        "SELECT MAX(varchar2_value) as maxVar2 FROM " + tableName);
      Assert.AreEqual("Mono", varchar2_value);

      Log("  Read Scalar: number_whole_value");
      decimal number_whole_value = (decimal)ReadScalar(conn,
        "SELECT number_whole_value as w FROM " + tableName);
      Log("     Int32 Value: " + number_whole_value.ToString());
      Assert.AreEqual(expected_dec_value, number_whole_value);

      Log("  Read Scalar: number_scaled_value");
      decimal number_scaled_value = (decimal)ReadScalar(conn,
        "SELECT MAX(number_scaled_value) FROM " + tableName);
      expected_dec_value = 456.78m;
      Log("     Decimal Value: " + number_scaled_value.ToString());
      Assert.AreEqual(expected_dec_value, number_scaled_value);

      if (expectedDateTime != DateTime.MinValue) {
        Log("  Read Scalar: date_value");
        DateTime date_value = (DateTime)ReadScalar(conn,
          "SELECT date_value FROM " + tableName);
        Log("     DateTime Value: " + date_value.ToString());
        if (expectedDateTime.Kind == DateTimeKind.Utc) {
          expectedDateTime = expectedDateTime.ToLocalTime();
        }
        Assert.AreEqual(expectedDateTime, date_value);
      }

      if (!checkLobs) {
        return;
      }
      Log("  Read Scalar: clob_value");
      string clob_value = ReadScalarString(conn,
        "SELECT clob_value FROM " + tableName);
      Log("     CLOB Value: " + clob_value);
      Assert.AreEqual("Mono is fun!", clob_value);

      Log("  Read Scalar: blob_value");
      byte[] blob_value = ReadScalarBytes(conn,
        "SELECT blob_value FROM " + tableName);
      string expected_sblob = GetHexString(blob_bytes);
      string sblob_value = GetHexString(blob_value);
      Log("     BLOB Value: " + sblob_value);
      Assert.AreEqual(expected_sblob, sblob_value);
    }

    #endregion

    #region setup and teardown methods

    [SetUp, TearDown]
    public void DropTables()
    {
      DropTables(OpenNewConnection());
    }

    public virtual void DropTables(DbConnection conn)
    {
      string[] allTables = new string[] { "s.MONO_DB2_TEST",
        "mono_adapter_test" };
      foreach (string table in allTables) {
        Log("  Dropping table " + table + "...");
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE " + table;
        try {
          cmd.ExecuteNonQuery();
          Log("  Dropped table " + table + "...");
        } catch (DbException ex) {
          // ignore if table already exists
          Log("Got exception: " + ex.ToString());
        }
      }
    }

    protected void DataAdapterTest2_Setup(DbConnection conn)
    {
      Log("  Creating table mono_adapter_test...");

      DbCommand cmd = GetCommandForUpdate(conn);
      cmd.CommandText = "CREATE TABLE mono_adapter_test ( " +
        " varchar2_value VarChar(32),  " +
        " number_whole_value Numeric(18) PRIMARY KEY, " +
        " number_scaled_value Numeric(18,2), " +
        " number_integer_value Int, " +
        " float_value Float, " +
        " date_value Timestamp, " +
        " char_value Char(32), " +
        " clob_value Clob, " +
        " blob_value Blob)";
      cmd.ExecuteNonQuery();

      // try with timestamp parameter as UTC time
      Log("  Inserting value into mono_adapter_test...");
      cmd.CommandText = "INSERT INTO mono_adapter_test " +
        "(varchar2_value, number_whole_value, number_scaled_value, " +
        " number_integer_value, float_value, date_value, char_value) VALUES(" +
        " ?, ?, ?, ?, ?, ?, ?)";
      cmd.Parameters.Add("Mono");
      cmd.Parameters.Add(123);
      cmd.Parameters.Add(456.78m);
      cmd.Parameters.Add(8765);
      cmd.Parameters.Add(235.2m);
      cmd.Parameters.Add(DateTime.Parse("2004-12-31 14:14:14.123 GMT")
          .ToUniversalTime());
      cmd.Parameters.Add("MonoTest");
      cmd.ExecuteNonQuery();

      UpdateClobBlob(conn, "mono_adapter_test", "Mono");
      // test that the values have been updated as expected
      VerifyCommonColumns(conn, "mono_adapter_test", 123m,
                          DateTime.Parse("2004-12-31 14:14:14.123"), true);

      // try with parameter as unspecified (local) time
      DbCommand cmd2 = GetCommandForUpdate(conn);
      cmd2.CommandText = "DELETE FROM mono_adapter_test where " +
        "varchar2_value='Mono'";
      cmd2.ExecuteNonQuery();

      Log("  Inserting value again into mono_adapter_test...");
      cmd.Parameters.Clear();
      cmd.Parameters.Add("Mono");
      cmd.Parameters.Add(123);
      cmd.Parameters.Add(456.78m);
      cmd.Parameters.Add(8765);
      cmd.Parameters.Add(235.2m);
      cmd.Parameters.Add(DateTime.Parse("2004-12-31 14:14:14.123"));
      cmd.Parameters.Add("MonoTest");
      cmd.ExecuteNonQuery();

      UpdateClobBlob(conn, "mono_adapter_test", "Mono");
      // test that the values have been updated as expected
      VerifyCommonColumns(conn, "mono_adapter_test", 123m,
                          DateTime.Parse("2004-12-31 14:14:14.123"), true);
    }

    #endregion
  }
}
