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
  /// Tests for some of the bugs reported against the ADO.NET driver.
  /// </summary>
  [TestFixture]
  public class BugTests : TestBase
  {
    [Test]
    public void DateTime_43233()
    {
      DbConnection conn = OpenNewConnection();
      DbCommand cmd = conn.CreateCommand();
      cmd.CommandText = "CREATE TABLE datetime_test "
          + "(id INT primary key, "
          + "type_date DATE, "
          + "type_time TIME,"
          + "type_datetime TIMESTAMP)";

      Log(cmd.CommandText);
      cmd.ExecuteNonQuery();

      cmd.CommandText = "INSERT INTO datetime_test VALUES"
          + " (1000, '2009-09-09', '01:01:01', '2009-09-09 01:01:01'),"
          + " (1001, '2009-09-09', '09:09:09', '2009-09-09 09:09:09'),"
          + " (1002, '2010-10-10', '10:10:10', '2010-10-10 10:10:10'),"
          + " (1003, '2011-11-11', '11:11:11', '2011-11-11 11:11:11'),"
          + " (1004, '2012-12-12', '12:12:12', '2012-12-12 12:12:12'),"
          + " (1005, '2011-04-24', '12:12:12', '2011-04-24 12:12:12'),"
          + " (1006, '2011-04-26', '12:12:12', '2011-04-26 12:12:12')";
      Log(cmd.CommandText);
      cmd.ExecuteNonQuery();
      RunDateTimeQueries(cmd);

      // now also try inserting with parameters and check using query again
      cmd.CommandText = "DELETE FROM datetime_test";
      cmd.Parameters.Clear();
      Assert.AreEqual(7, cmd.ExecuteNonQuery());

      DateTime dateTime;
      cmd.CommandText = "INSERT INTO datetime_test VALUES (?, ?, ?, ?)";

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1000);
      dateTime = DateTime.Parse("2009-09-09 01:01:01");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1001);
      dateTime = DateTime.Parse("2009-09-09 09:09:09");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1002);
      dateTime = DateTime.Parse("2010-10-10 10:10:10");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1003);
      dateTime = DateTime.Parse("2011-11-11 11:11:11");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1004);
      dateTime = DateTime.Parse("2011-04-24 12:12:12");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1005);
      dateTime = DateTime.Parse("2012-12-12 12:12:12");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      cmd.Parameters.Clear();
      cmd.Parameters.Add(1006);
      dateTime = DateTime.Parse("2011-04-26 12:12:12");
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      cmd.Parameters.Add(dateTime);
      Assert.AreEqual(1, cmd.ExecuteNonQuery());

      RunDateTimeQueries(cmd);
    }

    [Test]
    public void Bug43995() {
      string ddl = "CREATE TABLE ADDRESS" +
        " (address_id BIGINT NOT NULL, address1 VARCHAR(20) NOT NULL," +
        " address2 VARCHAR(10), address3 VARCHAR(10)," +
        " city VARCHAR(10) NOT NULL, state VARCHAR(10) NOT NULL," +
        " zip_code VARCHAR(5), province VARCHAR(20)," +
        " country_code INT NOT NULL, CONSTRAINT pk_address" +
        " PRIMARY KEY (address_id))";

      DbConnection conn = OpenNewConnection();
      DbCommand cmd = conn.CreateCommand();
      // try with both replicated and partitioned tables
      for(int i = 1; i <= 2; i++) {
        if (i == 1) {
          cmd.CommandText = ddl + " REPLICATE";
        }
        else {
          cmd.CommandText = ddl + " PARTITION BY PRIMARY KEY";
        }
        Log(cmd.CommandText);
        cmd.ExecuteNonQuery();

        // insert some rows
        cmd.CommandText = "insert into address" +
          " (address_id, address1, city, state, country_code)" +
            "values (3, 'address1', 'Pune', 'MH', 91)," +
            "(2, 'address2', 'Agra', 'UP', 91)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT address_id FROM ADDRESS ORDER BY" +
          " address_id DESC FETCH FIRST 1 ROWS ONLY";
        DbDataReader rdr = cmd.ExecuteReader();
        Assert.IsTrue(rdr.Read());
        Assert.AreEqual(3, rdr.GetInt64(0), "#A1");
        Assert.IsFalse(rdr.Read());

        // drop the table
        cmd.CommandText = "drop table ADDRESS";
        cmd.ExecuteNonQuery();
      }
    }

    private void RunDateTimeQueries(DbCommand cmd) {
      cmd.CommandText = "SELECT * FROM datetime_test ORDER BY id ASC";
      cmd.Parameters.Clear();
      ReadData(cmd);

      DateTime expected;
      cmd.CommandText = "SELECT * FROM datetime_test WHERE type_datetime=?";
      cmd.Parameters.Clear();
      DbParameter param = cmd.CreateParameter();

      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2009-09-09 01:01:01");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2009-09-09 09:09:09");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2010-10-10 10:10:10");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-11-11 11:11:11");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-04-24 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-04-26 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2012-12-12 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      ///////////////////////////////////////////////////////////////////////

      cmd.CommandText = "SELECT * FROM datetime_test WHERE type_date=?"
        + " order by type_datetime desc";
      cmd.Parameters.Clear();
      param = cmd.CreateParameter();

      DateTime[] nineNineAll = new DateTime[] {
        DateTime.Parse("2009-09-09 09:09:09"),
        DateTime.Parse("2009-09-09 01:01:01"),
      };

      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2009-09-09 01:01:01");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, nineNineAll);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2009-09-09 09:09:09");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, nineNineAll);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2010-10-10 10:10:10");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-11-11 11:11:11");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-04-24 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2012-12-12 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-04-26 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      ///////////////////////////////////////////////////////////////////////

      cmd.CommandText = "SELECT * FROM datetime_test WHERE type_time=?"
        + " order by type_datetime";

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();

      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2009-09-09 01:01:01");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2009-09-09 09:09:09");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2010-10-10 10:10:10");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = expected = DateTime.Parse("2011-11-11 11:11:11");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, expected);

      DateTime[] twelveTwelveAll = new DateTime[] {
        DateTime.Parse("2011-04-24 12:12:12"),
        DateTime.Parse("2011-04-26 12:12:12"),
        DateTime.Parse("2012-12-12 12:12:12"),
      };

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2011-04-24 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, twelveTwelveAll);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2011-04-26 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, twelveTwelveAll);

      cmd.Parameters.Clear();
      param = cmd.CreateParameter();
      param.DbType = DbType.DateTime;
      param.Value = DateTime.Parse("2012-12-12 12:12:12");
      cmd.Parameters.Add(param);
      VerifyDateTime(cmd, twelveTwelveAll);
    }

    private void ReadData(DbCommand cmd)
    {
      //Log(String.Format("{0}; param = {1}", cmd.CommandText,
      //                  cmd.Parameters[0].ToString()));
      Log(String.Format("{0}", cmd.CommandText));
      DbDataReader reader = cmd.ExecuteReader();

      while (reader.Read()) {
        Log(String.Format("{0}, {1}, {2}, {3}", reader.GetString(0),
                          reader.GetString(1), reader.GetString(2),
                          reader.GetString(3)));
      }

      Log("===============================================================");

      reader.Close();
    }

    private void VerifyDateTime(DbCommand cmd, DateTime expected)
    {
      DbDataReader reader = cmd.ExecuteReader();
      Assert.IsTrue(reader.Read());
      Log(String.Format("{0}, {1}, {2}, {3}", reader.GetString(0),
                        reader.GetString(1), reader.GetString(2),
                        reader.GetString(3)));
      Assert.AreEqual(expected.Date, reader.GetDateTime(1).Date);
      Assert.AreEqual(expected.TimeOfDay, reader.GetDateTime(2).TimeOfDay);
      Assert.AreEqual(expected, reader.GetDateTime(3));
      Assert.IsFalse(reader.Read());
      reader.Close();
    }

    private void VerifyDateTime(DbCommand cmd, DateTime[] allExpected)
    {
      DbDataReader reader = cmd.ExecuteReader();
      foreach (DateTime expected in allExpected)
      {
        Assert.IsTrue(reader.Read());
        Log(String.Format("{0}, {1}, {2}, {3}", reader.GetString(0),
                          reader.GetString(1), reader.GetString(2),
                          reader.GetString(3)));
        Assert.AreEqual(expected.Date, reader.GetDateTime(1).Date);
        Assert.AreEqual(expected.TimeOfDay, reader.GetDateTime(2).TimeOfDay);
        Assert.AreEqual(expected, reader.GetDateTime(3));
      }
      Assert.IsFalse(reader.Read());
      reader.Close();
    }

    #region setup and teardown methods

    [SetUp, TearDown]
    public void DropTables()
    {
      DropTables(OpenNewConnection());
    }

    public virtual void DropTables(DbConnection conn)
    {
      string[] allTables = new string[] { "DATETIME_TEST", "ADDRESS" };
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

    #endregion
  }
}
