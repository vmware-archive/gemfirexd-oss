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
package com.pivotal.gemfirexd.internal.engine.distributed.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.derbyTesting.junit.JDBC;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

public class QueryChecksTest extends JdbcTestBase {

  public QueryChecksTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(QueryChecksTest.class));
  }

  // bug test #43314
  public void testSimpleAvg() throws Exception {

    {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      final String derbyConn = "jdbc:derby:newDerbySecDB;";
      String sysConnUrl = derbyConn + "create=true;" + "user=" + bootUserName
          + ";password=" + bootUserPassword;

      Connection derby = DriverManager.getConnection(sysConnUrl);
      derby.setAutoCommit(true);

      if (GemFireXDUtils.hasTable(derby, "test_t"))
        derby.createStatement().execute("drop table test_t");

      derby.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      derby.createStatement().execute(
          "insert into test_t values ( 1, 2302), ( 2, 4690), ( 3, 4901)");

      ResultSet dr = derby.createStatement().executeQuery(
          "select avg(cast(col_ints as real)) from test_t ");

      assertTrue(dr.next());

      assertEquals(3964.3333f, dr.getObject(1));
      derby.commit();
    }

    {

      Connection c = TestUtil.getConnection();

      c.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      c.createStatement().execute(
          "insert into test_t values ( 1, 2302), ( 2, 4690), ( 3, 4901)");

      ResultSet r = c.createStatement().executeQuery(
          "select avg(cast(col_ints as real)) from test_t ");

      assertTrue(r.next());

      assertEquals(Float.valueOf(3964.3333f), r.getFloat(1));
    }
  }
  
  // bug test #43316
  public void testOverflowAvg() throws Exception {

    float derbyVal = 0;
    {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      final String derbyConn = "jdbc:derby:newDerbySecDB;";
      String sysConnUrl = derbyConn + "create=true;" + "user=" + bootUserName
          + ";password=" + bootUserPassword;

      Connection derby = DriverManager.getConnection(sysConnUrl);
      derby.setAutoCommit(true);

      if (GemFireXDUtils.hasTable(derby, "test_t"))
        derby.createStatement().execute("drop table test_t");

      derby.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      derby.createStatement().execute(
      "insert into test_t values ( 1, 2147483647), ( 2, 2147483646), (3, 2147483645)");

      ResultSet r = derby.createStatement().executeQuery(
          "select avg(cast(col_ints as float)) from test_t ");

      assertTrue(r.next());
      derbyVal = r.getFloat(1);
      assertFalse(r.next());
      derby.commit();
    }

    {

      Connection c = TestUtil.getConnection();

      c.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      c.createStatement().execute(
          "insert into test_t values ( 1, 2147483647), ( 2, 2147483646), (3, 2147483645)");

      ResultSet r = c.createStatement().executeQuery(
          "select avg(col_ints) from test_t ");
      assertTrue(r.next());
      assertEquals(2147483646l, r.getObject(1));
      assertFalse(r.next());

      r = c.createStatement().executeQuery(
          "select sum(col_ints) from test_t ");
      try {
        assertTrue(r.next());
        fail("expected overflow exception");
      } catch (SQLException sqle) {
        if (!"22003".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      r = c.createStatement().executeQuery(
          "select avg(cast(col_ints as float)) from test_t ");

      assertTrue(r.next());
      assertEquals(derbyVal, r.getFloat(1));
      assertFalse(r.next());
    }
  }

  // bug test #43316
  public void testTypePromotionOfSum() throws Exception {

    float derbySum = 0;
    {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      final String derbyConn = "jdbc:derby:newDerbySecDB;";
      String sysConnUrl = derbyConn + "create=true;" + "user=" + bootUserName
          + ";password=" + bootUserPassword;

      Connection derby = DriverManager.getConnection(sysConnUrl);
      derby.setAutoCommit(true);

      if (GemFireXDUtils.hasTable(derby, "test_t"))
        derby.createStatement().execute("drop table test_t");

      derby.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      derby.createStatement().execute(
          "insert into test_t values ( 1, 2147483647), ( 2, 2147483647)");

       ResultSet r = derby.createStatement().executeQuery(
       "select sum(cast(col_ints as real)) from test_t ");
      
       assertTrue(r.next());
       derbySum = r.getFloat(1);
       assertFalse(r.next());
       derby.commit();
    }

    {
      Connection c = TestUtil.getConnection();

      c.createStatement().execute(
          "create table test_t ( pk_col int primary key, col_ints int ) ");

      c.createStatement().execute(
          "insert into test_t values ( 1, 2147483647), ( 2, 2147483647)");

      ResultSet r = c.createStatement().executeQuery(
          "select sum(col_ints) from test_t ");

      try {
        assertTrue(r.next());
        fail("expected overflow exception");
      } catch (SQLException sqle) {
        if (!"22003".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
      //assertTrue(r.next());
      //System.out.println("OK " + r.getObject(1).getClass().getName());
      //assertEquals(4294967294l, r.getObject(1));
      //assertFalse(r.next());

      r = c.createStatement().executeQuery(
      "select sum(cast(col_ints as real)) from test_t ");

      assertTrue(r.next());
      assertEquals(derbySum, r.getFloat(1));
      assertFalse(r.next());
    }
  }

  public void testBug42889() throws Exception {
    setupConnection();
    final Connection conn = jdbcConn; //startNetserverAndGetLocalNetConnection();
    Statement stmt = conn.createStatement();

    /* Create test tables as a fixture for this test.  Note
     * that we're implicitly testing the creation of XML columns
     * as part of this setup.  All of the following should
     * succeed; see testXMLColCreation() for some tests where
     * column creation is expected to fail.
     */

    stmt.executeUpdate("create table t1 (i int, x xml)");
    stmt.executeUpdate("create table t2 (x2 xml not null)");
    stmt.executeUpdate("alter table t2 add column x1 xml");

    /* Insert test data.  Here we're implicitly tesing
     * the XMLPARSE operator in situations where it should
     * succeed.  Negative test cases are tesed in the
     * testIllegalNullInserts() and testXMLParse() methods
     * of the XMLTypeAndOps class.
     */

    // Null values.

    assertUpdateCount(stmt, 1, "insert into t1 values (1, null)");
    assertUpdateCount(stmt, 1, "insert into t1 values (2, cast (null as xml))");

    assertUpdateCount(stmt, 1, "insert into t1 (i) values (4)");
    assertUpdateCount(stmt, 1, "insert into t1 values (3, default)");

    // Non-null values.

    assertUpdateCount(stmt, 1, "insert into t1 values (5, xmlparse(document "
        + "'<hmm/>' preserve whitespace))");

    assertUpdateCount(stmt, 1, " insert into t1 values (6, xmlparse(document "
        + "'<half> <masted> bass </masted> boosted. </half>' "
        + "preserve whitespace))");

    assertUpdateCount(stmt, 1, " insert into t2 (x1, x2) values (null, "
        + "xmlparse(document '<notnull/>' preserve whitespace))");

    assertUpdateCount(stmt, 1,
        " insert into t1 values (7, xmlparse(document '<?xml "
            + "version=\"1.0\" encoding= \"UTF-8\"?><umm> decl "
            + "check </umm>' preserve whitespace))");

    assertUpdateCount(stmt, 1,
        "insert into t1 values (8, xmlparse(document '<lets> "
            + "<try> this out </try> </lets>' preserve whitespace))");

    assertUpdateCount(stmt, 1,
        " update t1 set x = xmlparse(document '<update> "
            + "document was inserted as part of an UPDATE "
            + "</update>' preserve whitespace) where i = 1");

    assertUpdateCount(stmt, 1,
        " update t1 set x = xmlparse(document '<update2> "
            + "document was inserted as part of an UPDATE "
            + "</update2>' preserve whitespace) where "
            + "xmlexists('/update' passing by ref x)");

    ResultSet rs = stmt
        .executeQuery("select xmlserialize(x as char(100)) from t1");

    String[] expColNames = new String[] { "1" };
    JDBC.assertColumnNames(rs, expColNames);

    String[][] expRS = new String [][] {
        {"<update2> document was inserted as part of an "
            + "UPDATE </update2>"},
        {null},
        {null},
        {null},
        {"<hmm/>"},
        {"<half> <masted> bass </masted> boosted. </half>"},
        {"<umm> decl check </umm>"},
        {"<lets> <try> this out </try> </lets>"}
    };

    JDBC.assertUnorderedResultSet(rs, expRS, true);

    // XMLEXISTS should return null if the operand is null.

    rs = stmt
        .executeQuery("select xmlexists('//lets' passing by ref x) from t1");

    expColNames = new String[] { "1" };
    JDBC.assertColumnNames(rs, expColNames);

    expRS = new String [][]
    {
        {"false"},
        {null},
        {null},
        {null},
        {"false"},
        {"false"},
        {"false"},
        {"true"}
    };

    JDBC.assertUnorderedResultSet(rs, expRS, true, !conn.getClass().getName()
        .contains("EmbedConnection"));

    stmt.executeUpdate("create table xqUpdate (i int, x xml default null)");
    assertUpdateCount(stmt, 2, "insert into xqUpdate (i) values 29, 30");
    assertUpdateCount(stmt, 1,
        "insert into xqUpdate values ("
        + "  9,"
        + "  xmlparse(document '<here><is><my "
        + "height=\"4.4\">attribute</my></is></here>' preserve "
        + "whitespace)"
        + ")");

    // These updates should succeed.

    assertUpdateCount(stmt, 1,
        "update xqUpdate"
        + "  set x = "
        + "    xmlquery('.' passing by ref"
        + "      xmlparse(document '<none><here/></none>' "
        + "preserve whitespace)"
        + "    returning sequence empty on empty)"
        + "where i = 29");

    assertUpdateCount(stmt, 1,
        " update xqUpdate"
        + "  set x = "
        + "    xmlquery('self::node()[//@height]' passing by ref"
        + "      (select"
        + "        xmlquery('.' passing by ref x empty on empty)"
        + "        from xqUpdate"
        + "        where i = 9"
        + "      )"
        + "    empty on empty)"
        + "where i = 30");

    stmt.close();
    conn.close();
  }

  public void testCaseInsensitiveSearch() throws Exception {
    checkCaseInsensitiveSearch(getConnection(), true, true);
  }

  public static void checkCaseInsensitiveSearch(Connection conn,
      boolean checkScanTypes, boolean dropTable) throws SQLException {
    final Statement stmt = conn.createStatement();

    // create a table
    stmt.execute("CREATE TABLE T.TABLE_DATA ("
        + "   ID VARCHAR (36) NOT NULL,"
        + "   F1 VARCHAR (100),"
        + "   F2 VARCHAR (100),"
        + "   F3 VARCHAR (100),"
        + "   F4 VARCHAR (100),"
        + "   F5 VARCHAR (100),"
        + "   F6 TIMESTAMP,"
        + "   F7 DECIMAL (16,2),"
        + "   F8 VARCHAR (20),"
        + "   F9 VARCHAR (100),"
        + "   F10 VARCHAR (100),"
        + "   F11 VARCHAR (100),"
        + "   F12 VARCHAR (100) WITH DEFAULT 'UNKNOWN',"
        + "   F13 VARCHAR (100),"
        + "   F14 VARCHAR (100),"
        + "   F15 VARCHAR (100),"
        + "   F16 VARCHAR (100),"
        + "   F17 VARCHAR (100),"
        + "   F18 VARCHAR (256),"
        + "   F19 VARCHAR (100),"
        + "   F20 TIMESTAMP,"
        + "   F21 TIMESTAMP,"
        + "   F22 CLOB NOT NULL,"
        + "   F23 SMALLINT NOT NULL,"
        + "   F24 SMALLINT WITH DEFAULT 1,"
        + "   F25 TIMESTAMP,"
        + "   F26 INTEGER,"
        + "   F27 CHAR (1) WITH DEFAULT 'N',"
        + "   F28 CHAR (1) WITH DEFAULT 'N',"
        + "   F29 VARCHAR (36) NOT NULL,"
        + "   F30 INTEGER NOT NULL,"
        + "   F31 VARCHAR (100) WITH DEFAULT 'UNKNOWN',"
        + "   F32 VARCHAR (30) WITH DEFAULT 'UNKNOWN',"
        + "   F34 SMALLINT,"
        + "   F35 TIMESTAMP,"
        + "   F36 TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP,"
        + "   CONSTRAINT DATA_PK PRIMARY KEY(ID)" + ")"
        + "PARTITION BY PRIMARY KEY "
        + "REDUNDANCY 1 "
        + "EVICTION BY LRUHEAPPERCENT EVICTACTION OVERFLOW "
        + "PERSISTENT ASYNCHRONOUS");

    // indexes including a case-insensitive one
    stmt.execute("create index t.if3 on T.TABLE_DATA(F3) "
        + "--gemfirexd-properties caseSensitive=false");
    stmt.execute("create index T.if4 on T.TABLE_DATA(F4)");
    stmt.execute("create index t.if5 on T.TABLE_DATA(F5)");
    stmt.execute("create index t.if6 on T.TABLE_DATA(F6)");
    stmt.execute("create index t.if7 on T.TABLE_DATA(F7)");
    stmt.execute("create index t.if8 on T.TABLE_DATA(F8)");
    stmt.execute("create index t.if3_4_5 on T.TABLE_DATA(F3, F4, F5) "
        + "--gemfirexd-properties caseSensitive=false");

    // insert some data
    StringBuilder sb = new StringBuilder()
        .append("insert into T.TABLE_DATA values (");
    for (int i = 1; i <= 35; i++) {
      sb.append("?,");
    }
    sb.append("?)");
    PreparedStatement ps = conn.prepareStatement(sb.toString());
    for (int row = 1; row <= 100; row++) {
      int r = row >>> 1;
      for (int i = 1; i <= 36; i++) {
        switch (i) {
          case 7:
          case 21:
          case 22:
          case 26:
          case 35:
          case 36:
            ps.setTimestamp(i, new Timestamp(System.currentTimeMillis()));
            break;
          case 4:
          case 5:
            ps.setString(i, "TestValue" + (r * i));
            break;
          case 6:
            ps.setString(i, "TestValue" + (row * i));
            break;
          case 28:
          case 29:
            ps.setString(i, "Y");
            break;
          default:
            ps.setString(i, String.valueOf(row * i));
            break;
        }
      }
      assertEquals(1, ps.executeUpdate());
    }

    caseInsensitiveQueries(conn, checkScanTypes);

    if (dropTable) {
      stmt.execute("drop table T.TABLE_DATA");
    }

    conn.close();
  }

  public static void caseInsensitiveQueries(Connection conn,
      boolean checkScanTypes) throws SQLException {

    // Set the observer to check that proper scans are being opened
    ScanTypeQueryObserver observer = new ScanTypeQueryObserver();

    // multiple index choices so need an explicit hint
    PreparedStatement ps = conn.prepareStatement("SELECT ID FROM T.TABLE_DATA "
        + "--gemfirexd-properties index=IF3_4_5 \n"
        + "where F3=? AND F4=? AND F5=?");
    ps.setString(1, "testvalue" + 100);
    ps.setString(2, "TestValue" + 125);
    ps.setString(3, "TestValue" + 300);

    GemFireXDQueryObserverHolder.setInstance(observer);
    ResultSet rs = ps.executeQuery();
    assertTrue(rs.next());
    assertEquals("50", rs.getString(1));
    assertFalse(rs.next());
    rs.close();

    // Check the scan types opened
    if (checkScanTypes) {
      observer.addExpectedScanType("t.table_data", "t.if3_4_5",
          ScanType.SORTEDMAPINDEX);
      observer.checkAndClear();
    }

    // multiple index choices so need an explicit hint
    ps = conn.prepareStatement("SELECT ID FROM T.TABLE_DATA "
        + "--gemfirexd-properties index=IF3_4_5 \n"
        + "where F3=? AND F4=?");
    ps.setString(1, "testvalue" + 100);
    ps.setString(2, "TestValue" + 125);

    GemFireXDQueryObserverHolder.setInstance(observer);
    rs = ps.executeQuery();
    assertTrue(rs.next());
    boolean first = false;
    if (rs.getInt(1) == 50) {
      first = true;
      assertEquals("50", rs.getString(1));
    }
    else {
      assertEquals("51", rs.getString(1));
    }
    assertTrue(rs.next());
    if (first) {
      assertEquals("51", rs.getString(1));
    }
    else {
      assertEquals("50", rs.getString(1));
    }
    assertFalse(rs.next());
    rs.close();

    // Check the scan types opened
    if (checkScanTypes) {
      observer.addExpectedScanType("t.table_data", "t.if3_4_5",
          ScanType.SORTEDMAPINDEX);
      observer.checkAndClear();
    }

    ps = conn.prepareStatement("SELECT ID FROM T.TABLE_DATA "
        + "where F3=? AND UPPER(F4)=? AND UPPER(F5)=?");
    ps.setString(1, "TESTVALUE" + 112);
    ps.setString(2, "TESTVALUE" + 140);
    ps.setString(3, "TESTVALUE" + 336);
    rs = ps.executeQuery();
    assertTrue(rs.next());
    assertEquals(56, rs.getInt(1));
    assertFalse(rs.next());
    rs.close();

    // Check the scan types opened
    if (checkScanTypes) {
      observer.addExpectedScanType("t.table_data", "t.if3",
          ScanType.SORTEDMAPINDEX);
      observer.checkAndClear();
    }

    ps = conn.prepareStatement("SELECT ID FROM T.TABLE_DATA "
        + "where F3=? AND UPPER(F4)=?");
    ps.setString(1, "TESTVALUE" + 112);
    ps.setString(2, "TESTVALUE" + 140);
    rs = ps.executeQuery();
    assertTrue(rs.next());
    first = false;
    if (rs.getInt(1) == 56) {
      first = true;
      assertEquals("56", rs.getString(1));
    }
    else {
      assertEquals("57", rs.getString(1));
    }
    assertTrue(rs.next());
    if (first) {
      assertEquals("57", rs.getString(1));
    }
    else {
      assertEquals("56", rs.getString(1));
    }
    assertFalse(rs.next());
    rs.close();

    // Check the scan types opened
    if (checkScanTypes) {
      observer.addExpectedScanType("t.table_data", "t.if3",
          ScanType.SORTEDMAPINDEX);
      observer.checkAndClear();
    }

    GemFireXDQueryObserverHolder.clearInstance();
  }

  public static void assertUpdateCount(Statement st, int expectedRC, String sql)
      throws SQLException {
    assertEquals("Update count does not match:", expectedRC,
        st.executeUpdate(sql));
  }
}
