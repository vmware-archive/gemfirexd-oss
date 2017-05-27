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
package com.pivotal.gemfirexd.transactions;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;

public class TransactionRRHDFSTableDUnit extends DistributedSQLTestBase {

  public TransactionRRHDFSTableDUnit(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;

  @Override
  public void shutDownAll() throws Exception {
    try {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      st.execute("drop hdfsstore if exists txhdfs");
      st.close();
      conn.commit();
      delete(new File("./txhdfs"));
    } catch (Exception e) {
      getLogWriter().error("UNEXPECTED exception in tearDown: " + e, e);
    }
    super.shutDownAll();
  }

  /**
   * Test transactional inserts on HDFS tables.
   * Set Eviction criteria
   * one row should be in operational data
   * other row shoule be evicted
   * update the evicted row in tx
   * update the operational row in tx
   * verify
   * @throws Exception
   */
  public void testTransactionalInsertOnHDFSTableWithCustomEviction() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    Statement st = conn.createStatement();

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) "+ getSuffix() + "eviction by criteria ( c2 > 10 ) EVICT INCOMING");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    st.execute("insert into tran.t1 values (10, 10)");
    st.execute("insert into tran.t1 values (20, 20)");

    conn.commit(); // commit two rows.
    ResultSet rs = st.executeQuery("Select * from tran.t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain one row ", 1, numRows);

    rs.close();

    rs = st.executeQuery("Select * from tran.t1 -- GEMFIREXD-PROPERTIES queryHDFS=true \n");
    numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);

    //rs.close();
    //conn.commit();

    st.execute("update tran.t1 set c2= 30 where c1 > 20");
    conn.commit();
    //st.execute("delete from tran.t1 where c2 > 5");
    //st.execute("delete from tran.t1 where c2 =10");
    //st.execute("delete from tran.t1 where c2 =20");


    rs = st.executeQuery("Select * from tran.t1");
    numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain one row ", 1, numRows);
    rs.close();

    rs = st.executeQuery("Select * from tran.t1 -- GEMFIREXD-PROPERTIES queryHDFS=true \n");
    numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned
      numRows++;
    }
    assertEquals("ResultSet should contain two rows ", 2, numRows);
    // Close connection, resultset etc...

    st.execute("drop table tran.t1");

    st.close();
    conn.commit();
    conn.close();
  }

  /**
   * tx inserts on HDFS tables.
   * do delete of one row
   * verify
   *
   * @throws Exception
   */
  public void testTransactionalInsertOnHDFSTable() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    Statement st = conn.createStatement();

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "primary key(c1)) " + getSuffix());
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st.execute("insert into tran.t1 values (10, 10)");
    //st.execute("insert into tran.t1 values (20, 20)");

    conn.rollback();// rollback.

    ResultSet rs = st.executeQuery("Select * from tran.t1");
    assertFalse("ResultSet should be empty ", rs.next());
    rs.close();

    st.execute("insert into tran.t1 values (10, 10)");
    //st.execute("insert into tran.t1 values (20, 20)");

    conn.commit(); // commit two rows.
    rs = st.executeQuery("Select * from tran.t1");
    int numRows = 0;
    while (rs.next()) {
      // Checking number of rows returned, since ordering of results
      // is not guaranteed. We can write an order by query for this (another
      // test).
      numRows++;
    }
    assertEquals("ResultSet should contain one row ", 1, numRows);

    rs.close();
    //st.execute("update tran.t1 set c2= 30 where c1= 20");
    //st.execute("delete from tran.t1 where c2 > 5");
    st.execute("delete from tran.t1 where c2 =10");
    //st.execute("delete from tran.t1 where c2 =20");
    conn.commit();


    rs = st.executeQuery("Select * from tran.t1");
    assertFalse("ResultSet should be empty ", rs.next());
    rs.close();
    // Close connection, resultset etc...

    st.execute("drop table tran.t1");

    st.close();
    conn.commit();
    conn.close();
  }

  public void testRepeatableReadSmall() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    Statement st = conn.createStatement();
    ResultSet rs;

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("Create table tran.t1 (c1 int not null, c2 int not null, "
        + "primary key(c1)) partition by primary key redundancy 2 persistent hdfsstore (txhdfs)");
    conn.commit();

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    PreparedStatement pstmt = conn
        .prepareStatement("insert into tran.t1 values (?, ?)");
    for (int c1 = 10; c1 <= 100; c1 += 10) {
      pstmt.setInt(1, c1);
      pstmt.setInt(2, c1 / 2);
      pstmt.execute();
    }
    conn.commit();

    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    PreparedStatement pstmt2 = conn2
        .prepareStatement("select * from tran.t1 where c1 = ?");
    // check repeats in reads
    for (int c1 = 10; c1 <= 100; c1 += 10) {
      System.out.println("Executing for c1 "  + c1);
      pstmt2.setInt(1, c1);
      rs = pstmt2.executeQuery();
      assertTrue(rs.next());
      assertEquals(c1, rs.getInt(1));
      assertEquals(c1 / 2, rs.getInt(2));
      assertFalse(rs.next());
    }

    // check that if reads finish then updates can go ahead
    conn2.commit();

    assertEquals(3, st.executeUpdate("delete from tran.t1 where c1 > 70"));
    conn.commit();
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (80, 40)"));
    conn.commit();

    st.execute("drop table tran.t1");

    conn.commit();
    conn.close();
  }

  /**
   * Test that reads are really repeatable and that intervening writes get
   * conflicts during commit and do not get conflicts in case the read txns
   * commit before its commit.
   */
  public void testRepeatableRead() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    Statement st = conn.createStatement();
    ResultSet rs;

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("Create table tran.t1 (c1 int not null, c2 int not null, "
        + "primary key(c1)) partition by primary key redundancy 2 persistent hdfsstore (txhdfs)");
    conn.commit();

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    PreparedStatement pstmt = conn
        .prepareStatement("insert into tran.t1 values (?, ?)");
    for (int c1 = 10; c1 <= 100; c1 += 10) {
      pstmt.setInt(1, c1);
      pstmt.setInt(2, c1 / 2);
      pstmt.execute();
    }
    conn.commit();

    // first try with get convertible reads
    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);

    PreparedStatement pstmt2 = conn2
        .prepareStatement("select * from tran.t1 where c1 = ?");
    for (int c1 = 50; c1 <= 80; c1 += 10) {
      pstmt2.setInt(1, c1);
      rs = pstmt2.executeQuery();
      assertTrue(rs.next());
      assertEquals(c1, rs.getInt(1));
      assertEquals(c1 / 2, rs.getInt(2));
      assertFalse(rs.next());
    }

    st = conn.createStatement();
    st.execute("delete from tran.t1 where c1 > 70");
    try {
      conn.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    for (int c1 = 50; c1 <= 80; c1 += 10) {
      st.execute("delete from tran.t1 where c1 = " + c1);
      try {
        conn.commit();
        fail("expected a conflict exception");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
    }
    st.execute("update tran.t1 set c2 = 50 where c1 = 70");
    try {
      conn.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // check repeats in reads
    for (int c1 = 10; c1 <= 100; c1 += 10) {
      System.out.println("Executing for c1 "  + c1);
      pstmt2.setInt(1, c1);
      rs = pstmt2.executeQuery();
      assertTrue(rs.next());
      assertEquals(c1, rs.getInt(1));
      assertEquals(c1 / 2, rs.getInt(2));
      assertFalse(rs.next());
    }

    // check that if reads finish then updates can go ahead
    conn2.commit();

    assertEquals(3, st.executeUpdate("delete from tran.t1 where c1 > 70"));
    conn.commit();
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (80, 40)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (90, 45)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (100, 50)"));
    conn.commit();
    assertEquals(3, st.executeUpdate("delete from tran.t1 where c1 > 70"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (80, 40)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (90, 45)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (100, 50)"));
    conn.commit();

    // now check the above with bulk reads
    Statement st2 = conn2.createStatement();
    rs = st2.executeQuery("select * from tran.t1 "
        + "where c1 > 40 and c1 <= 80 order by c2");
    for (int c1 = 50; c1 <= 80; c1 += 10) {
      assertTrue(rs.next());
      assertEquals(c1, rs.getInt(1));
      assertEquals(c1 / 2, rs.getInt(2));
    }
    assertFalse(rs.next());

    st.execute("delete from tran.t1 where c1 > 70");
    try {
      conn.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    for (int c1 = 50; c1 <= 80; c1 += 10) {
      st.execute("delete from tran.t1 where c1 = " + c1);
      try {
        conn.commit();
        fail("expected a conflict exception");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
    }
    st.execute("update tran.t1 set c2 = 50 where c1 = 70");
    try {
      conn.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // check repeats in reads
    for (int c1 = 10; c1 <= 100; c1 += 10) {
      pstmt2.setInt(1, c1);
      rs = pstmt2.executeQuery();
      assertTrue(rs.next());
      assertEquals(c1, rs.getInt(1));
      assertEquals(c1 / 2, rs.getInt(2));
      assertFalse(rs.next());
    }

    // check that if reads finish then updates can go ahead
    conn2.commit();

    assertEquals(3, st.executeUpdate("delete from tran.t1 where c1 > 70"));
    conn.commit();
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (80, 40)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (90, 45)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (100, 50)"));
    conn.commit();
    assertEquals(3, st.executeUpdate("delete from tran.t1 where c1 > 70"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (80, 40)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (90, 45)"));
    assertEquals(1, st.executeUpdate("insert into tran.t1 values (100, 50)"));
    conn.commit();

    st.execute("drop table tran.t1");

    conn.commit();
    conn.close();
  }

  public void testTXWithHDFSEvictionRR() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("Create table tran.t1 (c1 int not null, c2 int not null , c3 int not null, constraint uq unique (c2, c3), "
        + "primary key(c1)) partition by primary key redundancy 2 persistent hdfsstore (txhdfs)");
    conn.commit();

    for (int i = 10; i < 100; i += 10) {
      st.executeUpdate("insert into tran.t1 values (" + i + "," + i * 2 + ","
          + i * 5 + ")");
    }
    conn.commit();

    try {
      for (int i = 100; i < 1000; i += 100) {
        st.executeUpdate("insert into tran.t1 values (" + i + "," + (i / 10)
            * 2 + "," + (i / 10) * 5 + ")");
      }
      fail("Did not get expected constraint violation exception");
    } catch (SQLException e) {
      if (!"23505".equals(e.getSQLState())) {
        throw e;
      }
    }
    st.execute("drop table tran.t1");

    conn.commit();
    conn.close();
  }

  public void testTXWithHDFSEvictionRC() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("Create table tran.t1 (c1 int not null, c2 int not null , c3 int not null, constraint uq unique (c2, c3), "
        + "primary key(c1)) partition by primary key redundancy 2 persistent hdfsstore (txhdfs)");
    conn.commit();

    st.executeUpdate("insert into tran.t1 values (10, 20, 30)");
    conn.commit();
    try {
      st.executeUpdate("insert into tran.t1 values (20, 20, 30)");
      fail("Did not get expected constraint violation exception");
    } catch (SQLException e) {
      if (!"23505".equals(e.getSQLState())) {
        throw e;
      }
    }

    st.execute("drop table tran.t1");

    conn.commit();
    conn.close();
  }

  /**
   * Insert 3 rows one of which should be evicted
   * query to see everything is as expected
   * @throws Exception
   */
  public void testTXWithHDFSEvictionMultiRowRC() throws Exception {
    startVMs(1, 3);

    final String homeDir = new File(".", "txhdfs").getAbsolutePath();

    Connection conn = TestUtil.jdbcConn;
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    ResultSet rs;

    checkDirExistence(homeDir);
    st.execute("create hdfsstore txhdfs namenode 'localhost' homedir '" +
        homeDir + "' queuepersistent true");

    st.execute("Create table tran.t1 (c1 int not null, c2 int not null , c3 int not null, constraint uq unique (c2, c3), "
        + "primary key(c1)) partition by primary key redundancy 2 persistent hdfsstore (txhdfs)" + " eviction by criteria ( c3 > 2000 ) EVICT INCOMING");
    conn.commit();

    st.executeUpdate("insert into tran.t1 values (10, 100, 1000)");
    st.executeUpdate("insert into tran.t1 values (20, 200, 2000)");
    st.executeUpdate("insert into tran.t1 values (30, 300, 3000)");
    conn.commit();

    rs = st.executeQuery("select * from tran.t1 where c2=300");
    assertFalse(rs.next());
    rs.close();
    conn.commit();

    st.execute("drop table tran.t1");

    conn.commit();
    conn.close();
  }

  protected int getIsolationLevel() {
    return Connection.TRANSACTION_REPEATABLE_READ;
  }

  // Assume no other thread creates the directory at the same time
  private void checkDirExistence(String path) {
    File dir = new File(path);
    if (dir.exists()) {
      delete(dir);
    }
  }

  private String getSuffix() {
    return "redundancy 2 persistent hdfsstore (txhdfs)";
  }

  /**
   * Check local data size.
   *
   * @param regionName
   *          region name.
   * @param numEntries
   *          number of entries expected.
   */
  public static void checkData(String regionName, long numEntries) {
    final PartitionedRegion r = (PartitionedRegion)Misc
        .getRegionForTable(regionName, true);
    final long localSize = r.getLocalSize();
    assertEquals(
        "Unexpected number of rows in the table " + r.getUserAttribute(),
        numEntries, localSize);
  }
}
