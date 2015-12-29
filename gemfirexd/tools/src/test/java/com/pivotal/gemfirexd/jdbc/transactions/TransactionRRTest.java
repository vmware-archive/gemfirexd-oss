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

package com.pivotal.gemfirexd.jdbc.transactions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController;

public class TransactionRRTest extends TransactionTest {

  public TransactionRRTest(String name) {
    super(name);
  }

//  @Override
  public void testTxnUpdateBehaviorForLocking() throws Exception {

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch barrier = new CountDownLatch(2);
    final java.sql.Connection childConn = getConnection();
    Connection conn = getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("Create table t1 (c1 int not null primary key, "
        + "qty int not null, c3 int not null, exchange varchar(20), "
        + "c4 int not null, constraint C3_Unique unique (c3), "
        + "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex')), "
        + "constraint qty_ck check (qty >= 5))");

    // stmt.execute("create index idx on t1(c4)");
    stmt.execute("insert into t1 values(1, 10, 1, 'nye', 4), "
        + "(2, 20, 2, 'amex', 4)");

    conn.commit();

    MemHeapScanController.setWaitObjectAfterFirstQualifyForTEST(latch);
    MemHeapScanController.setWaitBarrierBeforeFirstScanForTEST(barrier);
    MemHeapScanController.setWaitForLatchForTEST(0);

    try {
      final Exception[] ex = new Exception[] { null };
      Runnable r = new Runnable() {

        @Override
        public void run() {
          try {
            childConn.setTransactionIsolation(getIsolationLevel());
            Statement cstmt = childConn.createStatement();
            MemHeapScanController.setWaitForLatchForTEST(3);
            cstmt.execute("update t1 set c4 = 40 where c4 = 4");
            updateCnt = cstmt.getUpdateCount();
          } catch (Exception e) {
            ex[0] = e;
          } finally {
            MemHeapScanController.setWaitForLatchForTEST(0);
          }
        }
      };

      Thread t = new Thread(r);
      t.start();

      // set stop after first qualify
      // start second txn and fire update such that to change the columns which
      // will no more satisfy the above condition
      Connection conn2 = getConnection();
      conn2.setTransactionIsolation(getIsolationLevel());
      conn2.setAutoCommit(false);
      Statement stmt2 = conn2.createStatement();
      stmt2.execute("update t1 set c4 = 10");
      // expect a conflict in this thread since the other thread takes SH lock
      // before qualify that will conflict at commit time for REPEATABLE_READ
      try {
        conn2.commit();
        fail("expected conflict exception");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
      // now fire update on this thread and let the other thread conflict
      stmt2.execute("update t1 set c4 = 10");
      latch.countDown();
      // move forward with the first txn and it should not update the row
      t.join();
      assertEquals(0, updateCnt);
      // expect a conflict in other thread since it takes SH lock before qualify
      // that will block commit, and then it will try to upgrade to EX_SH which
      // will conflict with this thread
      assertNotNull(ex[0]);
      if (!(ex[0] instanceof SQLException)) {
        throw ex[0];
      }
      if (!"X0Z02".equals(((SQLException)ex[0]).getSQLState())) {
        throw ex[0];
      }
    } finally {

      MemHeapScanController.setWaitBarrierBeforeFirstScanForTEST(null);
      MemHeapScanController.setWaitObjectAfterFirstQualifyForTEST(null);
    }
    childConn.commit();
  }

  public void testBug44297_1() throws Exception {
    setupConnection();
    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // create the table and load data
    stmt.execute("create table trade.customers (cid int primary key, "
        + "cust_name varchar(100) not null, tid int not null)");
    for (int id = 1; id <= 40; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }

    // fire selects using bulk table scans and hash index scans
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    assertEquals(1, stmt.executeUpdate("update trade.customers set "
        + "cust_name='cust1' where cid = 2 and tid <= 100"));

    ResultSet rs = stmt.executeQuery("select * from trade.customers where "
        + "tid > 8 and cid > 9 and cid < 11");

    assertTrue(rs.next());
    assertEquals(10, rs.getInt(1));
    assertEquals(20, rs.getInt(3));
    assertFalse(rs.next());

    // now try a delete on those rows and it should throw a conflict
    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    Statement stmt2 = conn2.createStatement();
    for (int id = 5; id <= 15; id++) {
      assertEquals(1,
          stmt2.executeUpdate("delete from trade.customers where cid=" + id));
    }
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    assertEquals(32,
        stmt2.executeUpdate("delete from trade.customers where cid > 8"));
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // now the same with updates
    for (int id = 10; id <= 20; id++) {
      int updated = stmt2
          .executeUpdate("update trade.customers set cust_name='customer" + id
              + "' where cid=" + id + " and tid >=" + id);
      assertEquals(1, updated);
    }
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    assertEquals(32, stmt2.executeUpdate("update trade.customers set "
        + "cust_name='cust' where cid > 8"));
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // now try the same after releasing the read locks and it should be fine
    conn.commit();

    for (int id = 5; id <= 15; id++) {
      assertEquals(1,
          stmt2.executeUpdate("delete from trade.customers where cid=" + id));
    }
    conn2.commit();
    for (int id = 5; id <= 15; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }
    conn.commit();
    assertEquals(32,
        stmt2.executeUpdate("delete from trade.customers where cid > 8"));
    conn2.commit();
    for (int id = 9; id <= 40; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }
    conn.commit();

    for (int id = 10; id <= 20; id++) {
      int updated = stmt2
          .executeUpdate("update trade.customers set cust_name='customer" + id
              + "' where cid=" + id + " and tid >=" + id);
      assertEquals(1, updated);
    }
    conn2.commit();
    assertEquals(32, stmt2.executeUpdate("update trade.customers set "
        + "cust_name='cust' where cid > 8"));
    conn2.commit();

    conn2.commit();
    conn.commit();
  }

  public void testBug44297_2() throws Exception {
    setupConnection();
    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // create the table and load data
    stmt.execute("create table trade.customers (cid int primary key, "
        + "cust_name varchar(100) not null, tid int not null)");
    for (int id = 1; id <= 40; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }

    // fire selects using bulk table scans and hash index scans
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    assertEquals(1, stmt.executeUpdate("update trade.customers set "
        + "cust_name='cust1' where cid = 2 and tid <= 100"));

    ResultSet rs = stmt.executeQuery("select * from trade.customers where "
        + "tid > 8 and cid=10");

    assertTrue(rs.next());
    assertEquals(10, rs.getInt(1));
    assertEquals(20, rs.getInt(3));
    assertFalse(rs.next());

    // now try a delete on those rows and it should throw a conflict
    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    Statement stmt2 = conn2.createStatement();
    for (int id = 5; id <= 15; id++) {
      assertEquals(1,
          stmt2.executeUpdate("delete from trade.customers where cid=" + id));
    }
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    assertEquals(32,
        stmt2.executeUpdate("delete from trade.customers where cid > 8"));
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // now the same with updates
    for (int id = 10; id <= 20; id++) {
      int updated = stmt2
          .executeUpdate("update trade.customers set cust_name='customer" + id
              + "' where cid=" + id + " and tid >=" + id);
      assertEquals(1, updated);
    }
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    assertEquals(32, stmt2.executeUpdate("update trade.customers set "
        + "cust_name='cust' where cid > 8"));
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    // now try the same after releasing the read locks and it should be fine
    conn.commit();

    for (int id = 5; id <= 15; id++) {
      assertEquals(1,
          stmt2.executeUpdate("delete from trade.customers where cid=" + id));
    }
    conn2.commit();
    for (int id = 5; id <= 15; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }
    conn.commit();
    assertEquals(32,
        stmt2.executeUpdate("delete from trade.customers where cid > 8"));
    conn2.commit();
    for (int id = 9; id <= 40; id++) {
      stmt.execute("insert into trade.customers values (" + id + ", 'cust" + id
          + "', " + (id * 2) + ')');
    }
    conn.commit();

    for (int id = 10; id <= 20; id++) {
      int updated = stmt2
          .executeUpdate("update trade.customers set cust_name='customer" + id
              + "' where cid=" + id + " and tid >=" + id);
      assertEquals(1, updated);
    }
    conn2.commit();
    assertEquals(32, stmt2.executeUpdate("update trade.customers set "
        + "cust_name='cust' where cid > 8"));
    conn2.commit();

    conn2.commit();
    conn.commit();
  }

  public void testBug44297_3() throws Exception {
    setupConnection();
    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // create the table and load data
    stmt.execute("create table trade.networth (cid int not null, "
        + "cash decimal (30, 1), securities decimal (30, 1), loanlimit int, "
        + "availloan decimal (30, 1),  tid int, "
        + "constraint netw_pk primary key (cid), "
        + "constraint cash_ch check (cash>=0), "
        + "constraint sec_ch check (securities >=0), "
        + "constraint availloan_ck check ("
        + "loanlimit>=availloan and availloan >=0))");

    // check read lock release for expressions in where clause for rows
    // that don't qualify
    for (int id = 650; id <= 700; id++) {
      stmt.execute("insert into trade.networth values (" + id + ", "
          + (id * 50) + ", " + (id * 10) + ", " + (id * 20) + ", " + (id * 20)
          + ", 5)");
    }

    // fire selects using bulk table scans and hash index scans
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    assertEquals(51, stmt.executeUpdate("update trade.networth set "
        + "availloan = 10500 where tid = 5"));
    conn.commit();

    ResultSet rs = stmt.executeQuery("select cid, loanlimit, availloan "
        + "from trade.networth where (loanlimit > 5000 and "
        + "loanlimit-availloan <= 3000) and tid = 5");

    int numResults = 0;
    while (rs.next()) {
      numResults++;
    }
    assertEquals(26, numResults);

    // now try a delete on a selected row and it should throw a conflict
    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    Statement stmt2 = conn2.createStatement();
    assertEquals(1,
        stmt2.executeUpdate("delete from trade.networth where cid=650"));
    try {
      conn2.commit();
      fail("expected a conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    // try delete on unselected row and it should be fine
    assertEquals(1,
        stmt2.executeUpdate("delete from trade.networth where cid=700"));
    conn2.commit();

    conn.commit();

    assertEquals(1,
        stmt2.executeUpdate("delete from trade.networth where cid=650"));
    conn2.commit();
  }

  @Override
  protected int getIsolationLevel() {
    return Connection.TRANSACTION_REPEATABLE_READ;
  }

  @Override
  protected void checkConnCloseExceptionForReadsOnly(Connection conn)
      throws SQLException {
    // conn.commit();
    try {
      conn.close();
      fail("expected connection active exception in close");
    } catch (SQLException e) {
      String state = e.getSQLState();
      if (!state.equals("25001")) {
        throw e;
      }
    }// catch.
  }
}
