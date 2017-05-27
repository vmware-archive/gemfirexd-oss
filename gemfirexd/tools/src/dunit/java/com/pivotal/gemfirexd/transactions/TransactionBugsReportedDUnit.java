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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gnu.trove.TIntHashSet;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.jdbc.transactions.TransactionTest;

import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class TransactionBugsReportedDUnit extends DistributedSQLTestBase {

  public TransactionBugsReportedDUnit(String name) {
    super(name);
  }

  public void testPkNonPkUpdates() throws Exception {
    startServerVMs(3, 0, null);

    serverExecute(1, new SerializableRunnable() {

      @Override
      public void run() {
        try {
          Connection conn = TestUtil.getConnection();
          conn.setAutoCommit(false);
          conn.setTransactionIsolation(getIsolationLevel());
          Statement stmt = conn.createStatement();
          // create table with primary key
          stmt.execute("create table t1 (id int primary key, "
              + "addr varchar(20)) redundancy 2");
          // create table without primary key
          stmt.execute("create table t2 (id int, "
              + "addr varchar(20)) redundancy 2");

          // some inserts
          PreparedStatement pstmt = conn
              .prepareStatement("insert into t1 values(?, ?)");
          for (int i = 0; i < 1000; i++) {
            pstmt.setInt(1, i);
            pstmt.setString(2, "addr" + i);
            assertEquals(1, pstmt.executeUpdate());
          }
          pstmt = conn.prepareStatement("insert into t2 values(?, ?)");
          for (int i = 0; i < 1000; i++) {
            pstmt.setInt(1, i);
            pstmt.setString(2, "addr" + i);
            assertEquals(1, pstmt.executeUpdate());
          }
          conn.commit();

          // now transactional updates
          pstmt = conn.prepareStatement("update t1 set addr = ? where id = ?");
          for (int i = 100; i < 200; i++) {
            pstmt.setString(1, "address" + i);
            pstmt.setInt(2, i);
            assertEquals(1, pstmt.executeUpdate());
          }
          pstmt = conn.prepareStatement("update t2 set addr = ? where id = ?");
          for (int i = 100; i < 200; i++) {
            pstmt.setString(1, "address" + i);
            pstmt.setInt(2, i);
            assertEquals(1, pstmt.executeUpdate());
          }

          ResultSet rs;
          int id;
          String addr;
          TIntHashSet ids = new TIntHashSet(100);
          // check transactional data
          ids.clear();
          rs = stmt
              .executeQuery("select * from t1 where id >= 100 and id < 200");
          while (rs.next()) {
            id = rs.getInt(1);
            addr = rs.getString(2);
            assertFalse("unexpected duplicate id=" + id, ids.contains(id));
            assertEquals("address" + id, addr);
            ids.add(id);
          }
          assertEquals(100, ids.size());

          ids.clear();
          rs = stmt
              .executeQuery("select * from t2 where id >= 100 and id < 200");
          while (rs.next()) {
            id = rs.getInt(1);
            addr = rs.getString(2);
            assertFalse("unexpected duplicate id=" + id, ids.contains(id));
            assertEquals("address" + id, addr);
            ids.add(id);
          }
          assertEquals(100, ids.size());
          conn.commit();

          // now after commit
          rs = stmt
              .executeQuery("select * from t1 where id >= 100 and id < 200");
          ids.clear();
          while (rs.next()) {
            id = rs.getInt(1);
            addr = rs.getString(2);
            assertFalse("unexpected duplicate id=" + id, ids.contains(id));
            assertEquals("address" + id, addr);
            ids.add(id);
          }
          assertEquals(100, ids.size());

          ids.clear();
          rs = stmt
              .executeQuery("select * from t2 where id >= 100 and id < 200");
          while (rs.next()) {
            id = rs.getInt(1);
            addr = rs.getString(2);
            assertFalse("unexpected duplicate id=" + id, ids.contains(id));
            assertEquals("address" + id, addr);
            ids.add(id);
          }
          assertEquals(100, ids.size());
        } catch (Exception ex) {
          fail("failed with exception", ex);
        }
      }
    });
  }

  public void testBug42905() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    java.sql.Connection conn = TestUtil.getConnection();
    //conn.setTransactionIsolation(getIsolationLevel());
    Statement stmt = conn.createStatement();
    stmt.execute("create synonym synForT1 for t1");
    stmt.execute("Create table t1 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null)");
    stmt.execute("Create table t2 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, foreign key (c1) references t1(c1))");
    stmt.execute("insert into synForT1 values(1, 1, 1)");
    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    stmt = conn.createStatement();

    boolean gotException = false;
    try {
      stmt.execute("insert into t2 values(1, 1, 1)");
      stmt.execute("delete from synForT1 where c1 = 1");
      fail("the above delete should have thrown a constraint violation");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testBug42923_uniqButUpdateRequiresFnExn() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, c2 int not null, "
        + "c3 int not null, constraint C3_Unique unique (c3))");

    clientSQLExecute(1, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    java.sql.Connection conn = TestUtil.getConnection();
    conn.setAutoCommit(false);
    // conn.setTransactionIsolation(getIsolationLevel());
    Statement stmt = conn.createStatement();
    
    conn.commit();
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("update t1 set c3 = 4 where c3 > 2");
          latch.countDown();
        } catch (Exception e) {
          fail("got exception in the spawned thread", e);
        }
      }
    };
    
    Thread t = new Thread(r);
    t.start();
    latch.await();
    conn.setTransactionIsolation(getIsolationLevel());
    
    stmt = conn.createStatement();
    boolean gotException = false;
    try {
      stmt.execute("update t1 set c3 = 4 where c3 > 2");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testBug42923_uniqButUpdateIsPutConvertible() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, c2 int not null, "
        + "c3 int not null, constraint C3_Unique unique (c3))");

    clientSQLExecute(1, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    java.sql.Connection conn = TestUtil.getConnection();
    // conn.setTransactionIsolation(getIsolationLevel());
    Statement stmt = conn.createStatement();
    
    conn.commit();
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("update t1 set c3 = 4 where c1 = 2");
          latch.countDown();
        } catch (Exception e) {
          fail("got exception in the spawned thread", e);
        }
      }
    };
    
    Thread t = new Thread(r);
    t.start();
    latch.await();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    
    stmt = conn.createStatement();
    boolean gotException = false;
    try {
      stmt.execute("update t1 set c3 = 4 where c1 = 2");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testTxnInsertUniqueIndexOnPartCol() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, c2 int not null, "
        + "c3 int not null, constraint C3_Unique unique (c3)) partition by column(c3)");

    clientSQLExecute(1, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    java.sql.Connection conn = TestUtil.getConnection();

    conn.commit();
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    Runnable r = new Runnable() {

      @Override
      public void run() {
        boolean gotException = false;
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("insert into t1 values(4, 4, 2)");
          latch.countDown();
        } catch (Exception e) {
          gotException = true;
          assertTrue(e instanceof SQLException);
          assertEquals("23505", ((SQLException)e).getSQLState());
        }
        assertTrue(gotException);
        latch.countDown();
      }
    };
    
    Thread t = new Thread(r);
    t.start();
    latch.await();
  }
  
  public void testTxnInsertUniqueIndexOnPartColButFirstAlsoUncommitted()
      throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(
        1,
        "Create table t1 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, constraint C3_Unique unique (c3)) partition by column(c3)");
    clientSQLExecute(1, "create synonym synForT1 for t1");

    final CountDownLatch latch = new CountDownLatch(1);

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("insert into synForT1 values(4, 4, 2)");
          latch.countDown();
        } catch (Exception e) {
          fail("should not have got exception", e);
        }
      }
    };

    Thread t = new Thread(r);
    t.start();
    latch.await();
    java.sql.Connection childConn = TestUtil.getConnection();
    childConn.setTransactionIsolation(getIsolationLevel());
    childConn.setAutoCommit(false);
    Statement stmt = childConn.createStatement();
    boolean gotException = false;
    try {
      stmt.execute("insert into synForT1 values(4, 4, 2)");
    } catch (Exception e) {
      gotException = true;
      gotException = true;
      assertTrue(e instanceof SQLException);
      assertEquals("X0Z02", ((SQLException)e).getSQLState());
    }
    assertTrue(gotException);
  }
  
  public void testTxnInsertUniqueIndexOnNonPartCol() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, c2 int not null, "
        + "c3 int not null, constraint C3_Unique unique (c3)) partition by column(c3)");

    clientSQLExecute(1, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)");
    java.sql.Connection conn = TestUtil.getConnection();

    conn.commit();
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    Runnable r = new Runnable() {

      @Override
      public void run() {
        boolean gotException = false;
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("insert into t1 values(4, 4, 2)");
          latch.countDown();
        } catch (Exception e) {
          gotException = true;
          assertTrue(e instanceof SQLException);
          assertEquals("23505", ((SQLException)e).getSQLState());
        }
        assertTrue(gotException);
        latch.countDown();
      }
    };
    
    Thread t = new Thread(r);
    t.start();
    latch.await();
  }
  
  public void testTxnInsertUniqueIndexOnNonPartColButFirstAlsoUncommitted()
      throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(
        1,
        "Create table t1 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, constraint C3_Unique unique (c3)) partition by column(c3)");

    final CountDownLatch latch = new CountDownLatch(1);

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("insert into t1 values(4, 4, 2)");
          latch.countDown();
        } catch (Exception e) {
          fail("should not have got exception", e);
        }
      }
    };

    Thread t = new Thread(r);
    t.start();
    latch.await();
    // TODO: KN: occasional suspect strings since main thread starts
    // shutdown and dropping conglomerates, while the other thread is still
    // holding an open transaction which gets rolled back due to shutdown
    // but roll back gets index cleanup exceptions; ideally the table drop
    // should have been blocked by the active transaction -- file a bug
    t.join();
    java.sql.Connection childConn = TestUtil.getConnection();
    childConn.setTransactionIsolation(getIsolationLevel());
    childConn.setAutoCommit(false);
    Statement stmt = childConn.createStatement();
    boolean gotException = false;
    try {
      stmt.execute("insert into t1 values(4, 4, 2)");
    } catch (Exception e) {
      gotException = true;
      assertTrue(e instanceof SQLException);
      assertEquals("X0Z02", ((SQLException)e).getSQLState());
    }
    assertTrue(gotException);
  }
  
  static volatile boolean gotException = false;
  
  public void testTxnDeleteParentRow() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(
        1,
        "Create table t1 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, constraint C3_Unique unique (c3))");
    
    clientSQLExecute(
        1,
    "Create table t2 (c1 int not null primary key, c2 int not null, "
    + "c3 int not null, foreign key (c1) references t1(c1))");

    clientSQLExecute(
        1,
        "insert into t1 values(1, 1, 1)");

    java.sql.Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmnt = conn.createStatement();
    stmnt.execute("delete from t1 where c1 = 1");
    
    final CountDownLatch latch = new CountDownLatch(1);

    Runnable r = new Runnable() {

      @Override
      public void run() {
        
        try {
          java.sql.Connection childConn = TestUtil.getConnection();
          childConn.setTransactionIsolation(getIsolationLevel());
          childConn.setAutoCommit(false);
          Statement childStmnt = childConn.createStatement();
          childStmnt.execute("insert into t2 values(1, 1, 1)");
          latch.countDown();
        } catch (Exception e) {
          assertTrue(e instanceof SQLException);
          assertEquals("X0Z02", ((SQLException)e).getSQLState());
          gotException = true;
        }
        assertTrue(gotException);
        latch.countDown();
      }
    };

    Thread t = new Thread(r);
    addExpectedException(null, new int[] { 1, 2 }, new Object[] {
        ConflictException.class });
    t.start();
    latch.await();
    assertTrue(gotException);
    removeExpectedException(null, new int[] { 1, 2 }, new Object[] {
        ConflictException.class });
  }
  
  public void testTxnDeleteParentRow_bothCaseOfChildCommittedAndUncommitted() throws Exception {
    
    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    
    Connection conn = TestUtil.getConnection();
    Connection conn2 = TestUtil.getConnection();
    Connection conn3 = TestUtil.getConnection();
    
    Statement stmt = conn.createStatement();
    stmt.execute("create synonym synForT2 for t2");
    stmt.execute("create synonym synForT3 for t3");
    stmt
        .execute("Create table t1 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, constraint C3_Unique unique (c3))");

    stmt.execute("create synonym synForT1 for t1");
    stmt
        .execute("Create table t2 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, foreign key (c1) references t1(c1))");

    stmt
        .execute("Create table t3 (c1 int not null primary key, c2 int not null, "
            + "c3 int not null, foreign key (c1) references t1(c1))");

    stmt.execute("insert into t1 values(1, 1, 1), (2, 2, 2)");

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    conn3.setTransactionIsolation(getIsolationLevel());
    conn3.setAutoCommit(false);

    Statement stmt2 = conn2.createStatement();
    Statement stmt3 = conn2.createStatement();

    stmt2.execute("insert into t2 values(1, 1, 1), (2, 2, 2)");
    stmt3.execute("insert into synForT3 values(1, 1, 1), (2, 2, 2)");

    for (int i = 0; i < 2; i++) {
      if (i == 1) {
        conn2.commit();
        conn3.commit();
      }
      boolean gotex = false;
      try {
        stmt.execute("delete from synForT1");
        fail("expected a conflict or constraint violation");
      } catch (SQLException e) {
        if (i == 0) {
          if (!"X0Z02".equals(e.getSQLState())) {
            throw e;
          }
        }
        else {
          if (!"23503".equals(e.getSQLState())) {
            throw e;
          }
        }
        gotex = true;
      }
      assertTrue(gotex);
    }
  }

  public void testTxnUpdateToSameValueOnSameRow() throws Exception {

    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    stmt.execute("Create table t1 (c1 int not null primary key, c2 int not null, " +
                "exchange varchar(20), symbol varchar(20), constraint sym_ex unique (symbol, exchange))");

    stmt.execute("insert into t1 values(1, 10, 'nyse', 'o'), (2, 20, 'amex', 'g')");

    conn.commit();
    stmt.execute("update t1 set exchange = 'nyse', symbol = 'o' where c1 = 1");
    conn.commit();
  }

  public void testTransactionsAndUniqueIndexes_43152() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    Connection nonTxconn = TestUtil.getConnection();
    Statement nonTxst = nonTxconn.createStatement();
    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    Statement st2 = conn2.createStatement();

    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "c3 int not null, primary key(c1), "
        + "constraint C3_Unique unique (c3)) replicate ");
    conn.commit();

    st.execute("insert into tran.t1 values(1, 1, 1)");

    conn.commit();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st.execute("update tran.t1 set c3 = 1, c2 = 4 where c1 = 1");

    st.execute("select * from tran.t1 where c3 = 1");

    ResultSet rs = st.getResultSet();
    int cnt = 0;
    while (rs.next()) {
      cnt++;
      assertEquals(1, rs.getInt(1));
      assertEquals(4, rs.getInt(2));
      assertEquals(1, rs.getInt(3));
    }
    assertEquals(1, cnt);

    nonTxst.execute("select * from tran.t1");

    ResultSet rsNonTx = nonTxst.getResultSet();
    int cntNonTx = 0;
    while (rsNonTx.next()) {
      cntNonTx++;
      assertEquals(1, rsNonTx.getInt(1));
      assertEquals(1, rsNonTx.getInt(2));
      assertEquals(1, rsNonTx.getInt(3));
    }
    assertEquals(1, cntNonTx);

    st2.execute("select * from tran.t1");

    ResultSet rsTx2 = st2.getResultSet();
    int cntTx2 = 0;
    while (rsTx2.next()) {
      cntTx2++;
      assertEquals(1, rsTx2.getInt(1));
      assertEquals(1, rsTx2.getInt(2));
      assertEquals(1, rsTx2.getInt(3));
    }
    assertEquals(1, cntTx2);

    st2.execute("select * from tran.t1 where c3 = 1");

    rsTx2 = st2.getResultSet();
    cntTx2 = 0;
    while (rsTx2.next()) {
      cntTx2++;
      assertEquals(1, rsTx2.getInt(1));
      assertEquals(1, rsTx2.getInt(2));
      assertEquals(1, rsTx2.getInt(3));
    }
    assertEquals(1, cntTx2);

    try {
      st2.execute("update tran.t1 set c3 = 1, c2 = 5 where c1 = 1");
      fail("the above update should have thrown conflict exception");
    } catch (SQLException sqle) {
      assertTrue("X0Z02".equals(sqle.getSQLState()));
    }

    Connection conn3 = TestUtil.getConnection();
    conn3.setTransactionIsolation(getIsolationLevel());
    conn3.setAutoCommit(false);
    Statement st3 = conn3.createStatement();

    try {
      st3.execute("delete from tran.t1");
      fail("the above delete should have thrown conflict exception");
    } catch (SQLException sqle) {
      assertTrue("X0Z02".equals(sqle.getSQLState()));
    }

    Connection conn4 = TestUtil.getConnection();
    conn4.setTransactionIsolation(getIsolationLevel());
    conn4.setAutoCommit(false);
    Statement st4 = conn4.createStatement();

    try {
      st4.execute("delete from tran.t1 where c1 = 1");
      fail("the above delete should have thrown conflict exception");
    } catch (SQLException sqle) {
      assertTrue("X0Z02".equals(sqle.getSQLState()));
    }

    st.execute("update tran.t1 set c3 = 1, c2 = 5 where c1 = 1");

    st.execute("select * from tran.t1");
    rs = st.getResultSet();
    cnt = 0;
    while (rs.next()) {
      cnt++;
      assertEquals(1, rs.getInt(1));
      assertEquals(5, rs.getInt(2));
      assertEquals(1, rs.getInt(3));
    }
    assertEquals(1, cnt);

    nonTxst.execute("select * from tran.t1");

    rsNonTx = nonTxst.getResultSet();
    cntNonTx = 0;
    while (rsNonTx.next()) {
      cntNonTx++;
      assertEquals(1, rsNonTx.getInt(1));
      assertEquals(1, rsNonTx.getInt(2));
      assertEquals(1, rsNonTx.getInt(3));
    }
    assertEquals(1, cntNonTx);

    conn.commit();

    nonTxst.execute("select * from tran.t1");

    rsNonTx = nonTxst.getResultSet();
    cntNonTx = 0;
    while (rsNonTx.next()) {
      cntNonTx++;
      assertEquals(1, rsNonTx.getInt(1));
      assertEquals(5, rsNonTx.getInt(2));
      assertEquals(1, rsNonTx.getInt(3));
    }
    assertEquals(1, cntNonTx);
    conn.commit();

    // also check for proper cleanup with multiple changes (#43761)
    st.execute("insert into tran.t1 values(2, 2, 2)");
    st.execute("insert into tran.t1 values(3, 3, 3)");
    st.execute("insert into tran.t1 values(4, 4, 4)");
    st.execute("update tran.t1 set c3 = 1, c2 = 4 where c1 = 1");
    st.execute("update tran.t1 set c3 = 2, c2 = 5 where c1 = 2");
    st.execute("update tran.t1 set c3 = 3, c2 = 6 where c1 = 3");
    conn.commit();

    rs = st.executeQuery("select c1, c2, c3 from tran.t1 order by c1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(1, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertFalse(rs.next());

    st.execute("update tran.t1 set c3 = 1, c2 = 5 where c1 = 1");
    st.execute("update tran.t1 set c3 = 2, c2 = 5 where c1 = 2");
    st.execute("update tran.t1 set c3 = 3, c2 = 5 where c1 = 3");

    rs = st.executeQuery("select c1, c2, c3 from tran.t1 order by c1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(1, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertFalse(rs.next());
    conn.commit();

    // check the latest updated values
    rs = st.executeQuery("select c1, c2, c3 from tran.t1 order by c1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(1, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertFalse(rs.next());

    // update again
    st.execute("update tran.t1 set c3 = 1, c2 = 6 where c1 = 1");
    st.execute("update tran.t1 set c3 = 2, c2 = 6 where c1 = 2");
    st.execute("update tran.t1 set c3 = 3, c2 = 6 where c1 = 3");

    rs = st.executeQuery("select c1, c2, c3 from tran.t1 order by c1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(1, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertFalse(rs.next());
    conn.commit();

    // check the latest updated values
    rs = st.executeQuery("select c1, c2, c3 from tran.t1 order by c1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(1, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertEquals(3, rs.getInt(3));
    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertEquals(4, rs.getInt(3));
    assertFalse(rs.next());
    conn.commit();
  }
  
  public void testPutAllInTxnAfterMerge_43733() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();

    st.execute("create schema tran");
    st.execute("Create table tran.t1 (c1 int not null , c2 int not null, "
        + "c3 int not null, primary key(c1), "
        + "constraint C3_Unique unique (c3)) partition by primary key ");

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    st.execute("insert into tran.t1 values(1, 1, 1), (2, 2, 2), (5, 10, 30), (100, 11, 12)");
    for (int i = 0; i < 10; i++) {
      st.execute("select * from tran.t1");
      ResultSet rs = st.getResultSet();
      int cnt = 0;
      while (rs.next()) {
        cnt++;
        if (rs.getInt(1) == 1) {
          assertEquals(1, rs.getObject(2));
          assertEquals(1, rs.getObject(3));
        }
        else if (rs.getInt(1) == 2) {
          assertEquals(2, rs.getObject(2));
          assertEquals(2, rs.getObject(3));
        }
        else if (rs.getInt(1) == 5) {
          assertEquals(10, rs.getObject(2));
          assertEquals(30, rs.getObject(3));
        }
        else if (rs.getInt(1) == 100) {
          assertEquals(11, rs.getObject(2));
          assertEquals(12, rs.getObject(3));
        }
        else {
          fail("unexpected result: " + rs.getInt(1));
        }
      }
      assertEquals(4, cnt);
    }

    conn.commit();

    Connection conn2 = TestUtil.getConnection();
    Statement st2 = conn2.createStatement();

    for (int i = 0; i < 10; i++) {
      st2.execute("select * from tran.t1");
      ResultSet rs = st2.getResultSet();
      int cnt = 0;
      while (rs.next()) {
        cnt++;
        if (rs.getInt(1) == 1) {
          assertEquals(1, rs.getObject(2));
          assertEquals(1, rs.getObject(3));
        }
        else if (rs.getInt(1) == 2) {
          assertEquals(2, rs.getObject(2));
          assertEquals(2, rs.getObject(3));
        }
        else if (rs.getInt(1) == 5) {
          assertEquals(10, rs.getObject(2));
          assertEquals(30, rs.getObject(3));
        }
        else if (rs.getInt(1) == 100) {
          assertEquals(11, rs.getObject(2));
          assertEquals(12, rs.getObject(3));
        }
        else {
          fail("unexpected result: " + rs.getInt(1));
        }
      }
      assertEquals(4, cnt);
    }
  }

  /**
   * Test foreign key constraint with null values for FK field under
   * transactions.
   * 
   * @throws Exception
   */
  public void testBug41168() throws Exception {

    // Start two clients and three servers
    startVMs(2, 3);

    // Create the table with self-reference FKs
    clientSQLExecute(1, "create table BinaryTree (id int primary key, "
        + "leftId int, rightId int, depth int not null,"
        + " foreign key (leftId) references BinaryTree(id),"
        + " foreign key (rightId) references BinaryTree(id)) replicate");

    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    addExpectedException(new int[] { 1, 2 }, new int[] { 1, 2, 3 },
        SQLIntegrityConstraintViolationException.class);
    TransactionTest.doBinaryTreeChecks(conn, true);
    removeExpectedException(new int[] { 1, 2 }, new int[] { 1, 2, 3 },
        SQLIntegrityConstraintViolationException.class);

    // now do the same for partitioned table
    serverSQLExecute(2, "drop table BinaryTree");
    clientSQLExecute(1, "create table BinaryTree (id int primary key, "
        + "leftId int, rightId int, depth int not null,"
        + " foreign key (leftId) references BinaryTree(id),"
        + " foreign key (rightId) references BinaryTree(id))");

    addExpectedException(new int[] { 1, 2 }, new int[] { 1, 2, 3 },
        SQLIntegrityConstraintViolationException.class);
    TransactionTest.doBinaryTreeChecks(conn, false);
    removeExpectedException(new int[] { 1, 2 }, new int[] { 1, 2, 3 },
        SQLIntegrityConstraintViolationException.class);
  }

  /**
   * Check that reference key checking holds the lock correctly on remote node
   * when parent and child are not colocated.
   */
  public void testBug44731() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    clientSQLExecute(1, "Create table t1 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null)");

    clientSQLExecute(1, "Create table t2 (c1 int not null primary key, "
        + "c2 int not null, c3 int not null, "
        + "foreign key (c2) references t1(c1)) partition by primary key");

    // some inserts
    for (int i = 1; i <= 10; i++) {
      serverSQLExecute(1, "insert into t1 values (" + i + ',' + (i + 5) + ','
          + (i + 3) + ')');
      if (i <= 5) {
        serverSQLExecute(2, "insert into t2 values (" + (i + 5) + ',' + i + ','
            + (i + 2) + ')');
      }
    }

    // now transactional inserts+deletes
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    for (int i = 6; i <= 10; i++) {
      stmt.execute("insert into t2 values (" + (i + 5) + ',' + i + ','
          + (i + 2) + ')');
    }

    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        ConflictException.class, "X0Z02" });
    Statement stmt2 = conn2.createStatement();
    // expect a conflict below
    for (int i = 6; i <= 10; i++) {
      try {
        stmt2.execute("delete from t1 where c2=" + (i + 5));
        fail("expected a conflict");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
    }

    // rollback inserts and check again
    conn.rollback();

    for (int i = 6; i <= 10; i++) {
      stmt2.execute("delete from t1 where c2=" + (i + 5));
    }

    // now conflicts other way round
    for (int i = 6; i <= 10; i++) {
      try {
        stmt.execute("insert into t2 values (" + (i + 5) + ',' + i + ','
            + (i + 2) + ')');
        fail("expected a conflict");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
    }
    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        ConflictException.class, "X0Z02" });

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, "23503");
    // now for the case of normal constraint violations rather than conflicts
    conn2.commit();
    for (int i = 6; i <= 10; i++) {
      try {
        stmt.execute("insert into t2 values (" + (i + 5) + ',' + i + ','
            + (i + 2) + ')');
        fail("expected constraint violation");
      } catch (SQLException sqle) {
        if (!"23503".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
    }
    conn.commit();
    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, "23503");
  }

  protected int getIsolationLevel() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }
}
