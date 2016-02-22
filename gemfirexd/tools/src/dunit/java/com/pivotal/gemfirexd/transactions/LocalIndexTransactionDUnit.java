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
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.access.index.OpenMemIndex;
import com.pivotal.gemfirexd.internal.engine.access.index.SortedMap2IndexScanController;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.Conglomerate;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.util.TestException;
import junit.framework.AssertionFailedError;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * 
 * @author Asif
 *
 */
@SuppressWarnings("serial")
public class LocalIndexTransactionDUnit extends DistributedSQLTestBase {

  public LocalIndexTransactionDUnit(String name) {
    super(name);
  }

  public static void main(String[] args)
  {
    TestRunner.run(new TestSuite(LocalIndexTransactionDUnit.class));
  }

  // Neeraj: Disabling this test as with the new on the fly index maintenance
  // this does not hold good.
  /*
  private static GemFireTransaction gft;

  private static FormatableBitSet indexCols;

  private static GemFireContainer baseContainer;

  public void testLocalIndexKeyImpactedByTxSingleColIndex() throws Exception
  {
    // Test local index impacted by tx insert
    startClientVMs(1);
    startServerVMs(1);
   
    final String baseTable = "T1";

    try {
      java.sql.Connection conn = TestUtil.getConnection();
      final long connID = ((EmbedConnection)conn).getConnectionID();
      final String schema = TestUtil.getCurrentDefaultSchemaName();
      conn.setTransactionIsolation(getIsolationLevel());
      Statement st = conn.createStatement();
      st
          .execute("Create table t1 (c1 int not null , c2 int not null, c3 int not null,"
              + " c4 int not null , c5 int not null ," + " primary key(c1)) ");
      st.execute("create index i1 on t1 (c5)");
      conn.commit();
      PreparedStatement pstmt = conn
          .prepareStatement("insert into t1 values(?,?,?,?,?)");
      for (int i = 1; i < 41; ++i) {
        pstmt.setInt(1, i);
        pstmt.setInt(2, i);
        pstmt.setInt(3, i);
        pstmt.setInt(4, i);
        pstmt.setInt(5, i);
        pstmt.executeUpdate();

      }
      SerializableRunnable fieldInitializer = getFieldInitializer(schema,
          connID, baseTable);
      serverExecute(1, fieldInitializer);
      serverExecute(1, new SerializableRunnable("validator1") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));

        }
      });

      conn.commit();
      serverExecute(1, new SerializableRunnable("validator2") {
        @Override
        public void run() throws CacheException
        {
          // Now the local index should not be impacted as commit happened
          assertFalse(gft.isLocalIndexImpacted(baseContainer, indexCols));

        }
      });

      // Now do bulk update which modifies index column.
      // this should impact local index
      assertEquals(3, st.executeUpdate("update t1 set c5 = 10 where "
          + "c1 > 3 and c1 <7 "));
      serverExecute(1, new SerializableRunnable("validator3") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();
      // Check behaviour for PK based delete
      assertEquals(1, st.executeUpdate("delete from t1 where c1 = 31 "));
      serverExecute(1, new SerializableRunnable("validator4") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();
      // Check behaviour for bulk delete
      assertEquals(3, st
          .executeUpdate("delete from t1 where c3 > 31  and c3 < 35 "));
      serverExecute(1, new SerializableRunnable("validator5") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();

      // Now do Pk based update which modifies index column.
      // this should impact local index
      assertEquals(1, st.executeUpdate("update t1 set c5 = 10 where c1 = 15 "));
      serverExecute(1, new SerializableRunnable("validator6") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();
      serverExecute(1, new SerializableRunnable("validator7") {
        @Override
        public void run() throws CacheException
        {
          // Now the local index should not be impacted as commit happened
          assertFalse(gft.isLocalIndexImpacted(baseContainer, indexCols));

        }
      });

      // Now do bulk update which does not modify index column.
      // this should not impact local index
      assertEquals(3, st.executeUpdate("update t1 set c4 = 10 where "
          + "c1 > 3 and c1 <7 "));
      serverExecute(1, new SerializableRunnable("validator8") {
        @Override
        public void run() throws CacheException
        {
          assertFalse(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();

      // Now do PK based update which does not modify index column.
      // this should not impact local index
      assertEquals(1, st.executeUpdate("update t1 set c4 = 10 where c1 = 18 "));
      serverExecute(1, new SerializableRunnable("validator9") {
        @Override
        public void run() throws CacheException
        {
          assertFalse(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      conn.commit();
      serverExecute(1, new SerializableRunnable("validator10") {
        @Override
        public void run() throws CacheException
        {
          assertFalse(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      for (int i = 42; i < 50; ++i) {
        pstmt.setInt(1, i);
        pstmt.setInt(2, i);
        pstmt.setInt(3, i);
        pstmt.setInt(4, i);
        pstmt.setInt(5, i);
        pstmt.executeUpdate();
      }
      serverExecute(1, new SerializableRunnable("validator11") {
        @Override
        public void run() throws CacheException
        {
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));
        }
      });

      assertEquals(3, st.executeUpdate("update t1 set c4 = 10 where "
          + "c1 > 3 and c1 <7 "));
      serverExecute(1, new SerializableRunnable("validator12") {
        @Override
        public void run() throws CacheException
        {
          // follow it with update which is on different col,
          // this should not impact local index affected
          assertTrue(gft.isLocalIndexImpacted(baseContainer, indexCols));

        }
      });
      conn.commit();

    }
    finally {
      GemFireXDQueryObserverHolder.clearInstance();
    }
  }
  */

  public void testTransactionalUpdateWithLocalIndexModified_EqualityCondition_PR()
      throws Exception {
    transactionalUpdateWithLocalIndexModified_EqualityCondition(true);
  }

  public void testTransactionalUpdateWithLocalIndexModified_EqualityCondition_RR()
      throws Exception {
    transactionalUpdateWithLocalIndexModified_EqualityCondition(false);
  }

  private void transactionalUpdateWithLocalIndexModified_EqualityCondition(
      boolean isPR) throws Exception {
    startVMs(1, 1);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st
        .execute("Create table t1 (c1 int not null , c2 int not null, c3 int not null,"
            + " c4 int not null , c5 int not null ,"
            + " primary key(c1)) "
            + (isPR ? "" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 21; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();

    }
    conn.commit();
    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);
      assertEquals(1, st.executeUpdate("update t1 set c5 =5 where c5 =1"));
      // Two rows should get modified . one in commited c5 = 5 and the other in
      // tx
      // area c5 =1
      assertEquals(2, st.executeUpdate("update t1 set c2 = 8  where c5 = 5"));
      conn.commit();

      ResultSet rs = st.executeQuery("Select c2 from t1 where c5 =5");
      assertTrue(rs.next());
      assertEquals(8, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(8, rs.getInt(1));
      conn.commit();
      csr = verifyIndexScan(3);
      serverExecute(1, csr);
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }
  }

  public void testTransactionalUpdateWithLocalIndexModified_Range_1_PR()
      throws Exception {
    transactionalUpdateWithLocalIndexModified_Range_1(true);
  }

  public void testTransactionalUpdateWithLocalIndexModified_Range_1_RR()
      throws Exception {
    transactionalUpdateWithLocalIndexModified_Range_1(false);
  }

  private void transactionalUpdateWithLocalIndexModified_Range_1(boolean isPR)
      throws Exception {
    startVMs(1, 3);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int  , c2 int , c3 int ,c4 int  , c5 int ,"
        + " primary key(c1)) " + (isPR ? "redundancy 2" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 31; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();

    }
    conn.commit();

    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");

    try {
      csr.run();
      serverExecute(1, csr);
      assertEquals(11, st
          .executeUpdate("update t1 set c5 = 25 where c5  > 9 and c5 < 21"));
      // Two sets of rows should get modified - one in commited c5 > 20 and
      // other in tx c5==25
      assertEquals(21, st.executeUpdate("update t1 set c2 = 8  where c5 > 20"));
      conn.commit();

      ResultSet rs = st.executeQuery("Select c2 from t1 where c5 > 20");
      for (int i = 1; i < 22; ++i) {
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
      }
      assertFalse(rs.next());
      conn.commit();
      if (isPR) {
        csr = verifyIndexScan(3);
        serverExecute(1, csr);
      }
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
      assertEquals(4, st
          .executeUpdate("update t1 set c5 = 10  where c1 > 15 and c1 < 20"));
      csr = getObserverInitializer(schema, baseTable, "I1");
      csr.run();
      serverExecute(1, csr);
      assertEquals(4, st
          .executeUpdate("update t1 set c2 = 1   where c5 > 9 and c5 < 13"));
      conn.commit();

      rs = st.executeQuery("Select c2 from t1 where c5 > 9 and c5 < 13");
      for (int i = 1; i < 5; ++i) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
      assertFalse(rs.next());
      if (isPR) {
        csr = verifyIndexScan(2);
        serverExecute(1, csr);
      }
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }
  }

  public void testTxUpdateOnTxInsert_Bug42177_PR() throws Exception {
    txUpdateOnTxInsert_Bug42177(true);
  }

  public void testTxUpdateOnTxInsert_Bug42177_RR_Bug42266() throws Exception {
    txUpdateOnTxInsert_Bug42177(false);
  }

  private void txUpdateOnTxInsert_Bug42177(boolean isPR) throws Exception {
    startVMs(1, 3);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null , c2 int not null,"
        + " c3 int not null, c4 int not null, c5 int not null ,"
        + " primary key(c1)) " + (isPR ? "redundancy 1" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(?,?,?,?,?)");
    for(int i = 1 ;i < 10;++i) {
      pstmt.setInt(1,i);
      pstmt.setInt(2,i);
      pstmt.setInt(3,i);
      pstmt.setInt(4,i);
      pstmt.setInt(5,i);
      pstmt.executeUpdate();
      
    }    
    conn.commit();
    
    pstmt = conn.prepareStatement("insert into t1 values(?,?,?,?,?)");
    for(int i = 10 ;i < 21;++i) {
      pstmt.setInt(1,i);
      pstmt.setInt(2,i);
      pstmt.setInt(3,i);
      pstmt.setInt(4,i);
      pstmt.setInt(5,i);
      pstmt.executeUpdate();
      
    }    
    
    SerializableRunnable csr = getObserverInitializer(schema,baseTable,"I1");
    try {
    csr.run();
    serverExecute(1, csr);
    assertEquals(1,st.executeUpdate("update t1 set c5 =5 where c5 =1"));
    //Two rows should get modified . one in commited c5 = 5 and the other in tx area c5 =1
    assertEquals(2,st.executeUpdate("update t1 set c2 = 8  where c5 = 5"));
    conn.commit();
   
      ResultSet rs = st.executeQuery("Select c2 from t1 where c5 =5");     
      assertTrue(rs.next());
      assertEquals(8,rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(8,rs.getInt(1));
      conn.commit();
      if (isPR) {
        csr = verifyIndexScan(3);
        serverExecute(1, csr);
      }
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }  
  }

  public void testTxInsertUpdateDeleteCombo_PR() throws Exception {
    txInsertUpdateDeleteCombo(true);
  }

  public void testTxInsertUpdateDeleteCombo_RR() throws Exception {
    txInsertUpdateDeleteCombo(false);
  }

  private void txInsertUpdateDeleteCombo(boolean isPR) throws Exception {
    startVMs(1, 2);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null, c2 int not null, "
        + "c3 int not null, c4 int not null, c5 int not null ,"
        + " primary key(c1)) " + (isPR ? "redundancy 1" : "replicate"));
    st.execute("create index i1 on t1 (c5) ");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 6; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();
    }

    for (int i = 31; i < 41; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();
    }
    conn.commit();

    pstmt = conn.prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 6; i < 31; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();
    }
    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      // Make c5 = 5 for commited and non commited insert
      assertEquals(10,
          st.executeUpdate("update t1 set c5 = 5 where c5 > 0 and c5 < 11"));
      // c2 would be set to 8 for 1 to 10 and then 15 to 20
      assertEquals(16, st.executeUpdate("update t1 set c2 = 8 "
          + " where c5 = 5 or ( c5 > 14 and c5 < 21)"));
      // 22 rows should be deleted some txnl and some committed
      assertEquals(22,
          st.executeUpdate("delete from t1 where c2 = 8 or c2 > 34"));
      conn.commit();
      if (isPR) {
        csr = verifyIndexScan(2);
        serverExecute(1, csr);
      }
    } finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }
  }

  public void testTxInsertUpdateDeleteComboForBatching_PR() throws Exception {
    txInsertUpdateDeleteComboForBatching(true);
  }

  public void testTxInsertUpdateDeleteComboForBatching_RR() throws Exception {
    txInsertUpdateDeleteComboForBatching(false);
  }

  private void txInsertUpdateDeleteComboForBatching(boolean isPR)
      throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    final String defaultSchema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null, c2 int not null, "
        + "c3 int not null, c4 int not null, c5 int not null ,"
        + " primary key(c1)) " + (isPR ? "redundancy 1" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    st.execute("create index i2 on t1 (c2)");
    conn.commit();

    // do ops from server to check batching
    serverExecute(PartitionedRegion.rand.nextInt(3) + 1,
        new SerializableRunnable() {

      @Override
      public void run() {
        try {
          Connection conn = TestUtil.getConnection();
          conn.setTransactionIsolation(getIsolationLevel());
          PreparedStatement pstmt = conn
              .prepareStatement("insert into t1 values(?,?,?,?,?)");
          for (int i = 1; i < 6; ++i) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i);
            pstmt.setInt(3, i);
            pstmt.setInt(4, i);
            pstmt.setInt(5, i);
            pstmt.executeUpdate();
          }

          for (int i = 31; i < 41; ++i) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i);
            pstmt.setInt(3, i);
            pstmt.setInt(4, i);
            pstmt.setInt(5, i);
            pstmt.executeUpdate();
          }
          conn.commit();
        } catch (SQLException sqle) {
          throw new TestException("unexpected exception", sqle);
        }
      }
    });

    // do ops from server to check batching
    serverExecute(1, new SerializableRunnable() {

      @Override
      public void run() {
        try {
          final Connection conn = TestUtil.jdbcConn;
          conn.setTransactionIsolation(getIsolationLevel());
          PreparedStatement pstmt = conn
              .prepareStatement("insert into t1 values(?,?,?,?,?)");
          for (int i = 6; i < 31; ++i) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i);
            pstmt.setInt(3, i);
            pstmt.setInt(4, i);
            pstmt.setInt(5, i);
            pstmt.executeUpdate();
          }
        } catch (SQLException sqle) {
          throw new TestException("unexpected exception", sqle);
        }
      }
    });

    // do ops from server to check batching
    serverExecute(1, new SerializableRunnable() {

      @Override
      public void run() {
        try {
          final Connection conn = TestUtil.jdbcConn;
          final Statement st = conn.createStatement();
          // first put convertible updates to not flush batch
          assertEquals(1, st.executeUpdate("update t1 set c5 = 5 where c1 = 1"));
          assertEquals(1, st.executeUpdate("update t1 set c5 = 6 where c1 = 2"));
          assertEquals(1, st.executeUpdate("update t1 set c5 = 6 where c1 = 7"));
          assertEquals(1, st.executeUpdate("update t1 set c5 = 5 where c1 = 9"));
          // Make c5 = 5 for commited and non commited insert
          assertEquals(8,
              st.executeUpdate("update t1 set c5 = 5 where c1 > 2 and c1 < 11"));
          // more put convertible updates
          assertEquals(1, st.executeUpdate("update t1 set c2 = 8 where c1 = 1"));
          assertEquals(1, st.executeUpdate("update t1 set c2 = 8 where c1 = 2"));
          assertEquals(1, st.executeUpdate("update t1 set c2 = 8 where c1 = 7"));
          assertEquals(1, st.executeUpdate("update t1 set c2 = 8 where c1 = 9"));
          // c2 would be set to 8 for 1 to 10 and then 15 to 20 except c1==2
          assertEquals(15, st.executeUpdate("update t1 set c2 = 8 "
              + " where c5 = 5 or ( c5 > 14 and c5 < 21)"));
          // 22 rows should be deleted some txnl and some committed
          assertEquals(22,
              st.executeUpdate("delete from t1 where c2 = 8 or c2 > 34"));
          conn.commit();
          TXManagerImpl.waitForPendingCommitForTest();
        } catch (SQLException sqle) {
          // dump all the indexes
          ((GfxdIndexManager)((LocalRegion)GemFireCacheImpl.getInstance()
              .getRegion("/" + defaultSchema + "/T1")).getIndexUpdater())
              .dumpAllIndexes();
          throw new TestException("unexpected exception", sqle);
        }
      }
    });

    // verify final values
    ResultSet rs = st
        .executeQuery("select c5 from t1 where c5 > 0 order by c5");
    for (int i = 11; i < 15; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    for (int i = 21; i < 35; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
    rs = st.executeQuery("select c2 from t1 where c2 > 0 order by c2");
    for (int i = 11; i < 15; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    for (int i = 21; i < 35; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
    conn.commit();
  }

  public void testTxDeleteImpactOnLocalIndex_PR() throws Exception
  {
    txDeleteImpactOnLocalIndex(true);
  }

  public void testTxDeleteImpactOnLocalIndex_RR() throws Exception
  {
    txDeleteImpactOnLocalIndex(false);
  }

  private void txDeleteImpactOnLocalIndex(boolean isPR) throws Exception
  {

    startVMs(1, 1);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st
        .execute("Create table t1 (c1 int not null , c2 int not null, c3 int not null,"
            + " c4 int not null , c5 int not null ,"
            + " primary key(c1)) "
            + (isPR ? "" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 41; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();

    }

    conn.commit();
    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      assertEquals(6, st
          .executeUpdate("Delete from t1  where c5 > 14 and c5 <= 20"));
      // Make c5 = 5 for commited and non commited insert
      /*
       * assertEquals(10, st
       * .executeUpdate("update t1 set c5 =5 where c5 > 0 and c5 < 11"));
       */
      // c2 would be set to 8 for 1 to 10 and then 15 to 20
      assertEquals(14, st.executeUpdate("update t1 set c2 = 8 "
          + " where c5 >= 1 and c5 < 21 "));
      conn.commit();
      csr = verifyIndexScan(2);
      serverExecute(1, csr);
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }

  }

  public void testTxPKDeleteImpactOnLocalIndex_PR() throws Exception
  {
    txPKDeleteImpactOnLocalIndex(true);
  }

  public void testTxPKDeleteImpactOnLocalIndex_RR() throws Exception
  {
    txPKDeleteImpactOnLocalIndex(false);
  }

  private void txPKDeleteImpactOnLocalIndex(boolean isPR) throws Exception
  {

    startVMs(1, 1);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st
        .execute("Create table t1 (c1 int not null , c2 int not null, c3 int not null,"
            + " c4 int not null , c5 int not null ,"
            + " primary key(c1)) "
            + (isPR ? "" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 31; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();

    }
    conn.commit();

    assertEquals(1, st.executeUpdate("Delete from t1  where c1 = 10"));
    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      // Make c5 = 5 for commited and non commited insert
      /*
       * assertEquals(10, st
       * .executeUpdate("update t1 set c5 =5 where c5 > 0 and c5 < 11"));
       */
      // c2 would be set to 8 for 1 to 10 and then 15 to 20
      assertEquals(19, st.executeUpdate("update t1 set c2 = 8 "
          + " where c5 >= 1 and c5 < 21 "));

      conn.commit();
      csr = verifyIndexScan(1);
      serverExecute(1, csr);
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }

  }

  public void testTxPKUpdateImpactOnLocalIndex_PR() throws Exception
  {
    txPKUpdateImpactOnLocalIndex(true);
  }

  public void testTtxPKUpdateImpactOnLocalIndex_RR() throws Exception
  {
    txPKUpdateImpactOnLocalIndex(false);
  }

  private void txPKUpdateImpactOnLocalIndex(boolean isPR) throws Exception
  {

    startVMs(1, 1);
    final String baseTable = "T1";
    java.sql.Connection conn = TestUtil.getConnection();
    final String schema = TestUtil.getCurrentDefaultSchemaName();
    conn.setTransactionIsolation(getIsolationLevel());
    Statement st = conn.createStatement();
    st
        .execute("Create table t1 (c1 int not null , c2 int not null, c3 int not null,"
            + " c4 int not null , c5 int not null ,"
            + " primary key(c1)) "
            + (isPR ? "" : "replicate"));
    st.execute("create index i1 on t1 (c5)");
    conn.commit();
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int i = 1; i < 31; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();

    }
    conn.commit();

    assertEquals(1, st.executeUpdate("update t1 set c5 = 20   where c1 = 15"));
    SerializableRunnable csr = getObserverInitializer(schema, baseTable,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      // c2 would be set to 8 for 1 to 10 and then 15 to 20
      assertEquals(9, st.executeUpdate("update t1 set c2 = 8 "
          + " where c5 >= 18 and c5 < 26 "));

      conn.commit();
      csr = verifyIndexScan(1);
      serverExecute(1, csr);
    }
    finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }
  }

  /**
   * Test for index data load from current transaction as well as other
   * transactions.
   */
  public void testTXIndexData_43299() throws Exception {
    // reduce lock timeout for this test
    Properties props = new Properties();
    props.setProperty(GfxdConstants.MAX_LOCKWAIT, "10000");

    startVMs(1, 1, 0, null, props);
    final String schema = "TEST";
    final String tableName = "T1";
    final String table = schema + '.' + tableName;
    java.sql.Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("Create table " + table + " (c1 int not null, c2 int not null, "
        + "c3 int not null, c4 int not null, c5 int not null, "
        + "primary key(c1))");

    PreparedStatement pstmt = conn.prepareStatement("insert into " + table
        + " values(?,?,?,?,?)");
    for (int i = 1; i < 31; ++i) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.setInt(4, i);
      pstmt.setInt(5, i);
      pstmt.executeUpdate();
    }

    // now start off another transaction to check index creation waits
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final Throwable[] failEx = new Throwable[] { null };
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          Connection conn = TestUtil.getConnection();
          conn.setTransactionIsolation(getIsolationLevel());
          conn.setAutoCommit(false);
          conn.createStatement().execute(
              "insert into " + table + " values(100, 100, 100, 100, 100)");

          // wait on barrier to force index creation happen after this
          barrier.await();
          // now wait on barrier again before commit
          barrier.await();
          // commit and end
          conn.commit();
        } catch (Throwable t) {
          failEx[0] = t;
          getLogWriter().error("unexpected exception", t);
        }
      }
    });
    t.start();

    // wait on barrier to let other thread do the insert first
    barrier.await();
    try {
      st.execute("create index i1 on " + table + " (c5)");
      fail("expected an unsupported exception");
    } catch (SQLException sqle) {
      if (!"0A000".equals(sqle.getSQLState())) {
        //throw new RuntimeException("got SQLState=" + sqle.getSQLState(), sqle);
        throw sqle;
      }
    }
    // committing the transaction should allow index creation to proceed but
    // will timeout due to lock held by other open transaction on table
    conn.commit();
    st = conn.createStatement();
    try {
      st.execute("create index i1 on " + table + " (c5)");
      fail("expected a timeout exception");
    } catch (SQLException sqle) {
      if (!"40XL1".equals(sqle.getSQLState())) {
        //throw new RuntimeException("got SQLState=" + sqle.getSQLState(), sqle);
        throw sqle;
      }
    }


    // break the barrier so index creation should now be successful
    barrier.await();
    assertEquals(1,
        st.executeUpdate("update " + table + " set c5 = 20 where c1 = 15"));
    assertEquals(5, st.executeUpdate("update " + table + " set c2 = 70 "
        + " where c5 >= 18 and c5 < 22"));
    conn.commit();
    st = conn.createStatement();
    st.execute("create index i1 on " + table + " (c5)");

    // check success in other thread
    t.join();
    assertNull(failEx[0]);

    SerializableRunnable csr = getObserverInitializer(schema, tableName,
        "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      // c2 would be set to 8 for 1 to 10 and then 15 to 20
      assertEquals(9, st.executeUpdate("update " + table + " set c2 = 8 "
          + " where c5 >= 18 and c5 < 26 "));

      csr = verifyIndexScan(1);
      serverExecute(1, csr);
    } finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }

    // now check correct values using index scan and table scan
    csr = getObserverInitializer(schema, tableName, "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      ResultSet rs = st.executeQuery("select c1, c2 from " + table
          + " where c5 = 20");
      assertTrue(rs.next());
      int v1 = rs.getInt(1);
      assertEquals(8, rs.getInt(2));
      assertTrue(rs.next());
      int v2 = rs.getInt(1);
      assertEquals(8, rs.getInt(2));
      assertFalse(rs.next());

      assertTrue("unexpected values v1=" + v1 + ", v2=" + v2,
          (v1 == 15 && v2 == 20) || (v1 == 20 && v2 == 15));

      csr = verifyIndexScan(1);
      serverExecute(1, csr);
    } finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }

    ResultSet rs = st.executeQuery("select c1, c2, c5 from " + table
        + " where c1 = 15 or c1 = 20");
    assertTrue(rs.next());
    int v1 = rs.getInt(1);
    assertEquals(8, rs.getInt(2));
    assertEquals(20, rs.getInt(3));
    assertTrue(rs.next());
    int v2 = rs.getInt(1);
    assertEquals(8, rs.getInt(2));
    assertEquals(20, rs.getInt(3));
    assertFalse(rs.next());

    assertTrue("unexpected values v1=" + v1 + ", v2=" + v2,
        (v1 == 15 && v2 == 20) || (v1 == 20 && v2 == 15));

    // check for value from other transaction
    csr = getObserverInitializer(schema, tableName, "I1");
    try {
      csr.run();
      serverExecute(1, csr);

      rs = st.executeQuery("select c1, c2 from " + table + " where c5 = 100");
      assertTrue(rs.next());
      assertEquals(100, rs.getInt(1));
      assertEquals(100, rs.getInt(2));
      assertFalse(rs.next());

      csr = verifyIndexScan(1);
      serverExecute(1, csr);
    } finally {
      csr = clearObserver();
      csr.run();
      serverExecute(1, csr);
    }

    rs = st.executeQuery("select c1, c2, c5 from " + table + " where c3 = 100");
    assertTrue(rs.next());
    assertEquals(100, rs.getInt(1));
    assertEquals(100, rs.getInt(2));
    assertEquals(100, rs.getInt(3));
    assertFalse(rs.next());
  }

  /*
  private SerializableRunnable getFieldInitializer(final String schema,
      final long connID, final String baseTable)
  {
    return new SerializableRunnable("Field Initializer") {
      @Override
      public void run() throws CacheException
      {
        try {
          GfxdConnectionWrapper wrapper = GfxdConnectionHolder.getHolder()
              .createWrapper(schema, connID, false);
          EmbedConnection conn = wrapper.getConnectionForSynchronization();
          gft = TestUtil.getGFT(conn);
          String regionName = Misc.getRegionPath(schema, baseTable, null);
          LocalRegion baseTableRegion = (LocalRegion)Misc.getRegion(regionName);
          baseContainer = (GemFireContainer)baseTableRegion.getUserAttribute();
          GemFireContainer indexContainer = TestUtil.getIndexContainer(
              baseTableRegion, schema + ".I1:base-table:" + schema + ".T1");
          ExtraTableInfo eti = indexContainer.getExtraTableInfo();
          indexCols = eti.getIndexedCols();
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
  */

  protected int getIsolationLevel() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  private SerializableRunnable getObserverInitializer(final String schema, 
      final String baseTable, final String indexName)
  {
    return new SerializableRunnable("Observer Initializer") {
      @Override
      public void run() throws CacheException
      {
        try {
          GemFireXDQueryObserver observer = new IndexInvocationObserver(schema, baseTable, indexName);
          GemFireXDQueryObserverHolder.setInstance(observer);
          
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
  
  private static class IndexInvocationObserver extends GemFireXDQueryObserverAdapter {
     private final String qualifiedIndexName;
     private volatile int numInvocations;
     
     IndexInvocationObserver(String schema, String baseTable, String indexName ) {
       qualifiedIndexName = schema.toUpperCase()+"."+indexName.toUpperCase() + ":base-table:"+
     schema.toUpperCase()+"."+baseTable.toUpperCase();
     getGlobalLogger().info("Qualified index name to compare="+qualifiedIndexName);
    }
    
    @Override
    public double overrideDerbyOptimizerIndexUsageCostForHash1IndexScan(
        OpenMemIndex memIndex, double optimzerEvalutatedCost)
    {
      return Double.MAX_VALUE;
    }
    
    @Override
    public double overrideDerbyOptimizerCostForMemHeapScan(
        GemFireContainer gfContainer, double optimzerEvalutatedCost)
    {
      return Double.MAX_VALUE;
    }
    
    @Override
    public double overrideDerbyOptimizerIndexUsageCostForSortedIndexScan(
        OpenMemIndex memIndex, double optimzerEvalutatedCost)
    {
      return 1;
    }

    @Override
    public void scanControllerOpened(Object sc, Conglomerate conglom) {
      if (sc instanceof SortedMap2IndexScanController) {
        SortedMap2IndexScanController smisc = (SortedMap2IndexScanController)sc;
        if (smisc.getQualifiedIndexName().equals(this.qualifiedIndexName)) {
          ++this.numInvocations;
        }
      }
    }

    int getNumInvocation() {
      return this.numInvocations;
    }

  }

  private SerializableRunnable clearObserver( )
  {
    return new SerializableRunnable("clear Observer") {
      @Override
      public void run() throws CacheException
      {
        try {          
          GemFireXDQueryObserverHolder.clearInstance();
          /*
          baseContainer = null;
          gft = null;
          indexCols = null;
          */
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
  private SerializableRunnable verifyIndexScan( final int minimumINumInvocation)
  {
    return new SerializableRunnable("verify and clear Observer") {
      @Override
      public void run() throws CacheException
      {
        try {
          
          IndexInvocationObserver ico = GemFireXDQueryObserverHolder.getObserver(IndexInvocationObserver.class);
          try {
             assertTrue(ico.getNumInvocation() >= minimumINumInvocation);
          }catch(AssertionFailedError afe) {
            getLogWriter().error("Inequality not satisfied: minimum Num invocation expected = "+minimumINumInvocation 
                + "; actual invocations ="+ico.getNumInvocation());
            throw afe;
          }          
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
}
