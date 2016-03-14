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
package com.pivotal.gemfirexd.insert;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireInsertResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.util.TestException;

public class DistributedInsertDUnit extends DistributedSQLTestBase {

  static boolean[] remoteCallbackInvoked = new boolean[] { false, false };
  
  public DistributedInsertDUnit(String name) {
    super(name);
  }

  /**
   * Tests multiple distributed inserts on Partitioned Tables.
   * 
   * @throws Exception
   */
  public void testDistribuedInsertOnPartitionedTables() throws Exception {
    // start one server and one client
    startVMs(1, 1);

    // create the schema
    clientSQLExecute(1, "create schema trade");

    // create the partitioned table
    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), "
        + "since date, addr varchar(100), tid int, " + "primary key (cid))");

    PreparedStatement custSt = null;
    try {
      custSt = getPreparedStatement("insert into trade.customers "
          + "values (?, ?, ?, ?, ?)");
    } catch (SQLException e) {
      throw new TestException("Exception getting preparedStatment ", e);
    }
    try {
      int maxRows = 100;
      for (int i = 0; i < maxRows; i++) {
        custSt.setInt(1, i);
        custSt.setString(2, "XXXX" + i);
        custSt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
        custSt.setString(4, "BEAV" + i);
        custSt.setInt(5, i);
        int r = custSt.executeUpdate();
        assertEquals("Distribted Insert failed ", 1, r);
      }

    } catch (SQLException e) {
      throw new TestException("Exception trying to execute "
          + "prepared statement", e);
    }
  }

  /**
   * Test multiple distribted inserts on replicated Tables.
   * 
   * @throws Exception
   */
  public void testDistribuedInsertOnReplicatedTables() throws Exception {
    // start one server and one client
    startVMs(1, 1);

    // create the schema
    clientSQLExecute(1, "create schema trade");

    // create the replicated table
    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), "
        + "since date, addr varchar(100), tid int, "
        + "primary key (cid)) REPLICATE");

    PreparedStatement custSt = null;
    try {
      custSt = getPreparedStatement("insert into trade.customers "
          + "values (?, ?, ?, ?, ?)");
    } catch (SQLException e) {
      throw new TestException("Exception getting preparedStatment ", e);
    }
    try {
      int maxRows = 100;
      for (int i = 0; i < maxRows; i++) {
        custSt.setInt(1, i);
        custSt.setString(2, "XXXX" + i);
        custSt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
        custSt.setString(4, "BEAV" + i);
        custSt.setInt(5, i);
        int r = custSt.executeUpdate();
        assertEquals("Distribted Insert failed ", 1, r);
      }

    } catch (SQLException e) {
      throw new TestException("Exception trying to execute "
          + "prepared statement", e);
    }
  }

  /**
   * Test PutAll and Drop Index in //.
   * 
   * @throws Exception
   */
  public void testBug47029() throws Exception {
    //System.setProperty("gemfirexd.debug.true", "QueryDistribution,TraceLock_*");

    // reduce lock timeout for this test
    Properties props = new Properties();
    props.setProperty(GfxdConstants.MAX_LOCKWAIT, "10000");

    // start one server and one client
    startVMs(1, 2, 0, null, props);
    GemFireXDQueryObserver old = null;
    try {
      // create the schema
      clientSQLExecute(1, "create schema trade");

      // create the partitioned table
      clientSQLExecute(1, "create table trade.customers (cid int not null, "
          + "cust_name varchar(100), "
          + "since date, addr varchar(100), tid int, " + "primary key (cid))");

      // create index
      clientSQLExecute(1, "create index trade.tid_idx on trade.customers (TID)");

      final boolean[] callbackInvoked = new boolean[] { false, false };
      final SerializableCallable obsAction = new SerializableCallable(
          "Drop Index") {
        @Override
        public Object call() throws CacheException {
          try {
            TestUtil.setupConnection();
            Statement stmt = TestUtil.jdbcConn.createStatement();
            stmt.executeUpdate("drop index trade.tid_idx");
            fail("Test should fail with LOCK_TIMEOUT");
          } catch (SQLException sqle) {
            assertEquals("40XL1", sqle.getSQLState());
            remoteCallbackInvoked[0] = true;
          } catch (Exception e) {
            e.printStackTrace();
          }
          remoteCallbackInvoked[1] = true;
          return remoteCallbackInvoked[0];
        }
      };
      final SerializableRunnable obsVerify = new SerializableRunnable(
          "Verify Observer") {
        @Override
        public void run() throws CacheException {
          assertTrue(remoteCallbackInvoked[0]);
          assertTrue(remoteCallbackInvoked[1]);
        }
      };
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterGemFireResultSetOpen(AbstractGemFireResultSet rs,
                LanguageConnectionContext lcc) {
              assertTrue(rs instanceof GemFireInsertResultSet);
              assertTrue(((GemFireInsertResultSet)rs).isPreparedBatch());
              callbackInvoked[0] = true;
            }

            @Override
            public void beforeFlushBatch(ResultSet rs,
                LanguageConnectionContext lcc) throws StandardException {
              assertTrue(rs instanceof GemFireInsertResultSet);
              try {
                assertTrue((Boolean)serverExecute(1, obsAction));
              } catch (Exception e1) {
                e1.printStackTrace();
              }
            }

            @Override
            public void afterFlushBatch(ResultSet rs,
                LanguageConnectionContext lcc) throws StandardException {
              assertTrue(rs instanceof GemFireInsertResultSet);
              try {
                clientSQLExecute(1, "drop index trade.tid_idx");
                callbackInvoked[1] = true;
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          });

      PreparedStatement custSt = null;
      try {
        custSt = getPreparedStatement("insert into trade.customers "
            + "values (?, ?, ?, ?, ?)");
      } catch (SQLException e) {
        throw new TestException("Exception getting preparedStatment ", e);
      }
      try {
        int maxRows = 100;
        for (int i = 0; i < maxRows; i++) {
          custSt.setInt(1, i);
          custSt.setString(2, "XXXX" + i);
          custSt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
          custSt.setString(4, "BEAV" + i);
          custSt.setInt(5, i);
          custSt.addBatch();
        }
        int[] r = custSt.executeBatch();
        assertEquals("Distribted Insert failed ", maxRows, r.length);
        assertTrue(callbackInvoked[0]);
        serverExecute(1, obsVerify);
        assertTrue(callbackInvoked[1]);
      } catch (SQLException e) {
        throw new TestException("Exception trying to execute "
            + "prepared statement", e);
      }
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      remoteCallbackInvoked[0] = false;
      remoteCallbackInvoked[1] = false;
    }
  }

  public static PreparedStatement getPreparedStatement(String sql)
      throws SQLException {

    TestUtil.setupConnection();
    PreparedStatement stmt = TestUtil.jdbcConn.prepareStatement(sql);
    return stmt;
  }
}
