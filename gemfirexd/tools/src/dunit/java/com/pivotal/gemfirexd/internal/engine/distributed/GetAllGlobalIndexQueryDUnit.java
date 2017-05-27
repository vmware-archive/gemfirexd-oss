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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.PrimaryDynamicKey;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.jdbc.GfxdCallbacksTest;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Tests for GetAllConvertible Queries
 * 
 * @author vivekb
 * 
 *         Some tests have corresponding junit tests in
 * @see com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfoInternalsTest
 */
@SuppressWarnings("serial")
public class GetAllGlobalIndexQueryDUnit extends DistributedSQLTestBase {
  /* Note:
   * Make this 'true' while running performance test @see _testINperf 
   */
  private static boolean changeDefaultTestProperties = false;

  // Use with THin Client Test
  static boolean[] remoteCallbackInvoked = new boolean[] { false, false, false, false };

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    if (changeDefaultTestProperties) {
      return "config";
    }
    else {
      return super.reduceLogging();
    }
  }

  public GetAllGlobalIndexQueryDUnit(String name) {
    super(name);
  }

  public void testINWithMultipleParameters() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    String query = "select type, id, name from t1 where id IN (?,?,?)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(3, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(3, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof PrimaryDynamicKey);
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(1, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(3, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);

      // insert data
      s.execute("Insert into  t1 values(1,'asif',2)");

      {
        ResultSet rs = ps1.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(2));
        assertEquals(2, rs.getInt(1));
        assertEquals("asif", rs.getString(3));
        assertTrue(callbackInvoked[0]);
        assertTrue(callbackInvoked[1]);
        assertTrue(callbackInvoked[2]);
        assertTrue(callbackInvoked[3]);
        rs.close();
      }
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testINWithMultipleParameters_noData() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    String query = "select type, id, name from t1 where id IN (?,?,?)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(3, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(3, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof PrimaryDynamicKey);
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(0, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(3, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);

      { // first with no data
        ResultSet rs = ps1.executeQuery();
        assertFalse(rs.next());
        assertTrue(callbackInvoked[0]);
        assertTrue(callbackInvoked[1]);
        assertTrue(callbackInvoked[2]);
        assertFalse(callbackInvoked[3]);
      }
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testINWithMultipleParameters_ThinClient() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Properties props = new Properties();
    props.setProperty("log-level", getLogLevel());
    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, props);
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    String query = "select type, id, name from t1 where id IN (?,?,?)";

    final GemFireXDQueryObserver getAllObserver = new GemFireXDQueryObserverAdapter() {
      @Override
      public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
          GenericPreparedStatement gps, LanguageConnectionContext lcc) {
        if (qInfo instanceof SelectQueryInfo) {
          remoteCallbackInvoked[0] = true;
          assertTrue(qInfo instanceof SelectQueryInfo);
          SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
          assertTrue(sqi.isPrimaryKeyBased());
          assertTrue(sqi.isDynamic());
          assertEquals(3, sqi.getParameterCount());
          Object[] pks = (Object[])sqi.getPrimaryKey();
          assertEquals(3, pks.length);
          for (int i = 0; i < pks.length; ++i) {
            assertTrue(pks[i] instanceof PrimaryDynamicKey);
          }
          try {
            assertTrue(sqi.createGFEActivation());
          } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
          }
        }
      }

      @Override
      public void createdGemFireXDResultSet(
          com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
        if (rs instanceof GemFireResultSet) {
          remoteCallbackInvoked[1] = true;
        }
      }

      @Override
      public void getAllInvoked(int numElements) {
        remoteCallbackInvoked[3] = true;
        //depending upon data is on same node or not
        assertTrue(numElements == 1 || numElements == 0);
      }
      
      @Override
      public void getAllGlobalIndexInvoked(int numElements) {
        remoteCallbackInvoked[2] = true;
        assertEquals(3, numElements);
      }
    };

    SerializableRunnable getAllObsSet = new SerializableRunnable(
        "Set GetAll Observer") {
      @Override
      public void run() throws CacheException {
        GemFireXDQueryObserverHolder.setInstance(getAllObserver);
      }
    };

    SerializableRunnable getAllObsReset = new SerializableRunnable(
        "Reset GetAll Observer") {
      @Override
      public void run() throws CacheException {
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
            });
      }
    };

    SerializableRunnable getAllObsVerify = new SerializableRunnable(
        "Verify GetAll Observer") {
      @Override
      public void run() throws CacheException {
        assertTrue(remoteCallbackInvoked[0]);
        assertTrue(remoteCallbackInvoked[1]);
        assertTrue(remoteCallbackInvoked[2]);
        assertTrue(remoteCallbackInvoked[3]);
      }
    };
    
    SerializableRunnable getAllObsVerify_noData = new SerializableRunnable(
        "Verify GetAll Observer if No Data") {
      @Override
      public void run() throws CacheException {
        assertTrue(remoteCallbackInvoked[0]);
        assertTrue(remoteCallbackInvoked[1]);
        assertTrue(remoteCallbackInvoked[2]);
        assertFalse(remoteCallbackInvoked[3]);
      }
    };

    try {
      serverExecute(1, getAllObsSet);

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);

      { // first with no data
        ResultSet rs = ps1.executeQuery();
        assertFalse(rs.next());
        serverExecute(1, getAllObsVerify_noData);
      }

      // insert data
      s.execute("Insert into  t1 values(1,'asif',2)");

      {
        ResultSet rs = ps1.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(2));
        assertEquals(2, rs.getInt(1));
        assertEquals("asif", rs.getString(3));
        serverExecute(1, getAllObsVerify);
        rs.close();
      }
    } finally {
      serverExecute(1, getAllObsReset);
    }
  }
  
  public void testINWithMultipleParameters_Replicated() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) replicate");
    String query = "select type, id, name from t1 where id IN (?,?,?)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertFalse(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(3, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertNull(pks);
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(3, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(3, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);

      { // first with no data
        ResultSet rs = ps1.executeQuery();
        assertFalse(rs.next());
        assertTrue(callbackInvoked[0]);
        assertFalse(callbackInvoked[1]);
        assertFalse(callbackInvoked[2]);
        assertFalse(callbackInvoked[3]);
      }

      // insert data
      s.execute("Insert into  t1 values(1,'asif',2)");

      {
        ResultSet rs = ps1.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(2));
        assertEquals(2, rs.getInt(1));
        assertEquals("asif", rs.getString(3));
        assertTrue(callbackInvoked[0]);
        assertFalse(callbackInvoked[1]);
        assertFalse(callbackInvoked[2]);
        assertFalse(callbackInvoked[3]);
        rs.close();
      }
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testINWithMultipleParametersAndDuplicateKeys() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    s.execute("Insert into  t1 values(1,'asif',2)");
    String query = "select type, id, name from t1 where id IN (?,?,?)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(3, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(3, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof PrimaryDynamicKey);
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(1, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(1, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 1);
      ps1.setInt(3, 1);
      ResultSet rs = ps1.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getInt(2), 1);
      assertEquals(rs.getInt(1), 2);
      assertEquals(rs.getString(3), "asif");
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testINWithConstantAndParameterizedFields() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    s.execute("Insert into  t1 values(1,'asif',2)");

    String query = "select type, id, name from t1 where id IN (?,?,3,4)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(sqi.getParameterCount(), 2);
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(pks.length, 4);
                for (int i = 0; i < pks.length; ++i) {
                  if (i < 2) {
                    assertTrue(pks[i] instanceof PrimaryDynamicKey);
                  }
                  else {
                    assertTrue(pks[i] instanceof RegionKey);
                  }
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(1, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(4, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ResultSet rs = ps1.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(2));
      assertEquals(2, rs.getInt(1));
      assertEquals("asif", rs.getString(3));
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testINWithLoader() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);
    Properties props = new Properties();
    props.setProperty("log-level", getLogLevel());
    Connection conn = TestUtil.getConnection(props);
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10)) partition by column(name)");
    // check get based query when there is a loader
    GfxdCallbacksTest.addLoader(null, "t1",
        "com.pivotal.gemfirexd.jdbc.TestRowLoader", "");

    String query = "select id, name from t1 "
        + "where id IN (?, ?, ?, ?)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(4, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(4, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof PrimaryDynamicKey);
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(1, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(4, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 1000);
      ps1.setInt(4, 2000);
      ResultSet rs = ps1.executeQuery();
      assertTrue(rs.next());
      assertEquals(1000, rs.getInt(1));
      assertEquals("Mark Black", rs.getString(2));
      assertFalse(rs.next());
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertFalse(callbackInvoked[3]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testINWithMultipleConstantFields() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);
    Properties props = new Properties();
    props.setProperty("log-level", getLogLevel());
    Connection conn = TestUtil.getConnection(props);
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int primary key, "
        + "name varchar(10)) partition by column(name)");
    s.execute("Insert into  t1 values(1, 'asif')");
    // check get based query when there is a loader
    GfxdCallbacksTest.addLoader(null, "t1",
        "com.pivotal.gemfirexd.jdbc.TestRowLoader", "");

    String query = "select id, name from t1 "
        + "where id IN (1, 2, 1000, 2000)";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertFalse(sqi.isDynamic());
                assertEquals(0, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(4, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof RegionKey);
                }
                try {
                  assertTrue(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(1, numElements);
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(4, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      ResultSet rs = ps1.executeQuery();
      Set<String> hashs = new HashSet<String>();
      hashs.add("asif");
      hashs.add("Mark Black");
      Set<Integer> hashi = new HashSet<Integer>();
      hashi.add(1);
      hashi.add(1000);
      while (rs.next()) {
        assertTrue(hashi.remove(rs.getInt(1)));
        assertTrue(hashs.remove(rs.getString(2)));
      }
      assertFalse(rs.next());
      assertTrue(hashi.isEmpty());
      assertTrue(hashs.isEmpty());
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBug40413_1() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create function TestUDF(str varchar(100), startIndex integer, "
        + "endIndex integer) returns varchar(100) "
        + "parameter style java no sql language java external name "
        + "'com.pivotal.gemfirexd.functions.TestFunctions.substring'");

    s.execute("create table t1 ( id int , name varchar(10), type int, "
        + "address varchar(50), Primary Key (id )) partition by column(type)");
    s.execute("Insert into  t1 values(1,'asif',2, 'J 604')");
    s.execute("Insert into  t1 values(2,'neeraj',2, 'J 604')");
    s.execute("Insert into  t1 values(3,'shoubhik',2, 'J 604')");
    s.execute("Insert into  t1 values(4,'eric',2, 'J 604')");
    String query = "select substr(name,1,2) from t1 where  id IN (1,1)";
    GemFireXDQueryObserver old = null;
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                try {
                  assertFalse(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      Set<String> hash = new HashSet<String>();
      hash.add("as");
      while (rs.next()) {
        assertTrue(hash.remove(rs.getString(1)));
      }
      assertTrue(hash.isEmpty());
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct TestUDF(address,2,5)  from t1 "
          + "where id IN (1, 3)";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      assertTrue(rs.next());
      assertEquals("604", rs.getString(1));
      assertFalse(rs.next());
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address, id  from t1 where  id IN (1,3) "
          + "order by id  asc , address desc";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address, id  from t1 where  id IN (1,3) "
          + "group by id, address   order by id ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  distinct type, AVG(type)  from t1 where id IN (1,3) "
          + "group by type order by AVG(type) desc ";
      rs = stmt.executeQuery(query);
      rs.close();

      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  distinct type, AVG(type), MAX(type)  from t1 "
          + "where id IN (1,3) group by type order by AVG(type) desc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  address, id  from t1 where  id IN (1,3) "
          + "group by id, address  order by id";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address  from t1 where  id IN (1,3) ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  address  from t1 where  id IN (1,3)  group by address ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      // here even though it is order by, still the order by node will be
      // removed by the optmizer because it assumes data to be in sorted order
      // when using index to get index keys in the FromBaseTable
      query = "select  id  from t1 where  id IN (3,1,4,2)  order by id ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 1; i < 5; ++i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  id  from t1 where  id IN (1,3,4,2)  order by id desc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 4; i > 0; --i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  id  from t1 where  id IN (3,1,4,2)  order by id asc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 1; i < 5; ++i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBug40413_2() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create function TestUDF(str varchar(100), startIndex integer, "
        + "endIndex integer) returns varchar(100) "
        + "parameter style java no sql language java external name "
        + "'com.pivotal.gemfirexd.functions.TestFunctions.substring'");

    s.execute("create table t1 ( id int , name varchar(10), type int, "
        + "address varchar(50), Primary Key (id )) partition by column(type)");
    // check get based query when there is a loader
    GfxdCallbacksTest.addLoader(null, "t1",
        "com.pivotal.gemfirexd.jdbc.TestRowLoader", "");

    s.execute("Insert into  t1 values(1,'asif',2, 'J 604')");
    s.execute("Insert into  t1 values(2,'neeraj',2, 'J 604')");
    s.execute("Insert into  t1 values(3,'shoubhik',2, 'J 604')");
    s.execute("Insert into  t1 values(4,'eric',2, 'J 604')");
    String query = "select substr(name,1,2) from t1 where  id = 1";
    GemFireXDQueryObserver old = null;
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                try {
                  assertFalse(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      Set<String> hash = new HashSet<String>();
      hash.add("as");
      while (rs.next()) {
        assertTrue(hash.remove(rs.getString(1)));
      }
      assertTrue(hash.isEmpty());
      assertFalse(callbackInvoked[2]);
      assertFalse(callbackInvoked[3]);
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct TestUDF(address,2,5)  from t1 "
          + "where id IN (1, 3)";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      assertTrue(rs.next());
      assertEquals("604", rs.getString(1));
      assertFalse(rs.next());
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address, id  from t1 where  id IN (1,3) "
          + "order by id  asc , address desc";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address, id  from t1 where  id IN (1,3) "
          + "group by id, address   order by id ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  distinct type, AVG(type)  from t1 where id IN (1,3) "
          + "group by type order by AVG(type) desc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  distinct type, AVG(type), MAX(type)  from t1 "
          + "where id IN (1,3) group by type order by AVG(type) desc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  address, id  from t1 where  id IN (1,3) "
          + "group by id, address  order by id";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address  from t1 where  id IN (1,3) ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  address  from t1 where  id IN (1,3)  group by address ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      // here even though it is order by, still the order by node will be
      // removed by the optmizer because it assumes data to be in sorted order
      // when using index to get index keys in the FromBaseTable
      query = "select  id  from t1 where  id IN (3,1,4,2)  order by id ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 1; i < 5; ++i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  id  from t1 where  id IN (1,3,4,2)  order by id desc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 4; i > 0; --i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select  id  from t1 where  id IN (3,1,4,2)  order by id asc ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      for (int i = 1; i < 5; ++i) {
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
      rs.close();

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBug40413_3() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int , name varchar(10), type int, "
        + "address varchar(50), Primary Key (id )) partition by column(type)");
    s.execute("Insert into  t1 values(1,'asif',8, 'J 601')");
    s.execute("Insert into  t1 values(2,'neeraj',9, 'J 602')");
    s.execute("Insert into  t1 values(4,'sumedh',11, 'J 604')");
    s.execute("Insert into  t1 values(3,'shoubhik',10, 'J 603')");

    String query = "select substr(address,4,5),substr(name,1,2) from t1 where "
        + " id  IN (1,3,4) order by name desc";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                try {
                  assertFalse(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(rs.next());
      assertEquals("04", rs.getString(1));
      assertEquals("su", rs.getString(2));
      assertTrue(rs.next());
      assertEquals("03", rs.getString(1));
      assertEquals("sh", rs.getString(2));
      assertTrue(rs.next());
      assertEquals("01", rs.getString(1));
      assertEquals("as", rs.getString(2));
      assertFalse(rs.next());
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBug40413_4() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s1 = conn.createStatement();
    s1.execute("create function TestUDF(str varchar(100), startIndex integer, "
        + "endIndex integer) returns varchar(100) "
        + "parameter style java no sql language java external name "
        + "'com.pivotal.gemfirexd.functions.TestFunctions.substring'");
    Statement s = conn.createStatement();
    s.execute("create table t1 ( id int , name varchar(10), type int, "
        + "address varchar(50), Primary Key (id )) partition by column(type)");
    s.execute("create index i1 on t1 (type)");
    s.execute("Insert into  t1 values(1,'asif',3, 'J 604')");
    s.execute("Insert into  t1 values(2,'neeraj',4, 'J 604')");
    s.execute("Insert into  t1 values(3,'shoubhik',5, 'J 604')");
    String query = "select type,TestUDF(name,1,4) from t1 where id IN (1,3) "
        + "order by name desc";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                try {
                  assertFalse(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
            }
            
            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      assertTrue(callbackInvoked[1]);
      rs.next();
      assertEquals(5, rs.getInt(1));
      assertEquals("hou", rs.getString(2));
      rs.next();
      assertEquals(3, rs.getInt(1));
      assertEquals("sif", rs.getString(2));
      assertFalse(rs.next());
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct address from t1 where  id IN (1,3) ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      rs.next();
      assertEquals("J 604", rs.getString(1));
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      assertFalse(rs.next());

      callbackInvoked[0] = false;
      callbackInvoked[1] = false;
      callbackInvoked[3] = false;
      callbackInvoked[2] = false;
      query = "select distinct TestUDF(address,2,5)  from t1 "
          + "where id IN (1,3) ";
      rs = stmt.executeQuery(query);
      assertTrue(callbackInvoked[0]);
      rs.next();
      assertEquals("604", rs.getString(1));
      assertTrue(callbackInvoked[1]);
      assertTrue(callbackInvoked[2]);
      assertTrue(callbackInvoked[3]);
      assertFalse(rs.next());
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testBug48246() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema trade");
    s.execute("create table trade.customer "
        + "(c_balance int not null, c_first int not null, c_middle int not null, "
        + "c_id int primary key) " + " partition by column(c_balance)");

    { // insert values
      PreparedStatement psInsert = conn
          .prepareStatement("insert into trade.customer values (?, ?, ?, ?)");
      for (int i = 0; i < 3; i++) {
        psInsert.setInt(1, i * 1);
        psInsert.setInt(2, i * 2);
        psInsert.setInt(3, i * 3);
        psInsert.setInt(4, i * 4);
        psInsert.executeUpdate();
      }
    }

    String query = "SELECT c_balance, c_first, c_middle, c_id FROM trade.customer "
        + "WHERE c_id in (?, ?, ?) "
        + "ORDER BY c_balance, c_first, c_middle, c_id";
    final boolean[] callbackInvoked = new boolean[] { false, false, false, false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                callbackInvoked[0] = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertTrue(sqi.isPrimaryKeyBased());
                assertTrue(sqi.isDynamic());
                assertEquals(3, sqi.getParameterCount());
                Object[] pks = (Object[])sqi.getPrimaryKey();
                assertEquals(3, pks.length);
                for (int i = 0; i < pks.length; ++i) {
                  assertTrue(pks[i] instanceof PrimaryDynamicKey);
                }
                try {
                  assertFalse(sqi.createGFEActivation());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail(e.toString());
                }
              }
            }

            @Override
            public void createdGemFireXDResultSet(
                com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs) {
              if (rs instanceof GemFireResultSet) {
                callbackInvoked[1] = true;
              }
            }

            @Override
            public void getAllInvoked(int numElements) {
              callbackInvoked[3] = true;
              assertEquals(3, numElements);
            }

            @Override
            public void getAllGlobalIndexInvoked(int numElements) {
              callbackInvoked[2] = true;
              assertEquals(3, numElements);
            }
          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      for (int i = 0; i < 5; i++) {
        ps1.setInt(1, 0);
        ps1.setInt(2, 4);
        ps1.setInt(3, 8);
        ResultSet rs = ps1.executeQuery();
        int count = 0;
        while (rs.next()) {
          assertEquals(count, rs.getInt(4));
          count = count + 4;
        }
        assertEquals(12, count);
        assertTrue(callbackInvoked[0]);
        assertTrue(callbackInvoked[1]);
        assertTrue(callbackInvoked[2]);
        assertTrue(callbackInvoked[3]);
        rs.close();
      }
    }

    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Test the IN operator Performance.
   */
  public void _testINperf() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    for (int i = 0; i < 10000; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  t1 values(" + i + ",'" + s + "'," + 2
          * i + ")");
    }
    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = TestUtil.jdbcConn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 6;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }

    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?" + ",?,?,?,?,?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = TestUtil.jdbcConn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 30;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }

    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = TestUtil.jdbcConn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 60;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }
  }

  /**
   * Test the IN operator Performance.
   */
  public void _testINperf_thinClient() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG2");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table t1 ( id int primary key, "
        + "name varchar(10), type int) partition by column(type)");
    for (int i = 0; i < 10000; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  t1 values(" + i + ",'" + s + "'," + 2
          * i + ")");
    }

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);

    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 6;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }

    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?" + ",?,?,?,?,?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 30;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }

    {
      String query = "select type, id, name from t1 where id IN (?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?"
          + ",?,?,?,?,?,?,?,?,?,?)";
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      PreparedStatement ps1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      int times = 100;
      int insize = 60;
      for (int i = 0; i < times; i++) {
        for (int j = 0; j < insize; j++) {
          ps1.setInt(j + 1, i + j);
        }
        ResultSet rs = ps1.executeQuery();
        for (int j = 0; j < insize; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestINperf: " + "for executing " + query + " " + times
              + " times, with in list size " + insize + " time taken was "
              + diff + " ns");
    }
  }
}
