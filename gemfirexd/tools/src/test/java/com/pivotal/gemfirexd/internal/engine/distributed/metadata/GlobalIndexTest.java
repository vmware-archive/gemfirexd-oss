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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;


import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireSelectActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * Tests nodes pruning using Global Index
 * @author Asif
 *
 */
public class GlobalIndexTest extends JdbcTestBase
{
  private boolean callbackInvoked = false;
  public GlobalIndexTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(GlobalIndexTest.class));
  }

  @Override
  public void tearDown() throws Exception {
    this.callbackInvoked = false;
    super.tearDown();
  }

  public void testGlobalIndexCaching_selectToGet() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
      Properties props1 = new Properties();
      int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props1.put("mcast-port", String.valueOf(mport));
      Connection conn = TestUtil.getConnection(props1);
      Statement s = conn.createStatement();
      s.execute("create table TESTTABLE (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
          + " partition by column(address)");

      String query = "select ID, DESCRIPTION from TESTTABLE where ID = 1";
      final LanguageConnectionContext lcc = ((EmbedConnection)conn)
          .getLanguageConnection();

      final Object[] robjsArr = new Object[2];
      final Object[] gIdkKeys = new Object[2];
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void afterPuttingInCached(Serializable globalIndexKey,
                Object result) {
              robjsArr[0] = result;
              gIdkKeys[0] = globalIndexKey;
            }

            public void beforeReturningCachedVal(Serializable globalIndexKey,
                Object cachedVal) {
              robjsArr[1] = cachedVal;
              gIdkKeys[1] = globalIndexKey;
            }
          });

      // Insert values 1 to 7
      for (int i = 0; i < 1; ++i) {
        s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
            + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      try {
        ResultSet rs = s.executeQuery(query);
        assertTrue(rs.next());
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertNotNull(robjsArr[0]);
      assertNotNull(gIdkKeys[0]);
      assertEquals(robjsArr[0], robjsArr[1]);
      assertEquals(gIdkKeys[0], gIdkKeys[1]);
      System.out.println("robjsArr[0]=" + robjsArr[0]);
      System.out.println("gIdkKeys[0]=" + gIdkKeys[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }

  public void testGlobalIndexCaching_selectToGetPS() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
      Properties props1 = new Properties();
      int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props1.put("mcast-port", String.valueOf(mport));
      Connection conn = TestUtil.getConnection(props1);
      Statement s = conn.createStatement();
      s.execute("create table TESTTABLE (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
          + " partition by column(address)");

      String query = "select ID, DESCRIPTION from TESTTABLE where ID = ?";
      final LanguageConnectionContext lcc = ((EmbedConnection)conn)
          .getLanguageConnection();

      final Object[] robjsArr = new Object[2];
      final Object[] gIdkKeys = new Object[2];
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void afterPuttingInCached(Serializable globalIndexKey,
                Object result) {
              robjsArr[0] = result;
              gIdkKeys[0] = globalIndexKey;
            }

            public void beforeReturningCachedVal(Serializable globalIndexKey,
                Object cachedVal) {
              robjsArr[1] = cachedVal;
              gIdkKeys[1] = globalIndexKey;
            }
          });

      // Insert values 1 to 7
      for (int i = 0; i < 1; ++i) {
        s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
            + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      try {
        PreparedStatement ps = conn.prepareStatement(query);
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertNotNull(robjsArr[0]);
      assertNotNull(gIdkKeys[0]);
      assertEquals(robjsArr[0], robjsArr[1]);
      assertEquals(gIdkKeys[0], gIdkKeys[1]);
      System.out.println("robjsArr[0]=" + robjsArr[0]);
      System.out.println("gIdkKeys[0]=" + gIdkKeys[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }
  
  @SuppressWarnings("serial")
  public void testGlobalIndexCaching_selectToFuncExn() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
      Properties props1 = new Properties();
      int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props1.put("mcast-port", String.valueOf(mport));
      Connection conn = TestUtil.getConnection(props1);
      Statement s = conn.createStatement();
      s.execute("create table TESTTABLE (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
          + " partition by column(address)");

      String query = "select ID, DESCRIPTION from TESTTABLE where ID = 1 and DESCRIPTION = 'First1'";

      final Object[] robjsArr = new Object[2];
      final Object[] gIdkKeys = new Object[2];

      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void afterPuttingInCached(Serializable globalIndexKey,
                Object result) {
              robjsArr[0] = result;
              gIdkKeys[0] = globalIndexKey;
            }

            public void beforeReturningCachedVal(Serializable globalIndexKey,
                Object cachedVal) {
              robjsArr[1] = cachedVal;
              gIdkKeys[1] = globalIndexKey;
            }
          });

      // Insert values 1 to 7
      for (int i = 0; i < 1; ++i) {
        s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
            + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      try {
        ResultSet rs = s.executeQuery(query);
        assertTrue(rs.next());
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertNotNull(robjsArr[0]);
      assertNotNull(gIdkKeys[0]);
      assertEquals(robjsArr[0], robjsArr[1]);
      assertEquals(gIdkKeys[0], gIdkKeys[1]);
      System.out.println("robjsArr[0]=" + robjsArr[0]);
      System.out.println("gIdkKeys[0]=" + gIdkKeys[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }

  @SuppressWarnings("serial")
  public void testGlobalIndexCaching_updateToFuncExn() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
      Properties props1 = new Properties();
      int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props1.put("mcast-port", String.valueOf(mport));
      Connection conn = TestUtil.getConnection(props1);
      Statement s = conn.createStatement();
      s.execute("create table TESTTABLE (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
          + " partition by column(address)");

      String query = "update TESTTABLE set description = 'somed' where ID = 1 and DESCRIPTION = 'First1'";
      final Object[] robjsArr = new Object[2];
      final Object[] gIdkKeys = new Object[2];
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void afterPuttingInCached(Serializable globalIndexKey,
                Object result) {
              robjsArr[0] = result;
              gIdkKeys[0] = globalIndexKey;
            }

            public void beforeReturningCachedVal(Serializable globalIndexKey,
                Object cachedVal) {
              robjsArr[1] = cachedVal;
              gIdkKeys[1] = globalIndexKey;
            }
          });

      // Insert values 1 to 7
      for (int i = 0; i < 1; ++i) {
        s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
            + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      try {
        int cnt = s.executeUpdate(query);
        assertEquals(1, cnt);
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertNotNull(robjsArr[0]);
      assertNotNull(gIdkKeys[0]);
      assertEquals(robjsArr[0], robjsArr[1]);
      assertEquals(gIdkKeys[0], gIdkKeys[1]);
      System.out.println("robjsArr[0]=" + robjsArr[0]);
      System.out.println("gIdkKeys[0]=" + gIdkKeys[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }

  @SuppressWarnings("serial")
  public void testGlobalIndexCaching_deleteToFuncExn() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
      Properties props1 = new Properties();
      int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props1.put("mcast-port", String.valueOf(mport));
      Connection conn = TestUtil.getConnection(props1);
      Statement s = conn.createStatement();
      s.execute("create table TESTTABLE (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
          + " partition by column(address)");

      String query = "delete from TESTTABLE where ID = 1 and DESCRIPTION = 'First1'";

      final Object[] robjsArr = new Object[2];
      final Object[] gIdkKeys = new Object[2];

      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void afterPuttingInCached(Serializable globalIndexKey,
                Object result) {
              robjsArr[0] = result;
              gIdkKeys[0] = globalIndexKey;
            }

            public void beforeReturningCachedVal(Serializable globalIndexKey,
                Object cachedVal) {
              robjsArr[1] = cachedVal;
              gIdkKeys[1] = globalIndexKey;
            }
          });

      // Insert values 1 to 7
      for (int i = 0; i < 1; ++i) {
        s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
            + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      try {
        int cnt = s.executeUpdate(query);
        assertEquals(1, cnt);
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertNotNull(robjsArr[0]);
      assertNotNull(gIdkKeys[0]);
      assertEquals(robjsArr[0], robjsArr[1]);
      assertEquals(gIdkKeys[0], gIdkKeys[1]);
      System.out.println("robjsArr[0]=" + robjsArr[0]);
      System.out.println("gIdkKeys[0]=" + gIdkKeys[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }
  
  public void testNodesPruningUsingGlobalIndex_2() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID), unique( DESCRIPTION,ADDRESS))");
    
    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2' AND DESCRIPTION = 'First2'";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" +(i+1)+"')");
    }
    final LanguageConnectionContext lcc= ((EmbedConnection)conn).getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                GlobalIndexTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                Set <Object> actualRoutingKeys = new HashSet<Object>(); 
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  Activation act = new GemFireSelectActivation(gps, lcc,
                      (DMLQueryInfo)qInfo, null, false);
                  ((BaseActivation)act).initFromContext(lcc, true, gps);
                  sqi.computeNodes(actualRoutingKeys,act, false);
                  assertEquals(actualRoutingKeys.size(),1);
                  assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));
                }catch(Exception e) {
                  e.printStackTrace();
                  fail("test failed because of exception="+e.toString());
                }
              }

            }     

          });

      try {
        s.executeQuery(query);
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
      assertTrue(this.callbackInvoked);      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }
  }
  
  public void testNodesPruningUsingGlobalIndex_3() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))");
    s.execute("CREATE  GLOBAL HASH  INDEX address_description_global ON TESTTABLE ( ADDRESS, DESCRIPTION)");
    
    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2' AND DESCRIPTION = 'First2'";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" +(i+1)+"')");
    }
    final LanguageConnectionContext lcc= ((EmbedConnection)conn).getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                GlobalIndexTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                Set <Object> actualRoutingKeys = new HashSet<Object>(); 
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  Activation act = new GemFireSelectActivation(gps, lcc,
                      (DMLQueryInfo)qInfo, null, false);
                  ((BaseActivation)act).initFromContext(lcc, true, gps);
                  sqi.computeNodes(actualRoutingKeys,act, false);
                  assertEquals(actualRoutingKeys.size(),1);
                  assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));
                  
                }catch(Exception e) {
                  e.printStackTrace();
                  fail("test failed because of exception="+e.toString());
                }
              }

            }     

          });

      try {
        s.executeQuery(query);
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }
  }
  
  public void testNodesPruningUsingGlobalIndex_1() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID), unique(ADDRESS))");
    
    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2'";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" +(i+1)+"')");
    }
    final LanguageConnectionContext lcc= ((EmbedConnection)conn).getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                GlobalIndexTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                Set <Object> actualRoutingKeys = new HashSet<Object>(); 
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  Activation act = new GemFireSelectActivation(gps, lcc,
                      (DMLQueryInfo)qInfo, null, false);
                  ((BaseActivation)act).initFromContext(lcc, true, gps);
                  sqi.computeNodes(actualRoutingKeys,act, false);
                  assertEquals(actualRoutingKeys.size(),1);
                  assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));
                }catch(Exception e) {
                  e.printStackTrace();
                  fail("test failed because of exception="+e.toString());
                }
              }

            }     

          });

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }
  }

  public void testNodesPruningUsingGlobalIndex_4_Bug39956() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ID_1 int not null, primary key (ID), unique(ID_1))");
    
    String query = "select ID, DESCRIPTION from TESTTABLE where ID_1  >= 3";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "',"+ (i+1)+")");
    }
    final LanguageConnectionContext lcc= ((EmbedConnection)conn).getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                GlobalIndexTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                Set<Object> actualRoutingKeys = new HashSet<Object>();
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  Activation act = new GemFireSelectActivation(gps, lcc,
                      (DMLQueryInfo)qInfo, null, false);
                  ((BaseActivation)act).initFromContext(lcc, true, gps);
                  sqi.computeNodes(actualRoutingKeys, act, false);
                  assertEquals(actualRoutingKeys.size(), 1);
                  assertTrue(actualRoutingKeys
                      .contains(ResolverUtils.TOK_ALL_NODES));
                } catch (Exception e) {
                  e.printStackTrace();
                  fail("test failed because of exception=" + e.toString());
                }
              }
            }
          });
      try {
        s.executeQuery(query);
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
  }
}
