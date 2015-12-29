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

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.AbstractConditionQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.AndJunctionQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ComparisonQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ConstantConditionsWrapperQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DynamicKey;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.InQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.JunctionQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.OrJunctionQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ParameterizedConditionsWrapperQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RangeQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ValueQueryInfo;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.engine.sql.execute.ConstantValueSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.ConstantValueSetImpl;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireSelectActivation;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeRegionKey;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.compile.RelationalOperator;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

public class StmtCachingEnabledExistingTest extends JdbcTestBase {
  public StmtCachingEnabledExistingTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(StmtCachingEnabledExistingTest.class));
  }

  private boolean callbackInvoked = false;
  
  //SelectQueryInfoInternalTest
  public void testWhereClauseWith3CompositeKey() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);

    Connection conn = getConnection();
    SelectQueryInfoInternalsTest.createTableWithThreeCompositeKey(conn);    
    String[] queries = new String[] { "Select * from orders where vol = 9  and id = 8 and  cust_name = 'asif' "};
    final boolean[] isGetConvertible = new boolean[] {true};
    final boolean[] usedGemfireActivation = new boolean[] {false};
    //Remember to captilize the name.
    final Region<?, ?> tableRegion = Misc.getRegionForTable(
        getCurrentDefaultSchemaName() + ".orders".toUpperCase(), true);
    final Object[] pks = new Object[] { new CompactCompositeRegionKey(
        new DataValueDescriptor[] { new SQLInteger(8), new SQLVarchar("asif"),
            new SQLInteger(9) }, ((GemFireContainer)tableRegion
            .getUserAttribute()).getExtraTableInfo()) };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private int index = 0;

            @Override
            public void beforeGemFireResultSetExecuteOnActivation(
                AbstractGemFireActivation activation) {
              usedGemfireActivation[0] = true;
            }
            
            private DynamicKey dk = null;
            private QueryInfo qi;

            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              StmtCachingEnabledExistingTest.this.callbackInvoked = true;              
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                AbstractConditionQueryInfo acqi = sqi.getWhereClause();
               // assertTrue(acqi instanceof JunctionQueryInfo);
                JunctionQueryInfo jqi = (JunctionQueryInfo) acqi;
                assertEquals(jqi.getJunctionType(), QueryInfoConstants.AND_JUNCTION);
                assertTrue(jqi instanceof AndJunctionQueryInfo);
                try {
                  assertTrue(qInfo.isPrimaryKeyBased() == isGetConvertible[index]);
                  if(qInfo.isPrimaryKeyBased()) {
                    dk = (DynamicKey)qInfo.getPrimaryKey();
                  }
                  qi = qInfo;
                }
                catch (StandardException e) {
                 fail("Test failed because of exception="+e);
                }
            }
            
            @Override
            public void beforeQueryExecution(EmbedStatement stmt, Activation activation) throws SQLException {
              try {
                assertTrue(dk != null);
                
                RegionKey actual = dk.getEvaluatedPrimaryKey(activation,
                    (GemFireContainer)qi.getRegion().getUserAttribute(), false);
                RegionKey expected= (RegionKey)pks[index];
                assertEquals(actual,expected);
              } catch (StandardException e) {
                fail("Test failed because of exception="+e);
              }
              ++index;              
            }

          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement s = conn.createStatement();
      for (int i = 0; i < queries.length; ++i) {
        try {
          s.executeQuery(queries[i]);
          assertTrue(usedGemfireActivation[i]);
        }catch (SQLException e) {
          throw new SQLException(e.toString() + " Exception in executing query = "+queries[i],e.getSQLState());
        }
      }
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
    
  }

  //SelectQueryInfoInternalTest
  public void testNodesPruningUsingPartitionByExpression_Bug40857_2()
      throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    try {
      Connection conn = getConnection();
      Statement s = conn.createStatement();
      s
          .execute("create table TESTTABLE (a int not null, "
              + " DESCRIPTION varchar(1024) not null, b int not null, c int not null,d int not null, primary key (a)) Partition by ( ((c  + b)*a)/d )");

      String queries[] = new String[] {
          "select * from TESTTABLE where   ((c  + b)*a )/d = 5",
          "select * from TESTTABLE where a = 7 and  b = 8 and  c = 1 and d = 3",
          "select * from TESTTABLE where  ((c  + b)*a)/d = 5 and Description = 'abc'",
          "select * from TESTTABLE where  (c  + b)*a/d = 5 and Description = 'abc'",
          "select * from TESTTABLE where  (c  + b)*a/d = 5 and a = 7 and b =8  and c=1 and d =3  and Description = 'abc'",
          "select * from TESTTABLE where  (c  + b)*a/d = 5 OR ( a = 7 and b =8  and c=1 and d =3 ) and Description = 'abc'",
          "select * from TESTTABLE x where  (x.c  + x.b)*x.a/d = 5 OR ( a = 7 and b =8  and c=1 and d =3 ) and Description = 'abc'",
          "select * from TESTTABLE x where  (x.c  + x.b)*x.a/d = 5 OR ( a IN (7,5) and b IN (8,7)  and c=1 and d =3 ) and Description = 'abc'" /*,
                                                                                                                                               "select * from TESTTABLE where  (b  + c)*a/d = 5 and Description = 'abc'"*/
      };
      Object expectedRoutingKeys[] = new Object[] {
          new Integer(5),
          new Integer(21),
          new Integer(5),
          new Integer(5) /*,new Integer(5)*/,
          new Object[] {/*new Integer(5),*/new Integer(21) },
          new Object[] { new Integer(5), new Integer(21) },
          new Object[] { new Integer(5), new Integer(21) },
          new Object[] { new Integer(5), new Integer(21), new Integer(15),
              new Integer(13), new Integer(18) } };
      final LanguageConnectionContext lcc = ((EmbedConnection)conn)
          .getLanguageConnection();
      for (int i = 0; i < queries.length; ++i) {

        final Object expectedRoutingKey = expectedRoutingKeys[i];
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              SelectQueryInfo _sqi = null;
              GenericPreparedStatement _gps = null;
              @Override
              public void queryInfoObjectAfterPreparedStatementCompletion(
                  QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

                assertTrue(qInfo instanceof SelectQueryInfo);
                _sqi = (SelectQueryInfo)qInfo;
                _gps = gps;
              }

              @Override
              public void beforeQueryExecution(EmbedStatement stmt,
                  Activation activation) {
                if (_sqi == null) {
                  return;
                }
                
                  StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                  Set<Object> actualRoutingKeys = new HashSet<Object>();
                  actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                  try {
                    //[sb] handing over the incoming activations constantValueSet 
                    //to the below created one for test purposes. Ideally only one
                    //kind of activation will be created.
                    lcc.setConstantValueSet(null, activation.getParameterValueSet());
                    
                    BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                        _sqi, null, false);
                    act.initFromContext(lcc, true, _gps);
                    act.setupActivation(_gps, false, null);                    
                    _sqi.computeNodes(actualRoutingKeys, act, false);
                    if (expectedRoutingKey instanceof Object[]) {
                      Object[] keys = (Object[])expectedRoutingKey;
                      for (Object key : keys) {
                        assertTrue(actualRoutingKeys.remove(key));
                      }
                      assertTrue(actualRoutingKeys.isEmpty());
                    }
                    else {
                      assertTrue(actualRoutingKeys.remove(expectedRoutingKey));
                      assertTrue(actualRoutingKeys.isEmpty());
                    }

                  } catch (Exception e) {
                    e.printStackTrace(SanityManager.GET_DEBUG_STREAM());
                    fail("test failed because of exception=" + e.toString());
                  }
                }

              });

        s.executeQuery(queries[i]);
        assertTrue(this.callbackInvoked);
        this.callbackInvoked = false;
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
    }
  }

  //SelectQueryInfoInternalTest
  public void testNodesPruningUsingPartitionByExpression_Bug40857_1()
      throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    try {
      Connection conn = getConnection();
      Statement s = conn.createStatement();
      s
          .execute("create table TESTTABLE (a int not null, "
              + " DESCRIPTION varchar(1024) not null, b int not null, c int not null,d int not null, primary key (a)) Partition by  (c * a + b)");

      String queries[] = new String[] {
          "select * from TESTTABLE where c * a = 3",
          "select * from TESTTABLE where  (c * a + b) = 5",
          "select * from TESTTABLE where a = 7 and  b = 8 and  c = 1 and d = 3",
          "select * from TESTTABLE where  (c *a + b) = 5 and Description = 'abc'" };
      Object expectedRoutingKeys[] = new Object[] {
          ResolverUtils.TOK_ALL_NODES, new Integer(5), new Integer(15),
          new Integer(5) };
      final LanguageConnectionContext lcc = ((EmbedConnection)conn)
          .getLanguageConnection();
      for (int i = 0; i < queries.length; ++i) {

        final Object expectedRoutingKey = expectedRoutingKeys[i];
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              SelectQueryInfo _sqi = null;
              GenericPreparedStatement _gps = null;
              @Override
              public void queryInfoObjectAfterPreparedStatementCompletion(
                  QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

                assertTrue(qInfo instanceof SelectQueryInfo);
                _sqi = (SelectQueryInfo)qInfo;
                _gps = gps;
              }

              @Override
              public void beforeQueryExecution(EmbedStatement stmt,
                  Activation activation) {
                if (_sqi == null) {
                  return;
                }
                  StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                  Set<Object> actualRoutingKeys = new HashSet<Object>();
                  actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                  try {
                    //[sb] handing over the incoming activations constantValueSet 
                    //to the below created one for test purposes. Ideally only one
                    //kind of activation will be created.
                    lcc.setConstantValueSet(null, activation.getParameterValueSet());
                    BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                        _sqi, null, false);
                    act.initFromContext(lcc, true, _gps);
                    act.setupActivation(_gps, false, null);                    
                    _sqi.computeNodes(actualRoutingKeys, act, false);
                    if (expectedRoutingKey instanceof Object[]) {
                      Object[] keys = (Object[])expectedRoutingKey;
                      for (Object key : keys) {
                        assertTrue(actualRoutingKeys.remove(key));
                      }
                      assertTrue(actualRoutingKeys.isEmpty());
                    }
                    else {
                      assertTrue(actualRoutingKeys.remove(expectedRoutingKey));
                      assertTrue(actualRoutingKeys.isEmpty());
                    }

                  } catch (Exception e) {
                    e.printStackTrace();
                    fail("test failed because of exception=" + e.toString());
                  }
                }

            });

        s.executeQuery(queries[i]);
        assertTrue(this.callbackInvoked);
        this.callbackInvoked = false;
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
    }
  }

  //SelectQueryInfoInternalTest
  public void testBug41617_1_single_column_global_index_pruning_IN()
      throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table sellorders (oid int not null constraint orders_pk primary key"
            + ", cid int, sid int, tid int) partition by column (cid )");

    String query = "select * from  sellorders where oid IN (1, 2, 3, 4, 5) and tid < 41 ";
    String insertquery = "insert into sellorders values(?,?,?,?)";
    PreparedStatement ps = conn.prepareStatement(insertquery);
    // Insert values 1 to 7
    for (int i = 1; i < 11; ++i) {
      ps.setInt(1, i);
      ps.setInt(2, 2 * i);
      ps.setInt(3, 3 * i);
      ps.setInt(4, 4 * i);
      ps.executeUpdate();

    }
    final LanguageConnectionContext lcc = ((EmbedConnection)conn)
        .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            SelectQueryInfo _sqi = null;
            GenericPreparedStatement _gps = null;
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) {
                if (_sqi == null) {
                  return;
                }

                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                Set<Object> actualRoutingKeys = new HashSet<Object>();
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  //[sb] handing over the incoming activations constantValueSet 
                  //to the below created one for test purposes. Ideally only one
                  //kind of activation will be created.
                  lcc.setConstantValueSet(null, activation.getParameterValueSet());
                  BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                      _sqi, null, false);
                  act.initFromContext(lcc, true, _gps);
                  act.setupActivation(_gps, false, null);                    
                  _sqi.computeNodes(actualRoutingKeys, act, false);
                  // Externally compute the routing objects.
                  Set<Object> externallyCmputed = new HashSet<Object>();

                  GfxdPartitionResolver spr = (GfxdPartitionResolver)((PartitionedRegion)_sqi
                      .getRegion()).getPartitionResolver();
                  Object[] routingObjs = spr
                      .getRoutingObjectsForList(new DataValueDescriptor[] {
                          new SQLInteger(2), new SQLInteger(4),
                          new SQLInteger(6), new SQLInteger(8),
                          new SQLInteger(10) });

                  for (Object obj : routingObjs) {
                    externallyCmputed.add(obj);
                  }

                  assertFalse(actualRoutingKeys
                      .contains(ResolverUtils.TOK_ALL_NODES));
                  assertEquals(externallyCmputed.size(), actualRoutingKeys
                      .size());
                  assertTrue(externallyCmputed.removeAll(actualRoutingKeys));
                  assertTrue(externallyCmputed.isEmpty());
                } catch (Throwable e) {
                  e.printStackTrace();
                  fail("test failed because of exception=" + e.toString());
                }
              }
          });

      try {
        s.executeQuery(query);
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }

  }
  
  //GlobalIndexTest
  public void testNodesPruningUsingGlobalIndex_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID), unique( DESCRIPTION,ADDRESS))");

    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2' AND DESCRIPTION = 'First2'";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
          + (i + 1) + "', 'abc" + (i + 1) + "')");
    }
    final LanguageConnectionContext lcc = ((EmbedConnection)conn)
        .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            SelectQueryInfo _sqi = null;

            GenericPreparedStatement _gps = null;

            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) {
              if (_sqi == null) {
                return;
              }
              StmtCachingEnabledExistingTest.this.callbackInvoked = true;
              Set<Object> actualRoutingKeys = new HashSet<Object>();
              actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
              try {
                //[sb] handing over the incoming activations constantValueSet 
                //to the below created one for test purposes. Ideally only one
                //kind of activation will be created.
                lcc.setConstantValueSet(null, activation.getParameterValueSet());
                BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                    _sqi, null, false);
                act.initFromContext(lcc, true, _gps);
                act.setupActivation(_gps, false, null);                    
                _sqi.computeNodes(actualRoutingKeys, act, false);
                assertEquals(actualRoutingKeys.size(), 1);
                assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));
              } catch (Exception e) {
                e.printStackTrace();
                fail("test failed because of exception=" + e.toString());
              }

            }

          });

      try {
        s.executeQuery(query);
      } catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  //GlobalIndexTest
  public void testNodesPruningUsingGlobalIndex_3() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
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
            SelectQueryInfo _sqi = null;
            GenericPreparedStatement _gps = null;
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }
            
            @Override
            public void beforeQueryExecution(EmbedStatement stmt, Activation activation) {
              if(_sqi == null) {
                return;
              }

              StmtCachingEnabledExistingTest.this.callbackInvoked = true;
              Set<Object> actualRoutingKeys = new HashSet<Object>();
              actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
              try {
                //[sb] handing over the incoming activations constantValueSet 
                //to the below created one for test purposes. Ideally only one
                //kind of activation will be created.
                lcc.setConstantValueSet(null, activation.getParameterValueSet());
                BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                    _sqi, null, false);
                act.initFromContext(lcc, true, _gps);
                act.setupActivation(_gps, false, null);                    
                _sqi.computeNodes(actualRoutingKeys, act, false);
                assertEquals(actualRoutingKeys.size(), 1);
                assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));

              } catch (Exception e) {
                e.printStackTrace();
                fail("test failed because of exception=" + e.toString());
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
  }

  //GlobalIndexTest
  public void testNodesPruningUsingGlobalIndex_1() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
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
            SelectQueryInfo _sqi = null;
            GenericPreparedStatement _gps = null;
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) {
              if (_sqi == null) {
                return;
              }

              StmtCachingEnabledExistingTest.this.callbackInvoked = true;
              Set<Object> actualRoutingKeys = new HashSet<Object>();
              actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
              try {
                //[sb] handing over the incoming activations constantValueSet 
                //to the below created one for test purposes. Ideally only one
                //kind of activation will be created.
                lcc.setConstantValueSet(null, activation.getParameterValueSet());
                BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                    _sqi, null, false);
                act.initFromContext(lcc, true, _gps);
                act.setupActivation(_gps, false, null);                    
                _sqi.computeNodes(actualRoutingKeys, act, false);
                assertEquals(actualRoutingKeys.size(), 1);
                assertTrue(actualRoutingKeys.contains(Integer.valueOf(2)));
              } catch (Exception e) {
                e.printStackTrace();
                fail("test failed because of exception=" + e.toString());
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
    }
  }

  //GlobalIndexTest
  public void testNodesPruningUsingGlobalIndex_4_Bug39956() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
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
            SelectQueryInfo _sqi = null;
            GenericPreparedStatement _gps = null;
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) {
              if (_sqi == null) {
                return;
              }

              StmtCachingEnabledExistingTest.this.callbackInvoked = true;
              Set<Object> actualRoutingKeys = new HashSet<Object>();
              actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
              try {
                //[sb] handing over the incoming activations constantValueSet 
                //to the below created one for test purposes. Ideally only one
                //kind of activation will be created.
                lcc.setConstantValueSet(null, activation.getParameterValueSet());
                Activation act = new GemFireSelectActivation(_gps, lcc,
                    _sqi, null, false);
                ((BaseActivation)act).initFromContext(lcc, true, _gps);
                _sqi.computeNodes(actualRoutingKeys, act, false);
                assertEquals(actualRoutingKeys.size(), 1);
                assertTrue(actualRoutingKeys
                    .contains(ResolverUtils.TOK_ALL_NODES));
              } catch (Exception e) {
                e.printStackTrace();
                fail("test failed because of exception=" + e.toString());
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
  }
  
  //SelectQueryInfoInternalsTest
  public void testSubqueryDerbyActivationDerbyResultset() throws Exception  
  {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    final String subquery = "Select SUM(ID2) from Testtable2 where description2 = 'desc2_'";
    String query = "select ID1, DESCRIPTION1 from TESTTABLE1 where ID1 IN ("
      + subquery+ ")";
    
    s
        .execute("create table TESTTABLE1 (ID1 int not null, "
            + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))"
            + "PARTITION BY COLUMN ( ID1 )");
    for(int i = 35 ;i <56 ; ++i) {
      s.execute("Insert into  TESTTABLE1 values("+i+",'desc1_"+i+"', 'ADD_1"+ i+"')");
    }
    
    s
        .execute("create table TESTTABLE2 (ID2 int not null, "
            + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, " +
                        "primary key (ID2)) partition by column( ID2)  colocate with (TESTTABLE1) ");
    for(int i = 1;i <11; ++i) {
      s.execute("Insert into  TESTTABLE2 values("+i+",'desc2_"+"', 'ADD_2"+ i+"')");
    }
    final boolean secondaryCallback[] = new boolean[] { false };
    try {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter() {  
        @Override
        public void independentSubqueryResultsetFetched(Activation activation,
            com.pivotal.gemfirexd.internal.iapi.sql.ResultSet results) {
          assertFalse(activation instanceof AbstractGemFireActivation);    
         callbackInvoked = true;
        }
        
        @Override
        public void subqueryNodeProcessedData(SelectQueryInfo qInfo,
            GenericPreparedStatement gps,  String subqry,
            List<Integer> paramPositions)
        {
          assertEquals(subquery,subqry);
          secondaryCallback[0] = true;
        }
      });

      ResultSet rs = ((EmbedStatement)s).executeQueryByPassQueryInfo(query,
          true, true, 0, null);
      assertTrue(rs.next());
      assertEquals(rs.getInt(1),55);
      assertEquals(rs.getString(2),"desc1_55");
      assertFalse(rs.next());
      assertTrue(this.callbackInvoked);
      assertTrue(secondaryCallback[0]);
      
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
    }
  }

  //SelectQueryInfoInternalsTest
  public void testMultiColumnResolverNodesPruningForMultipleINClause()
      throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ID_1 int not null, primary key (ID)) Partition by Column(ID, ID_1)");

    String query = "select ID, DESCRIPTION from TESTTABLE where ID_1  IN (5003,9837,1000101) AND ID IN (76542,138371,91332783)";
    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      s.execute("insert into TESTTABLE values (" + (i + 1) + ", 'First"
          + (i + 1) + "'," + (i + 1) + ")");
    }
    final LanguageConnectionContext lcc = ((EmbedConnection)conn)
        .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            SelectQueryInfo _sqi = null;
            GenericPreparedStatement _gps = null;
            @Override
            public void queryInfoObjectAfterPreparedStatementCompletion(
                QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              assertTrue(qInfo instanceof SelectQueryInfo);
              _sqi = (SelectQueryInfo)qInfo;
              _gps = gps;
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) {
              if (_sqi == null) {
                return;
              }
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                Set<Object> actualRoutingKeys = new HashSet<Object>();
                actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
                try {
                  //[sb] handing over the incoming activations constantValueSet 
                  //to the below created one for test purposes. Ideally only one
                  //kind of activation will be created.
                  lcc.setConstantValueSet(null, activation.getParameterValueSet());
                  BaseActivation act = new GemFireSelectActivation(_gps, lcc,
                      _sqi, null, false);
                  act.initFromContext(lcc, true, _gps);
                  act.setupActivation(_gps, false, null);                    
                  _sqi.computeNodes(actualRoutingKeys, act, false);
                  // Externally compute the routing objects.
                  Set<Object> externallyCmputed = new HashSet<Object>();
                  GfxdPartitionResolver spr = (GfxdPartitionResolver)((PartitionedRegion)_sqi
                      .getRegion()).getPartitionResolver();
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(76542), new SQLInteger(5003) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(138371), new SQLInteger(5003) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(91332783), new SQLInteger(5003) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(76542), new SQLInteger(9837) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(138371), new SQLInteger(9837) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(91332783), new SQLInteger(9837) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(76542), new SQLInteger(1000101) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(138371), new SQLInteger(1000101) }));
                  externallyCmputed
                      .add(spr
                          .getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {
                              new SQLInteger(91332783), new SQLInteger(1000101) }));
                  assertFalse(actualRoutingKeys
                      .contains(ResolverUtils.TOK_ALL_NODES));
                  assertEquals(externallyCmputed.size(), actualRoutingKeys
                      .size());
                  assertTrue(externallyCmputed.removeAll(actualRoutingKeys));
                  assertTrue(externallyCmputed.isEmpty());
                } catch (Exception e) {
                  e.printStackTrace();
                  fail("test failed because of exception=" + e.toString());
                }
            }

          });

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testRangeQueryInfoGeneration_1() throws Exception
  {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1  AND ID < 8";
    ((EmbedConnection)jdbcConn)
        .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                           
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.groupParameterizedConditions(activation, false);
                assertEquals(rqi.getLowerBound(), new SQLInteger(1));
                assertEquals(rqi.getUpperBound(), new SQLInteger(8));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_THAN_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_THAN_RELOP);
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testRangeQueryInfoGeneration_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1  AND ID < 8 AND ID >=4";
    ((EmbedConnection)jdbcConn)
    .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;                
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                           
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;

                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.groupParameterizedConditions(activation, false);
                assertEquals(rqi.getLowerBound(), new SQLInteger(4));
                assertEquals(rqi.getUpperBound(), new SQLInteger(8));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_THAN_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testRangeQueryInfoGeneration_3() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null  , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1  AND ID < 8 AND ID >=4  AND  ID <=7";
    ((EmbedConnection)jdbcConn)
    .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;                  
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;

                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.groupParameterizedConditions(activation, false);
                assertEquals(rqi.getLowerBound(), new SQLInteger(4));
                assertEquals(rqi.getUpperBound(), new SQLInteger(7));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_EQUALS_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }

  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testRangeQueryInfoGeneration_4() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1  AND ID < 8 AND ID >=4  AND  ID <=7 AND ID >12";
    ((EmbedConnection)jdbcConn)
    .getLanguageConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;

                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.groupParameterizedConditions(activation, false);
                assertEquals(ccwqi.getOperands().size(),2);
                ComparisonQueryInfo cqi=null ;
                RangeQueryInfo rqi = null;
                AbstractConditionQueryInfo acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(0);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqi = (ComparisonQueryInfo)acqi;
                }
                
                acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(1);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqi = (ComparisonQueryInfo)acqi;
                }
                assertNotNull(rqi);
                assertNotNull(cqi);
                assertEquals(rqi.getLowerBound(), new SQLInteger(4));
                assertEquals(rqi.getUpperBound(), new SQLInteger(7));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_EQUALS_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                assertEquals(cqi.getRelationalOperator(), RelationalOperator.GREATER_THAN_RELOP);               
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testRangeQueryInfoGeneration_5() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1  AND ID < 8 AND ID >=4  AND  ID <=7 AND ID < -2 AND ID > 12";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;

                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.groupParameterizedConditions(activation, false);
                assertEquals(ccwqi.getOperands().size(),3);
                List<ComparisonQueryInfo> cqis= new ArrayList<ComparisonQueryInfo>();
                RangeQueryInfo rqi = null;
                AbstractConditionQueryInfo acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(0);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqis.add((ComparisonQueryInfo)acqi);
                }
                
                acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(1);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqis.add((ComparisonQueryInfo)acqi);
                }
                
                acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(2);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqis.add((ComparisonQueryInfo)acqi);
                }
                assertNotNull(rqi);
                assertEquals(2,cqis.size());
                assertEquals(rqi.getLowerBound(), new SQLInteger(4));
                assertEquals(rqi.getUpperBound(), new SQLInteger(7));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_EQUALS_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                          
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testMultipleRangeQueryInfoGeneration_1() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null  , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID >= 1  AND ID >=9  AND  ID <= 11 AND ID <= 8";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {

            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);                
                assertEquals(ccwqi.getOperands().size(),2);
                ComparisonQueryInfo cqi=null ;
                RangeQueryInfo rqi = null;
                AbstractConditionQueryInfo acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(0);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqi = (ComparisonQueryInfo)acqi;
                }
                
                acqi = (AbstractConditionQueryInfo)ccwqi.getOperands().get(1);
                if( acqi instanceof RangeQueryInfo) {
                  rqi = (RangeQueryInfo)acqi;
                }else {
                  cqi = (ComparisonQueryInfo)acqi;
                }
                assertNotNull(rqi);
                assertNotNull(cqi);
                assertNotNull(rqi);                           
                assertTrue(rqi.getLowerBound().equals( new SQLInteger(9)));           
                assertTrue(rqi.getUpperBound().equals( new SQLInteger(11) ));
                assertTrue(rqi.getUpperBoundOperator()== RelationalOperator.LESS_EQUALS_RELOP);                
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                          
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testMultipleRangeQueryInfoGeneration_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID >= 1  AND ID <=9  AND  ID >= 11 AND ID <= 13";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);                
                assertEquals(ccwqi.getOperands().size(),2);
                List operands = ccwqi.getOperands();
                assertEquals(operands.size(), 2);
                Iterator itr = operands.iterator();
                RangeQueryInfo rqi1 =(RangeQueryInfo) itr.next();
                RangeQueryInfo rqi2 =(RangeQueryInfo) itr.next();                            
                assertTrue(rqi1.getLowerBound().equals( new SQLInteger(1)) || rqi1.getLowerBound().equals( new SQLInteger(11)));           
                assertTrue(rqi2.getLowerBound().equals( new SQLInteger(1)) || rqi2.getLowerBound().equals( new SQLInteger(11)));
                assertTrue(rqi1.getUpperBound().equals( new SQLInteger(9) ) || rqi1.getUpperBound().equals( new SQLInteger(13) ));
                assertTrue(rqi2.getUpperBound().equals( new SQLInteger(9) ) || rqi2.getUpperBound().equals( new SQLInteger(13) ));
                assertTrue(rqi1.getUpperBoundOperator()== RelationalOperator.LESS_EQUALS_RELOP);                
                assertTrue(rqi2.getUpperBoundOperator()== RelationalOperator.LESS_EQUALS_RELOP);
                assertEquals(rqi1.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                assertEquals(rqi2.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testMultipleRangeQueryInfoGeneration_3() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID >= 1 AND  ID =7  AND ID <=9  ";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            private ComparisonQueryInfo cqi;
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {

              if (qInfo instanceof SelectQueryInfo) {
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                assertTrue(acqi instanceof AndJunctionQueryInfo);
                AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                assertEquals(jqi.getJunctionType(),
                    QueryInfoConstants.AND_JUNCTION);
                assertEquals(jqi.getOperands().size(), 2);
                acqi = (AbstractConditionQueryInfo)jqi.getOperands().get(0);
                if (acqi instanceof ParameterizedConditionsWrapperQueryInfo) {
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)acqi;
                }
                if (acqi instanceof ComparisonQueryInfo) {
                  cqi = (ComparisonQueryInfo)acqi;
                }

                acqi = (AbstractConditionQueryInfo)jqi.getOperands().get(1);
                if (acqi instanceof ParameterizedConditionsWrapperQueryInfo) {
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)acqi;
                }
                if (acqi instanceof ComparisonQueryInfo) {
                  cqi = (ComparisonQueryInfo)acqi;
                }
                
                assertNotNull(pcwqi);
                assertNotNull(cqi);

              }

            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);                           
                assertTrue(rqi.getLowerBound().equals( new SQLInteger(1)));           
                assertTrue(rqi.getUpperBound().equals( new SQLInteger(9) ));
                assertTrue(rqi.getUpperBoundOperator()== RelationalOperator.LESS_EQUALS_RELOP);                
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                try {
                  assertTrue(((ValueQueryInfo)cqi.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(7)) );
                 
                }catch (StandardException se) {
                  fail("test  failed because of exception="+se );
                }    
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testMultipleNonRangeConditions() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID <= -10 AND ID >= 1  AND ID <= 9  ";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);                
                assertEquals(ccwqi.getOperands().size(),2);
                List operands = ccwqi.getOperands();                              
                assertEquals(operands.size(), 2);
                Iterator itr = operands.iterator();
                ComparisonQueryInfo cqi1 =(ComparisonQueryInfo) itr.next();
                ComparisonQueryInfo cqi2 =(ComparisonQueryInfo) itr.next();
                try {
                  assertTrue(((ValueQueryInfo)cqi1.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(-10))||
           ((ValueQueryInfo)cqi1.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(1)) );
                  assertTrue(((ValueQueryInfo)cqi2.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(-10))||
           ((ValueQueryInfo)cqi2.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(1)) );

                 
                }catch (StandardException se) {
                  fail("test  failed because of exception="+se );
                }     
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  public void testMergeTwoORJunctionsToFormANDJunction() throws Exception
  {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024) , TYPE int)");
    String query = "select ID, DESCRIPTION from TESTTABLE where ((DESCRIPTION  = 'a' OR ADDRESS < 'c') AND (DESCRIPTION ='d' OR ADDRESS > 'b'))  OR ID IN (1, 2, 3, 4, 5) OR TYPE = 3 ";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            
                   
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              if (qInfo instanceof SelectQueryInfo) {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof SelectQueryInfo);
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                assertTrue(acqi instanceof OrJunctionQueryInfo);
                OrJunctionQueryInfo root = (OrJunctionQueryInfo)acqi;
                assertEquals(root.getJunctionType(),
                    QueryInfoConstants.OR_JUNCTION);
                assertTrue(root.isStaticallyNotGetConvertible());
                assertTrue(root.isWhereClauseDynamic());
                assertEquals(root.getOperands().size(), 2);
                List Ops = root.getOperands();
                Iterator prmItr = Ops.iterator();
                while (prmItr.hasNext()) {
                  AbstractConditionQueryInfo temp = (AbstractConditionQueryInfo)prmItr
                      .next();
                  assertTrue(temp instanceof InQueryInfo
                      || temp instanceof ComparisonQueryInfo);
                }
                AndJunctionQueryInfo ajqi = (AndJunctionQueryInfo)root.children[0];
                assertTrue(ajqi.getOperands().isEmpty());
                assertEquals(ajqi.children.length, 2);
                assertTrue(ajqi.children[0] instanceof OrJunctionQueryInfo);
                assertEquals(ajqi.children[0].getOperands().size(),2);
                assertTrue(ajqi.children[1] instanceof OrJunctionQueryInfo);
                assertEquals(ajqi.children[1].getOperands().size(),2);

              }

            }

          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      Statement stmnt = conn.createStatement();
      stmnt.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBug39530_1() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where ID > 1 AND ID >= 4   AND ID < 8 AND ID <= 4";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {

            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.groupParameterizedConditions(activation, false) ;
                         
                assertTrue(rqi.getLowerBound().equals( new SQLInteger(4)));           
                assertTrue(rqi.getUpperBound().equals( new SQLInteger(4) ));
                assertTrue(rqi.getUpperBoundOperator()== RelationalOperator.LESS_EQUALS_RELOP);                
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);
                          
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  public void testBug39530_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "select ID, DESCRIPTION from TESTTABLE where ID  >= 1 And ID <= 5  AND ID <= 7 AND ID >= 5";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {

            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                RangeQueryInfo rqi = (RangeQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);              
                assertEquals(rqi.getLowerBound(), new SQLInteger(5));
                assertEquals(rqi.getUpperBound(), new SQLInteger(5));
                assertEquals(rqi.getUpperBoundOperator(), RelationalOperator.LESS_EQUALS_RELOP);
                assertEquals(rqi.getLowerBoundOperator(), RelationalOperator.GREATER_EQUALS_RELOP);    
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  /**
   * Tests if the where clause contains multiple conditions on same column forming
   * a close range, a range query info object is created
   * 
   */
  public void testANDJunctionConditionNotSupportingRange() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int primary key, "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query = "Select * from TESTTABLE where (ID > 1 AND ID < 10)  AND ID < -1";
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {


            private ParameterizedConditionsWrapperQueryInfo pcwqi= null; 
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
             
                if (qInfo instanceof SelectQueryInfo) {
                  assertTrue(qInfo instanceof SelectQueryInfo);
                  SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                  AbstractConditionQueryInfo acqi = sqi.getWhereClause();
                  assertTrue(acqi instanceof AndJunctionQueryInfo);
                  AndJunctionQueryInfo jqi = (AndJunctionQueryInfo)acqi;
                  assertEquals(jqi.getJunctionType(),
                      QueryInfoConstants.AND_JUNCTION);
                  assertEquals(jqi.getOperands().size(), 1);
                  pcwqi = (ParameterizedConditionsWrapperQueryInfo)jqi
                      .getOperands().get(0);                  
                }
                          
            }

            @Override
            public void beforeQueryExecution(EmbedStatement stmt,
                Activation activation) throws SQLException
            {
              try {
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;                
                ConstantConditionsWrapperQueryInfo ccwqi = (ConstantConditionsWrapperQueryInfo)pcwqi.
                groupParameterizedConditions(activation, false);     
                
                assertEquals(ccwqi.getOperands().size() ,2);
               
                if(ccwqi.getOperands().get(0) instanceof RangeQueryInfo || ccwqi.getOperands().get(1) instanceof RangeQueryInfo) {
                  RangeQueryInfo rqi ;
                  ComparisonQueryInfo cqi;
                  if(ccwqi.getOperands().get(0) instanceof RangeQueryInfo ) {                    
                    rqi = (RangeQueryInfo)ccwqi.getOperands().get(0);
                    cqi = (ComparisonQueryInfo)ccwqi.getOperands().get(1);
                  } else {
                    rqi = (RangeQueryInfo)ccwqi.getOperands().get(1);
                    cqi = (ComparisonQueryInfo)ccwqi.getOperands().get(0);  
                  }
                  try {
                    assertEquals(1, rqi.getLowerBound().getInt());
                    assertEquals(RelationalOperator.GREATER_THAN_RELOP, rqi
                        .getLowerBoundOperator());
                    assertEquals(10, rqi.getUpperBound().getInt());
                    assertEquals(RelationalOperator.LESS_THAN_RELOP, rqi
                        .getUpperBoundOperator());

                    assertEquals(((ValueQueryInfo)cqi.rightOperand)
                        .evaluateToGetDataValueDescriptor(null),
                        new SQLInteger(-1));
                  }catch(StandardException se) {
                    se.printStackTrace();
                    fail("Test failed because of .Exception= "+se);
                  }                 
                  
                }else {
                  ComparisonQueryInfo cqi1 = (ComparisonQueryInfo)ccwqi.getOperands().get(0);
                  ComparisonQueryInfo cqi2 = (ComparisonQueryInfo)ccwqi.getOperands().get(1);
                  try {
                    assertTrue( ((ValueQueryInfo)cqi1.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(-1)) || ((ValueQueryInfo)cqi1.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(1)));  
                    assertTrue( ((ValueQueryInfo)cqi2.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(-1)) || ((ValueQueryInfo)cqi2.rightOperand).evaluateToGetDataValueDescriptor(activation).equals(new SQLInteger(1)));
                  }catch(StandardException se) {
                    se.printStackTrace();
                    fail("Test failed because of .Exception= "+se);
                  }
                  
                }   
              }
              catch (StandardException e) {
                throw new GemFireXDRuntimeException(e);
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
  }
  
  
  //ColocatedQueriesTest
  public void testDifferentColumnsOrConstantTying_EXECUTE() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null)"
            + " partition by column (ADDRESS2, ID2) colocate with (TESTTABLE1)");
    s
        .execute("create table TESTTABLE3 (ID3 varchar(1024), "
            + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) not null)"
            + " partition by column (ADDRESS3, ID3) colocate with (TESTTABLE1)");
    s.execute("create table REPLTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) REPLICATE");
    s.execute("create table PARTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1)");
    s
        .execute("create table PARTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null)"
            + " partition by column (ADDRESS2) colocate with (PARTTABLE1)");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p1.ADDRESS1 and p1.ADDRESS1 = p2.ID2"
          + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p3.ADDRESS3 and p3.ADDRESS3 = p2.ID2"
          + " and p1.ID1 = p3.ID3 and p1.ADDRESS1 = p2.DESCRIPTION2"
          + " and p2.ADDRESS2 = p2.ID2 and p3.ID3 = p2.DESCRIPTION2";
//      String query3 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
//          + " TESTTABLE2 p2 where p1.ID1 = 'X3' and 'X3' = p2.ID2"
//          + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
//          + " and p1.ADDRESS1 = p2.ADDRESS2";
/*sb: temporary disable.
      String query4 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 IN ('X3') and p2.ID2 IN ('X3', 'X4')"
          + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p1.ADDRESS1 = p2.ADDRESS2 and p2.ID2 IN ('X3')";
      String query5 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 IN ('X3') and p2.ID2 IN ('X3', 'X3')"
          + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p1.ADDRESS1 = p2.ADDRESS2";
*/          
      String query6 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p1.DESCRIPTION1 and p1.ID1 = p3.ID3"
          + " and p1.DESCRIPTION1 = p2.ID2 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query7 = "select ID1, DESCRIPTION2 from REPLTABLE1, TESTTABLE2 "
          + "where ID2 = ADDRESS2";
      String query8 = "select p1.ID1, p2.DESCRIPTION2 from REPLTABLE1 r1, "
          + "PARTTABLE1 p1, PARTTABLE2 p2 where p1.ID1 = p2.ADDRESS2";
      String query9 = "select r1.ID1, p1.DESCRIPTION1 from REPLTABLE1 r1, "
          + "PARTTABLE1 p1 where p1.ID1 = 'X1'";
      String query10 = "select r1.ID1, p1.DESCRIPTION1 from REPLTABLE1 r1, "
          + "PARTTABLE1 p1 where p1.ID1 IN ('X1', 'X2', 'X3')";
//      String query11 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
//          + "where ID1 IN ('X7') and ADDRESS2 IN ('X7')";

      s.executeQuery(query1);
      s.executeQuery(query2);
//      /s.executeQuery(query3);
/*
      s.executeQuery(query4);
      s.executeQuery(query5);
*/
      s.executeQuery(query6);
      s.executeQuery(query7);
      s.executeQuery(query8);
      s.executeQuery(query9);
      s.executeQuery(query10);
//      s.executeQuery(query11);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  
  public void testBug40307() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024), "
        + "DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024)"
        + " not null) partition by column (ID2)"
        + " colocate with (TESTTABLE1) ");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024)"
        + " not null) REPLICATE");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                StmtCachingEnabledExistingTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 r1, TESTTABLE1 p1, "
          + "TESTTABLE2 p2 where p1.ID1 = p2.ID2 and r1.ID3 = p2.ID2";
      String query2 = "select * from TESTTABLE3 r1, TESTTABLE1 p1, "
          + "TESTTABLE2 p2 where p1.ID1 = r1.ID3 and r1.ID3 = p2.ID2";
      s.executeQuery(query1);
      s.executeQuery(query2);
      //s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testParameterWithStatementMatching() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
//    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
//    final String derbyConn = "jdbc:derby:newDerbySecDB;";
//    String sysConnUrl = derbyConn + "create=true;" + "user=" + bootUserName
//        + ";password=" + bootUserPassword;
//    getLogger().info("Connecting system user with " + sysConnUrl);
//    Connection conn = DriverManager.getConnection(sysConnUrl);
    
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024), "
        + "DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024)"
        + " not null) "); //partition by column (ID1)
    try {
      s
          .execute("insert into testtable1 values (?, 'fail compilation', 'due to parameter') ");
      fail("expected to fail with 07000");
    } catch (SQLException sqle) {
      if (!"07000".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    try {
      s.execute("update testtable1 set description1 = 'ddd' where id1 = ?");
      fail("expected to fail with 07000");
    } catch (SQLException sqle) {
      if (!"07000".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    try {
      s
          .execute("delete from testtable1 t where t.id1 in (select id1 from testtable1 where id1 = t.id1 and id1 like ?)");
      fail("expected to fail with 07000");
    } catch (SQLException sqle) {
      if (!"07000".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    try {
      s
          .executeQuery("select * from testtable1 a, testtable1 b where a.id1 = b.id1 and a.id1 = ?");
      fail("expected to fail with 07000");
    } catch (SQLException sqle) {
      if (!"07000".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

  }
  //TODO : Asif? Check why a statement is getting converted into PreparedStatement if the generalized statement
  // is not found
  public void _testSubqueryStatementMatching_1() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String query = "select  DESCRIPTION1 from TESTTABLE1 where "
        + "ID1 IN (select ID2 from TESTTABLE2) ";

    String table1 = "create table TESTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

    String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";
    s.execute(table1);
    s.execute(table2);

    String insert = "Insert into  TESTTABLE1 values(1,'desc1_1','add1_1')";
    s.execute(insert);

    for (int i = 1; i < 3; ++i) {
      insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
          + "', 'add2_')";
      s.execute(insert);

    }
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo
                  && ((DMLQueryInfo)qInfo).isSubQuery()) {

                callbackInvoked = true;
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertFalse(sqi.isPreparedStatementQuery());
              }
            }
          });

      s.executeQuery(query);

      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    }
  }

  public void testSubqueryStatementMatching_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String query = "select  ID1 from TESTTABLE1 where "
        + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 1) ";

    String table1 = "create table TESTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

    String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";
    s.execute(table1);
    s.execute(table2);

    
    for (int i = 1; i < 4; ++i) {
      String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_"+i+"','add1_"+i+"')";          
      s.execute(insert);

    }
    for (int i = 1; i < 7; ++i) {
     String insert = "Insert into  TESTTABLE2 values(" + i + ","+i+",'desc2_"
          + "', 'add2_')";
      s.execute(insert);

    }
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo
                  && ((DMLQueryInfo)qInfo).isSubQuery()) {
                callbackInvoked = true;
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                assertFalse(sqi.isPreparedStatementQuery());

              }
            }
          });

      ResultSet rs = s.executeQuery(query);
      int count = 0;
      Set<Integer> keys = new HashSet<Integer>();
      for(int i = 2 ; i <4; ++i) {
        keys.add(i);
      }
      while(rs.next()) {
        ++count;
        assertTrue(keys.contains(rs.getInt(1)));
      }
      assertEquals(2,count);
      assertTrue(this.callbackInvoked);
      keys.clear();
     
      this.callbackInvoked = false;
      query = "select  ID1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 2) ";
      count = 0;
      
      for(int i = 3 ; i <4; ++i) {
        keys.add(i);
      }
      rs = s.executeQuery(query);
      while(rs.next()) {
        ++count;
        assertTrue(keys.contains(rs.getInt(1)));
      }
      assertEquals(1,count);
      assertFalse(this.callbackInvoked);
   
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    }
  }
  public void testBug46928() throws Exception {
	    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
	    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

	    Connection conn = getConnection();
	    Statement s = conn.createStatement();
	    String query =  "select c.c1 from t1 c where exists " +
       "(select * from t3 f where f.c1 = c.c1 and f.c2 > 1) " + "UNION " + "select s.c1 from t2 s where " 
	    		+ "(select sum(f.c2) from t4 f where f.c1 = s.c1 GROUP BY f.c1) > 1";

	    String table1 = "create table t1 (c1 int, c2 int) replicate" ;
	    String table2 = "create table t2 (c1 int, c2 int) replicate" ;
	    String table3 = "create table t3 (c1 int, c2 int) replicate" ;
	    String table4 = "create table t4 (c1 int, c2 int) replicate" ;
	    
	    s.execute(table1);
	    s.execute(table2);
	    s.execute(table3);
	    s.execute(table4);
	    

	    GemFireXDQueryObserver old = null;
	    try {
	      old = GemFireXDQueryObserverHolder
	          .setInstance(new GemFireXDQueryObserverAdapter() {
	            @Override
	            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
	                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
	              if (qInfo instanceof SelectQueryInfo ) {
	                callbackInvoked = true;
	                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
	                assertFalse(sqi.isPreparedStatementQuery());

	              }
	            }
	          });

	      ResultSet rs = s.executeQuery(query);
	      
	      assertTrue(this.callbackInvoked);
	      
	    } finally {
	      if (old != null) {
	        GemFireXDQueryObserverHolder.setInstance(old);
	      }
	      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
	    }
	  }
  
  public void testBug() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
 //   System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING,"true");
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

      BufferedReader scriptReader = new BufferedReader(new FileReader(
          TestUtil.getResourcesDir()
              + "/lib/GemFireXD_EF_IntegrationQuery.sql"));
      StringBuilder qry = new StringBuilder();
      String qryLine;
      while ((qryLine = scriptReader.readLine()) != null) {
        qry.append(qryLine).append('\n');
      }

      Connection conn = getConnection();
      Statement s = conn.createStatement();
        
      try {
        s.execute( "create schema TESTAPP");
        s.execute(
            "CREATE TABLE TESTAPP.TESTTABLE(ID INT, NAME VARCHAR(50))");
        s.execute( "create schema TEMP");
        s.execute(
            "CREATE TABLE TEMP.SCHEMATABLES(TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), TABLE_NAME VARCHAR(50), " +
            "TABLE_TYPE VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMACOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION INTEGER, COLUMN_DEFAULT VARCHAR(50), " +
            "IS_NULLABLE VARCHAR(50), DATA_TYPE INTEGER, TYPE_NAME VARCHAR(50), CHARACTER_OCTET_LENGTH INTEGER, " +
            "COLUMN_SIZE INTEGER, SCALE INTEGER, PRECISION_RADIX INTEGER, IS_AUTOINCREMENT VARCHAR(50), " +
            "PRIMARY_KEY VARCHAR(50), EDM_TYPE VARCHAR(50))  replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAVIEWS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAINDEXES (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), INDEX_NAME VARCHAR(50), INDEX_QUALIFIER VARCHAR(50), INDEX_TYPE VARCHAR(50), " +
            "COLUMN_NAME VARCHAR(50), SUNIQUE SMALLINT, SORT_TYPE VARCHAR(50), FILTER_CONDITION VARCHAR(50), " +
            "PRIMARY_KEY VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAINDEXCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), INDEX_NAME VARCHAR(50), INDEX_QUALIFIER VARCHAR(50), INDEX_TYPE VARCHAR(50), " +
            "COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, IS_NOT_UNIQUE SMALLINT, SORT_TYPE VARCHAR(50), " +
            "FILTER_CONDITION VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAFOREIGNKEYS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
            "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
            "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), DEFERRABILITY VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAFOREIGNKEYCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
            "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
            "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, " +
            "DEFERRABILITY VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMAPRIMARYKEYCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), " +
            "ORDINAL_POSITION SMALLINT) replicate");
        s.execute(            "CREATE TABLE TEMP.SCHEMACONSTRAINTCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
            "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
            "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
            "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, " +
            "DEFERRABILITY VARCHAR(50)) replicate");
        s.execute(
            "CREATE TABLE TEMP.SCHEMASCHEMAS (SCHEMA_CATALOG VARCHAR(50), SCHEMA_NAME VARCHAR(50)) replicate");

        for (int i = 1; i < 4; ++i) {
          String insert = "Insert into TESTAPP.TESTTABLE values(" + i + ",'desc1_"  + i + "')";
          s.execute(insert);
        }

        ResultSet rs45342 = TestUtil.getConnection().createStatement()
            .executeQuery(qry.toString());
        assertFalse(rs45342.next());
      } finally {
        s.execute("drop table if exists TEMP.SCHEMASCHEMAS");
        s.execute( "drop table if exists TEMP.SCHEMACONSTRAINTCOLUMNS");
        s.execute("drop table if exists TEMP.SCHEMAPRIMARYKEYCOLUMNS");
        s.execute( "drop table if exists TEMP.SCHEMAFOREIGNKEYCOLUMNS");
        s.execute( "drop table if exists TEMP.SCHEMAFOREIGNKEYS");
        s.execute( "drop table if exists TEMP.SCHEMAINDEXCOLUMNS");
        s.execute("drop table if exists TEMP.SCHEMAINDEXES");
        s.execute( "drop table if exists TEMP.SCHEMAVIEWS");
        s.execute( "drop table if exists TEMP.SCHEMACOLUMNS");
        s.execute( "drop table if exists TEMP.SCHEMATABLES");
        s.execute( "drop schema TEMP RESTRICT");
        s.execute("drop table if exists TESTAPP.TESTTABLE");
        s.execute( "drop schema TESTAPP RESTRICT");
      
    }/**/

  }
  
  
  public void testBug42488() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    String query1 = "Select * from TESTTABLE where  ID >= 1 ";
    String query2 = "Select * from TESTTABLE where  ID >= 2 ";
    s.executeUpdate("insert into testtable values(1,'1','1')");
    GemFireXDQueryObserver old = null;
    try {
      //old = GemFireXDQueryObserverHolder .setInstance(new GemFireXDQueryObserverAdapter() {});

      try {
        s.executeQuery(query1);
        s.executeQuery(query2);
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query1, e.getSQLState());
      }
      //assertTrue(this.callbackInvoked);      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void _testBug42488_1() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
   for(int i = 1; i < 2; ++i) {
    s.executeUpdate("insert into testtable values("+i+",'1','1')");
   }

    String query1 = "Select * from TESTTABLE where  ID >= 1";
    //Let us execute query1 which will introduce the generalized statement
    s.executeQuery(query1);
    
    Thread th = new Thread(new Runnable() {
      public void run() {
        try {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        String query = "Select * from TESTTABLE where  ID >= 3";
        GemFireXDQueryObserver old = null;
        try {
          old = GemFireXDQueryObserverHolder .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterQueryExecution(GenericPreparedStatement stmt,
                Activation activation) throws StandardException {
              if(!(activation instanceof AbstractGemFireActivation) ) {
                stmt.makeInvalid(DependencyManager.BULK_INSERT, activation.getLanguageConnectionContext());
                stmt.rePrepare(activation.getLanguageConnectionContext(), activation);
              }
              GemFireXDQueryObserverHolder.clearInstance();
            }
          });

          stmt.executeQuery(query);
        
        }catch(Exception e) {
          throw new GemFireXDRuntimeException(e);
        }finally {
          
        }
        }catch(Exception e) {
          throw new GemFireXDRuntimeException(e);
        }
      }
    });
    th.start();
    String query2 = "Select * from TESTTABLE where  ID >= <?>";
      try {
        LanguageConnectionContext lcc = ((EmbedConnection)conn).getLanguageConnectionContext();
        String[] tokImg = {"6"};
        int tokKind [] = new int[] {516};
        ConstantValueSet cvs = new ConstantValueSetImpl(tokKind,tokImg);
        lcc.setConstantValueSet(null, cvs);
       ((EmbedStatement)s).executeQueryByPassQueryInfo(query2,false, false,0, null);
     
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query1, e.getSQLState());
      }
  }
  
  public void testBug46805() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
     //System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING,"true");
       SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
       Connection conn = getConnection();
       Statement s = conn.createStatement();
       s.execute("create table Customers(CustomerId integer not null generated always as identity, "
           + "CustomerCode varchar(8) not null, constraint PKCustomers primary key (CustomerId), "
           + "constraint UQCustomers unique (CustomerCode)) "
           + "partition by column (CustomerId) redundancy 1 persistent");

   String query = "insert into Customers (CustomerCode) values ('CC1')";
    s.executeUpdate(query);
    String query1 = "insert into Customers (CustomerCode) values ('CC2')";
    s.executeUpdate(query1);
  }

}
