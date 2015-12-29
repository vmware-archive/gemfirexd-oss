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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.query.HeapThresholdHelper.PauseVariants;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

public class HeapThresholdTest extends JdbcTestBase {

  public HeapThresholdTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(HeapThresholdTest.class));
    SanityManager.DEBUG_SET("TraceHeapThreshold");
  }

  private volatile static boolean queryExecutionSuccess = true;

  public void testBug41438() throws Exception {
    // boot up the DB first
    setupConnection();
    addExpectedException("heap critical threshold");
    Connection conn = TestUtil.startNetserverAndGetLocalNetConnection();

    HeapThresholdHelper.prepareTables(conn);
    conn.close();
    conn = null;

    TestUtil.stopNetServer();

    conn = TestUtil.startNetserverAndGetLocalNetConnection();

    HeapThresholdHelper.prepareTables(conn);
    conn.close();
    conn = null;

    TestUtil.stopNetServer();
  }

  public void testQueryCancellation_1() throws Exception {

    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    HeapThresholdHelper.prepareTables(null);
    addExpectedException("heap critical threshold");

    TestUtil.getLogger().info("BEGIN testQueryCancellation_1");
    String[] query = new String[] { "Select ID1 from testtable1 where id1 > 2" };

    checkCriticalUp(PauseVariants.PAUSE_AFTER_OPTIMIZATION, query, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_QUERY_EXECUTE, query, null);

    checkCriticalUp(PauseVariants.PAUSE_BEFORE_GENERIC_PREP_STMT_QUERY_EXECUTE,
        query, null);

    checkCriticalUp(PauseVariants.PAUSE_AFTER_GENERIC_PREP_STMT_QUERY_EXECUTE,
        query, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_RESULT_SET_OPEN, query, null);
    checkCriticalUp(PauseVariants.PAUSE_ON_RESULT_SET_NEXT, query, null);

    TestUtil.getLogger().info("END testQueryCancellation_1");

    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
  }
  
  public void testQueryCancellation_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);

    HeapThresholdHelper.prepareTables(null);
    addExpectedException("heap critical threshold");

    TestUtil.getLogger().info("BEGIN testQueryCancellation_2");
    String[] query = new String[] { "Select ID1 from testtable1 where id1 > 2" };
    final String[][] constantList = new String[][] {
        new String[] {"2"}
    };

    checkCriticalUp(PauseVariants.PAUSE_AFTER_OPTIMIZATION, query, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_QUERY_EXECUTE, query, null);

    checkCriticalUp(PauseVariants.PAUSE_BEFORE_GENERIC_PREP_STMT_QUERY_EXECUTE,
        query, constantList);

    checkCriticalUp(PauseVariants.PAUSE_AFTER_GENERIC_PREP_STMT_QUERY_EXECUTE,
        query, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_RESULT_SET_OPEN, query, null);
    checkCriticalUp(PauseVariants.PAUSE_ON_RESULT_SET_NEXT, query, null);

    TestUtil.getLogger().info("END testQueryCancellation_2");

  }
  
  
  public void testMultipleQueryCancellation_1() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    final Connection conn = getConnection();
    addExpectedException("heap critical threshold");
    HeapThresholdHelper.prepareTables(conn);

    TestUtil.getLogger().info("BEGIN testMultipleQueryCancellation_1");
    String[] queries = new String[] {
        "Select ID1 from testtable1 where id1 > 12",
        "Select * from testtable1 where id1 > 10",
        "Select ID1, DESCRIPTION1 from testtable1 where id1 > 9" };

    checkCriticalUp(PauseVariants.PAUSE_AFTER_OPTIMIZATION, queries, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_QUERY_EXECUTE, queries, null);
    checkCriticalUp(PauseVariants.PAUSE_BEFORE_GENERIC_PREP_STMT_QUERY_EXECUTE,
        queries, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_GENERIC_PREP_STMT_QUERY_EXECUTE,
        queries, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_RESULT_SET_OPEN, queries, null);
    checkCriticalUp(PauseVariants.PAUSE_ON_RESULT_SET_NEXT, queries, null);

    TestUtil.getLogger().info("END testMultipleQueryCancellation_1");
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
  }
  
  public void testMultipleQueryCancellation_2() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    final Connection conn = getConnection();
    addExpectedException("heap critical threshold");
    HeapThresholdHelper.prepareTables(conn);

    TestUtil.getLogger().info("BEGIN testMultipleQueryCancellation_2");
    final String[] queries = new String[] {
        "Select ID1 from testtable1 where id1 > 12",
        "Select * from testtable1 where id1 > 10",
        "Select ID1, DESCRIPTION1 from testtable1 where id1 > 9" };
    
    final String[][] constantList = new String[][] {
        new String[] {"12"}
       ,new String[] {"10"}
       ,new String[] {"9"}
    };

    checkCriticalUp(PauseVariants.PAUSE_AFTER_OPTIMIZATION, queries, null);
    checkCriticalUp(PauseVariants.PAUSE_BEFORE_QUERY_EXECUTE, queries, null);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_QUERY_EXECUTE, queries,
        constantList);
    checkCriticalUp(PauseVariants.PAUSE_BEFORE_GENERIC_PREP_STMT_QUERY_EXECUTE,
        queries, constantList);
    checkCriticalUp(PauseVariants.PAUSE_AFTER_GENERIC_PREP_STMT_QUERY_EXECUTE,
        queries, constantList);
   checkCriticalUp(PauseVariants.PAUSE_AFTER_RESULT_SET_OPEN, queries,
        constantList);
    checkCriticalUp(PauseVariants.PAUSE_ON_RESULT_SET_NEXT, queries,
        constantList);

    TestUtil.getLogger().info("END testMultipleQueryCancellation_2");
  }

  @SuppressWarnings("serial")
  public void testMemoryEstimationOfQueries() throws SQLException {

    HeapThresholdHelper.prepareTables(null);
    addExpectedException("heap critical threshold");

    String[] queries = new String[] {
        "Select ID1 from testtable1 where id1 > 12",
        "Select * from testtable1 where id1 > 10",
        "Select ID1 from testtable1 where id1 > 9 order by description1",
        "Select ID1 from testtable1 order by description1",
        "Select ID1, ADDRESS1 from testtable1 order by address1",
        "Select ID1 from testtable1 t1 join testtable2 t2 on t1.id1 = t2.id2",
        "Select t1.id1, t2.id2 from testtable1 t1 left outer join testtable2 t2 on t1.id1 = t2.id2",
        "Select * from testtable1 t1 join testtable2 t2 on t1.id1 = t2.id2" };

    Connection conn = getConnection();
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public long estimatedMemoryUsage(String stmtText, long memused) {
              SanityManager.DEBUG_PRINT(null, "Memory computed for " + stmtText
                  + " " + memused);
              return memused;
            }
          });

      Activation acts[] = new Activation[queries.length];
      Statement s = conn.createStatement();
      for (int i = 0; i < queries.length; i++) {
        ResultSet r = s.executeQuery(queries[i]);

        acts[i] = ((EmbedConnection)conn).getLanguageConnection()
            .getLastActivation();
        acts[i].estimateMemoryUsage();

        for (int z = 0; z < 6; z++, r.next()) {
          ;
        }

        acts[i].estimateMemoryUsage();
      }
    } catch (StandardException e) {
      e.printStackTrace();
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }

  }
  
  private void checkCriticalUp(PauseVariants variant, String[] queries, String[][] constantList)
      throws SQLException, InterruptedException, InstantiationException,
      IllegalAccessException {
    reset();
    TestUtil.getLogger().info("Begin Checking " + variant.name());

    Thread executionThrds[] = new Thread[queries.length];
    UnitTestQueryExecutor executors[] = new UnitTestQueryExecutor[queries.length];

    int i = 0;
    for (int qi = 0; qi < queries.length; qi++) {

      executors[i] = new UnitTestQueryExecutor(queries[qi], variant,
          constantList != null && qi < constantList.length ? constantList[qi]
              : null);
      executionThrds[i] = HeapThresholdHelper
          .executeQueryInThread(executors[i]);

      i++;
    }

    for (int j = 0; j < executors.length; j++) {
      executors[j].waitForCompilation();
    }
    TestUtil.getLogger().info(
        "Resuming as the query is in the midst of execution ");

    HeapThresholdHelper.raiseMemoryEvent(true, false);

    for (UnitTestQueryExecutor exec1 : executors) {
      exec1.notifyMemoryEvent();
    }

    for (Thread t : executionThrds) {
      t.join();
    }

    HeapThresholdHelper.raiseMemoryEvent(false, true);

    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());

    if (!queryExecutionSuccess) {
      fail("Execution failed for " + variant);
    }

    TestUtil.getLogger().info("Done Checking " + variant.name());
  }

  @SuppressWarnings("serial")
  class UnitTestQueryExecutor extends HeapThresholdHelper.QueryExecutor {

    UnitTestQueryExecutor(String queryStr, PauseVariants variant,
        String[] constantList) {
      super(queryStr, variant, constantList);
      useThreadLocal = true;
    }

    public void run() {
      try {
        execute(true);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected void setFailedStatus(String query, String failmsg) {
      queryExecutionSuccess = false;
    }
  }

  private void reset() {
    queryExecutionSuccess = true;
  }

}
