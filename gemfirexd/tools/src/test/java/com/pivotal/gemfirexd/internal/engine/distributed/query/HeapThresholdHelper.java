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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.MemoryMonitorJUnitTest;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.execute.CallbackStatement;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.sql.GeneralizedStatement;
import com.pivotal.gemfirexd.internal.engine.sql.conn.GfxdHeapThresholdListener;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import io.snappydata.test.dunit.DistributedTestBase;

public class HeapThresholdHelper {

  private static final ThreadLocal<GemFireXDQueryObserver> tObserver = new ThreadLocal<GemFireXDQueryObserver>();
  private static final ThreadLocal<String> queryExecuted = new ThreadLocal<String>();

  @SuppressWarnings("serial")
  public static abstract class QueryExecutor implements Runnable, Serializable {

    protected String queryStr;

    protected String[] constantList;

    protected GemFireXDQueryObserver observer;

    protected final PauseVariants variant;

    protected int numOfNexts = 0;

    protected final int minRowsExpected;

    protected int numRowsFetched = 0;

    // threadlocal for DUnit cannot happen as observer is set from one thread
    // and
    // callback happens from another. So, query compare is needed in DUnit
    // testing.
    // with statement matching query string changes to a generic form and
    // therefore
    // only certain phase of the quey can be tested in dunit.
    protected boolean useThreadLocal;

    private volatile boolean wait4MemoryEvent = true;

    private volatile boolean observerCallbackInvoked = false;

    protected QueryExecutor() {
      minRowsExpected = -1;
      variant = null;
    }

    protected QueryExecutor(String queryStr, PauseVariants variant,
        String[] constantList) {
      this.queryStr = queryStr;
      this.constantList = constantList;
      this.minRowsExpected = variant.minExpectedRows();
      this.observer = variant.observer(this);
      this.variant = variant;
    }

    protected abstract void setFailedStatus(String query, String failmsg);

    protected Connection getConnection() throws SQLException {
      return TestUtil.getConnection();
    }

    public void execute(boolean setLocalObserver) throws SQLException {
      Connection conn = getConnection();
      try {
        Statement s;
        if (setLocalObserver) {
          GemFireXDQueryObserverHolder.putInstance(observer);
          tObserver.set(observer);
        }

        s = conn.createStatement();
        ResultSet rs = s.executeQuery(queryStr);
        while (rs.next()) {
          numRowsFetched++;
          TestUtil.getLogger().info("got result " + rs.getInt(1));
        }

        String failmsg = "Expected StandardException indicating statement cancelled due to low resources";
        setFailedStatus(queryStr, failmsg);
        DistributedTestBase.fail(failmsg);
      } catch (SQLException e) {
        if (!"XCL54".equals(e.getSQLState())) {
          String failmsg = "got unexpected exception " + e.getSQLState() + " "
              + SanityManager.getStackTrace(e);
          setFailedStatus(queryStr, failmsg);
          DistributedTestBase.fail(failmsg, e);
        }
        // else if query got canceled, lets check it haven't prematurely.
        else if (minRowsExpected != -1 && numRowsFetched < minRowsExpected) {
          String failmsg = "Expected " + minRowsExpected + " rows and got "
              + numRowsFetched + " rows before query is canceled";
          setFailedStatus(queryStr, failmsg);
          DistributedTestBase.fail(failmsg);
        }
      } finally {
        if (setLocalObserver) {
          GemFireXDQueryObserverHolder.removeObserver(observer);
          /* in case callback in query observer never got called and main
           * is still waiting for query compilation to happen whereas 
           * we are now failing the test. 
           */
          notifyCompilation();
        }
        conn.close();
      }
    }

    public synchronized void waitForCompilation() throws InterruptedException {
      while (!observerCallbackInvoked) {
        TestUtil.getLogger().info("Waiting for compilation of " + queryStr);
        wait();
      }
    }

    public synchronized void notifyCompilation() {
      TestUtil.getLogger().info("Done compilation of " + queryStr + " ");
      observerCallbackInvoked = true;
      notify();
      Thread.yield();
    }

    public synchronized void waitForMemoryEvent() {

      if (minRowsExpected != -1 && ++numOfNexts < minRowsExpected) {
        return;
      }

      notifyCompilation();

      TestUtil.getLogger().info("Start waiting for memory event ");

      while (wait4MemoryEvent) {
        try {
          wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      TestUtil.getLogger().info("Done waiting for memory event ");

    }

    public synchronized void notifyMemoryEvent() {
      TestUtil.getLogger().info("Notifying memoryEvent for " + queryStr + " ");
      wait4MemoryEvent = false;
      notify();
    }

    public String getGenericQuery() {
      if (constantList == null) {
        return queryStr;
      }
      else {
        String q = queryStr;
        for (String s : constantList) {
          q = q.replaceFirst(s, GeneralizedStatement.CONSTANT_PLACEHOLDER);
        }
        return q;
      }
    }

    public String query() {
      return queryStr;
    }

    public GemFireXDQueryObserver observer() {
      return observer;
    }

    public PauseVariants variant() {
      return variant;
    }

    public synchronized boolean isWaitingForMemoryEvent() {
      return wait4MemoryEvent;
    }
  }

  public enum PauseVariants {

    PAUSE_AFTER_OPTIMIZATION {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
          }
          @Override
          public void afterOptimizedParsedTree(String query, StatementNode qt,
              LanguageConnectionContext lcc) {
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }

            if (!executor.useThreadLocal
                && !query.equals(executor.query())) {
              return;
            }
            queryStr = executor.query();
            TestUtil.getLogger().info(
                "afterOptimizedParsedTree: honoring callback " + query);
            executor.waitForMemoryEvent();
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }
          
          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterOptimizedParsedTree: estimated memory for " + stmtText
                    + " is " + memused);
            return memused;
          }
        };
      }
    },

    PAUSE_BEFORE_QUERY_EXECUTE {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          @Override
          public void beforeQueryExecution(EmbedStatement stmt, Activation activation) {
            queryExecuted.set(stmt.getSQLText());
            LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }

            if (!executor.useThreadLocal
                && !stmt.getSQLText().equals(executor.getGenericQuery())) {
              return;
            }
            queryStr = stmt.getSQLText();
            TestUtil.getLogger().info(
                "beforeQueryExecution: honoring callback " + stmt.getSQLText());
            executor.waitForMemoryEvent();
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterQueryExecution: estimated memory for " + stmtText
                    + " is " + memused);
            return memused;
          }
        };
      }
    },
    
    PAUSE_AFTER_QUERY_EXECUTE {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
          }
          @Override
          public boolean afterQueryExecution(final CallbackStatement stmt,
              SQLException sqle) {
            if (executor.useThreadLocal && tObserver.get() != this) {
              return false;
            }

            if (!executor.useThreadLocal
                && !queryExecuted.get().equals(executor.query())) {
              return false;
            }
            queryStr = queryExecuted.get();
            TestUtil.getLogger().info(
                "afterQueryExecution: honoring callback " + queryExecuted.get());
            executor.waitForMemoryEvent();
            return false;
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterQueryExecution: estimated memory for " + stmtText
                    + " is " + memused);
            return memused;
          }
        };
      }
    },

    PAUSE_BEFORE_GENERIC_PREP_STMT_QUERY_EXECUTE {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
         
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }
            if (!executor.useThreadLocal
                && !stmt.getUserQueryString(lcc).equals(executor.query())) {
              return;
            }
            queryStr = executor.query();
            TestUtil.getLogger().info(
                "beforeGenericPSQueryExecution: honoring callback " + queryStr);
            executor.waitForMemoryEvent();
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "beforeGenericPSQueryExecution: estimated memory for " + stmtText
                    + " is " + memused);
            return memused;
          }
        };
      }
    },

    PAUSE_AFTER_GENERIC_PREP_STMT_QUERY_EXECUTE {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
          }
          @Override
          public void afterQueryExecution(GenericPreparedStatement stmt,
              Activation activation) throws StandardException {
            
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }

            if (!executor.useThreadLocal
                && !queryExecuted.get().equals(executor.query())) {
              return;
            }
            queryStr = executor.query();
            TestUtil.getLogger().info(
                "afterGenericPSQueryExecution: honoring callback " + queryExecuted.get());
            executor.waitForMemoryEvent();
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterGenericPSQueryExecution: estimated memory for " + stmtText
                    + " is " + memused);
            return memused;
          }
        };
      }
    },

    PAUSE_AFTER_RESULT_SET_OPEN {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
          }
          @Override
          public void afterResultSetOpen(GenericPreparedStatement stmt, LanguageConnectionContext lcc, com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }
            if (!executor.useThreadLocal
                && !queryExecuted.get().equals(executor.query())) {
              return;
            }
            queryStr = executor.query();
            TestUtil.getLogger().info(
                "afterResultSetOpen: honoring callback " + stmt.getUserQueryString(lcc));
            executor.waitForMemoryEvent();
          }

          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            delayNext(newRow, executor.query(), theResults);
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterResultOpen: estimated memory for " + stmtText + " is "
                    + memused);
            return memused;
          }
        };
      }
    },

    PAUSE_ON_RESULT_SET_NEXT {

      @Override
      @SuppressWarnings("serial")
      GemFireXDQueryObserver observer(final QueryExecutor executor) {
        return new GemFireXDQueryObserverAdapter() {
          @Override
          public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
            queryExecuted.set(stmt.getUserQueryString(lcc));
          }
          @Override
          public void onEmbedResultSetMovePosition(EmbedResultSet rs,
              ExecRow newRow,
              com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
            if (executor.useThreadLocal && tObserver.get() != this) {
              return;
            }
            String query = executor.query();
            if (!executor.useThreadLocal
                && !query.equals(queryExecuted.get())) {
              return;
            }
            queryStr = executor.query();
            TestUtil.getLogger().info(
                "onEmbedResultSetMovePosition: honoring callback "
                    + queryExecuted.get());
            delayNext(newRow, query, theResults);
            executor.waitForMemoryEvent();
          }

          @Override
          public long estimatedMemoryUsage(String stmtText, long memused) {
            TestUtil.getLogger().info(
                "afterResultSetNext: estimated memory for " + stmtText + " is "
                    + memused);
            return memused;
          }
        };
      }

      @Override
      int minExpectedRows() {
        return 4;
      }

    };

    String queryStr;

    abstract GemFireXDQueryObserver observer(final QueryExecutor executor);

    private static void delayNext(ExecRow newRow, String execQryStr,
        com.pivotal.gemfirexd.internal.iapi.sql.ResultSet theResults) {
      if (newRow == null) {
        return;
      }
      if (! execQryStr.equals(queryExecuted.get())) {
        TestUtil.getLogger().info("NOT Delaying next for [" + execQryStr + "]");
        return;
      }

      try {
        Thread t = Thread.currentThread();
        synchronized (t) {
          TestUtil.getLogger().info("Delaying next on [" + execQryStr + "]");
          t
              .wait(GfxdHeapThresholdListener.queryCancellationTimeInterval + 1500);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    int minExpectedRows() {
      return -1;
    }

    @Override
    public String toString() {
      return "QueryString " + (queryStr != null ? queryStr : " null ")
          + " with PauseVariant " + this.name();
    }
  }

  @SuppressWarnings("serial")
  public static void raiseMemoryEvent(boolean criticalUp,
      boolean waitForEventToHonor) throws InterruptedException {

    TestUtil.getLogger().info(
        "About to raise memory event "
            + (criticalUp ? "CRITICAL_UP" : "CRITICAL_DOWN"));
    GemFireCacheImpl gfCache = Misc.getGemFireCache();

    InternalResourceManager resMgr = gfCache.getResourceManager();
    resMgr.getHeapMonitor().setTestMaxMemoryBytes(100);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    resMgr.setCriticalHeapPercentage(90F);

    if (criticalUp) {
      TestUtil.getLogger().info("About to raise CRITICAL_UP event ");
      gfCache.getLoggerI18n().fine(
          MemoryMonitorJUnitTest.addExpectedAbove);
      resMgr.getHeapMonitor().updateStateAndSendEvent(92);
      gfCache.getLoggerI18n().fine(
          MemoryMonitorJUnitTest.removeExpectedAbove);
    }
    else {

      final boolean[] criticalDownHonoredArr = new boolean[] { false };
      GemFireXDQueryObserver observer = new GemFireXDQueryObserverAdapter() {
        
        @Override
        public void beforeQueryExecution(GenericPreparedStatement stmt, LanguageConnectionContext lcc) {
          queryExecuted.set(stmt.getUserQueryString(lcc));
        }

        @Override
        public void criticalDownMemoryEvent(GfxdHeapThresholdListener listener) {
          synchronized (this) {
            criticalDownHonoredArr[0] = true;
            TestUtil.getLogger().info(
                "Notifying CRITICAL_DOWN event to the waiters "
                    + criticalDownHonoredArr[0]);
            this.notify();
          }
        }

      };

      GemFireXDQueryObserver old = null;
      try {
        if (waitForEventToHonor) {
          old = GemFireXDQueryObserverHolder.setInstance(observer);
        }

        TestUtil.getLogger().info("About to raise CRITICAL_DOWN event ");
        gfCache.getLoggerI18n().fine(
            MemoryMonitorJUnitTest.addExpectedBelow);
        resMgr.getHeapMonitor().updateStateAndSendEvent(19);
        gfCache.getLoggerI18n().fine(
            MemoryMonitorJUnitTest.removeExpectedBelow);

        if (waitForEventToHonor) {
          synchronized (observer) {
            /*until critical down event is seen by the query.canceller thread. 
             *set via observer callback.
             */
            while (!criticalDownHonoredArr[0]) {
              TestUtil.getLogger().info(
                  "Waiting for CRITICAL_DOWN event to be honored "
                      + criticalDownHonoredArr[0]);
              observer.wait();

            }
            ;
          }
        }
        Thread.yield();
      } finally {
        if (old != null) {
          GemFireXDQueryObserverHolder.setInstance(old);
        }
      }
    }

  }

  public static void prepareTables(Connection connection) throws SQLException {

    Connection conn = null;
    if (connection == null) {
      TestUtil.setupConnection();
      conn = TestUtil.jdbcConn;
    }
    else {
      conn = connection;
    }
    Statement s = conn.createStatement();

    try {
      s.execute("drop table testtable2");
      s.execute("drop table testtable1");
    } catch (SQLException ignore) {
      TestUtil.getLogger().info("Ignoring Drop exception " + ignore);
    }

    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by primary key");

    s
        .execute("create table TESTTABLE2 (ID2 int primary key , "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by primary key colocate with (TestTable1)");

    s.execute("create index tt2_idx1 on TESTTABLE2(DESCRIPTION2) ");

    String tab1 = getData(new StringBuilder(
        "insert into testtable1 (ID1, DESCRIPTION1, ADDRESS1) values "));
    s.executeUpdate(tab1);

    // s.execute("insert into TestTable2 select * from testtable1 union select r1, r2, r2 from (values (20, 'd' , 'dd') ) as MyT(r1, r2, r3)");

    String tab2 = getData(new StringBuilder(
        "insert into testtable2 (ID2, DESCRIPTION2, ADDRESS2) values "));
    s.executeUpdate(tab2);

    conn.close();
  }

  private static String getData(StringBuilder sb) {
    for (int i = 0; i < 20; i++) {
      if (i != 0) {
        sb.append(",");
      }

      sb.append("(" + i);
      sb.append(",'DESC'");
      sb.append(",'ADD')");
    }
    return sb.toString();
  }

  public static Thread executeQueryInThread(
      HeapThresholdHelper.QueryExecutor executor) throws SQLException,
      InterruptedException {

    Thread executeQ = new Thread(executor, "[" + executor.query() + "]") {

      @Override
      public void start() {
        super.start();
        TestUtil.getLogger().info("starting " + this);
      }

    };

    executeQ.setDaemon(true);
    executeQ.setPriority(Thread.MIN_PRIORITY);
    executeQ.start();

    Thread.yield();
    return executeQ;
  }

}
