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
package cacheperf.comparisons.gemfirexd.useCase5;

import cacheperf.comparisons.gemfirexd.QueryPerfException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to UseCase5.
 */
public class UseCase5Stats extends PerformanceStatistics {

  public static enum Stmt {
    selstm, inslog, updbal, insbal, updterm, insterm, instkt, instsn;
  }

  /** <code>UseCase5Stats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  protected static final String INSERT_TX_COMPLETED = "insertTxCompleted";
  protected static final String INSERT_TX_TIME = "insertTxTime";

  protected static final String selstm_stmt = "selstmStmtsExecuted";
  protected static final String selstm_stmt_time = "selstmStmtTime";
  protected static final String inslog_stmt = "inslogStmtsExecuted";
  protected static final String inslog_stmt_time = "inslogStmtTime";
  protected static final String updbal_stmt = "updbalStmtsExecuted";
  protected static final String updbal_stmt_time = "updbalStmtTime";
  protected static final String insbal_stmt = "insbalStmtsExecuted";
  protected static final String insbal_stmt_time = "insbalStmtTime";
  protected static final String updterm_stmt = "updtermStmtsExecuted";
  protected static final String updterm_stmt_time = "updtermStmtTime";
  protected static final String insterm_stmt = "instermStmtsExecuted";
  protected static final String insterm_stmt_time = "instermStmtTime";
  protected static final String instkt_stmt = "instktStmtsExecuted";
  protected static final String instkt_stmt_time = "instktStmtTime";
  protected static final String instsn_stmt = "instsnStmtsExecuted";
  protected static final String instsn_stmt_time = "instsnStmtTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>UseCase5Stats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
      factory().createIntGauge
      (
        VM_COUNT,
        "When aggregated, the number of VMs using this statistics object.",
        "VMs"
      ),
      factory().createIntCounter
      (
        INSERT_TX_COMPLETED,
        "Number of insert transactions completed (committed).",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        INSERT_TX_TIME,
        "Total time spent on insert transactions that were completed (committed).",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selstm_stmt,
        "Number of selstm statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selstm_stmt_time,
        "Total time spent executing selstm statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        inslog_stmt,
        "Number of inslog statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        inslog_stmt_time,
        "Total time spent executing inslog statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        updbal_stmt,
        "Number of updbal statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        updbal_stmt_time,
        "Total time spent executing updbal statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insbal_stmt,
        "Number of insbal statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insbal_stmt_time,
        "Total time spent executing insbal statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        updterm_stmt,
        "Number of updterm statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        updterm_stmt_time,
        "Total time spent executing updterm statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insterm_stmt,
        "Number of insterm statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insterm_stmt_time,
        "Total time spent executing insterm statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        instkt_stmt,
        "Number of instkt statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        instkt_stmt_time,
        "Total time spent executing instkt statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        instsn_stmt,
        "Number of instsn statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        instsn_stmt_time,
        "Total time spent executing instsn statements.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static UseCase5Stats getInstance() {
    UseCase5Stats tps =
         (UseCase5Stats)getInstance(UseCase5Stats.class, SCOPE);
    tps.incVMCount();
    return tps;
  }
  public static UseCase5Stats getInstance(String name) {
    UseCase5Stats tps =
         (UseCase5Stats)getInstance(UseCase5Stats.class, SCOPE, name);
    tps.incVMCount();
    return tps;
  }
  public static UseCase5Stats getInstance( String name, String trimspecName ) {
    UseCase5Stats tps =
         (UseCase5Stats)getInstance(UseCase5Stats.class, SCOPE, name, trimspecName);
    tps.incVMCount();
    return tps;
  }

/////////////////// Construction / initialization ////////////////

  public UseCase5Stats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) {
    super( cls, type, scope, instanceName, trimspecName );
  }

/////////////////// Updating stats /////////////////////////

// vm count --------------------------------------------------------------------

  /**
   * increase the count on the vmCount
   */
  private synchronized void incVMCount() {
    if (!VMCounted) {
      statistics().incInt(VM_COUNT, 1);
      VMCounted = true;
    }
  }
  private static boolean VMCounted = false;

// histogram -------------------------------------------------------------------

  /**
   * increase the time on the optional histogram by the supplied amount
   */
  public void incHistogram(HistogramStats histogram, long amount) {
    if (histogram != null) {
      histogram.incBin(amount);
    }
  }

// transactions ----------------------------------------------------------------

  public long startCommit() {
    return NanoTimer.getTime();
  }

  public long startTransaction() {
    return NanoTimer.getTime();
  }

  public void endTransaction(long start, long commitStart) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(INSERT_TX_COMPLETED, 1);
    statistics().incLong(INSERT_TX_TIME, elapsed);
  }

// statements ------------------------------------------------------------------

  public long startStmt(Stmt stmt) {
    return NanoTimer.getTime();
  }

  public void endStmt(Stmt stmt, long start) {
    endStmt(stmt, start, 1, null);
  }

  public void endStmt(Stmt stmt, long start, HistogramStats histogram) {
    endStmt(stmt, start, 1, histogram);
  }

  public void endStmt(Stmt stmt, long start, int amount, HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    switch (stmt) {
      case selstm:
        statistics().incInt(selstm_stmt, amount);
        statistics().incLong(selstm_stmt_time, elapsed);
        break;
      case inslog:
        statistics().incInt(inslog_stmt, amount);
        statistics().incLong(inslog_stmt_time, elapsed);
        break;
      case updbal:
        statistics().incInt(updbal_stmt, amount);
        statistics().incLong(updbal_stmt_time, elapsed);
        break;
      case insbal:
        statistics().incInt(insbal_stmt, amount);
        statistics().incLong(insbal_stmt_time, elapsed);
        break;
      case updterm:
        statistics().incInt(updterm_stmt, amount);
        statistics().incLong(updterm_stmt_time, elapsed);
        break;
      case insterm:
        statistics().incInt(insterm_stmt, amount);
        statistics().incLong(insterm_stmt_time, elapsed);
        break;
      case instkt:
        statistics().incInt(instkt_stmt, amount);
        statistics().incLong(instkt_stmt_time, elapsed);
        break;
      case instsn:
        statistics().incInt(instsn_stmt, amount);
        statistics().incLong(instsn_stmt_time, elapsed);
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
    incHistogram(histogram, elapsed);
  }
}
