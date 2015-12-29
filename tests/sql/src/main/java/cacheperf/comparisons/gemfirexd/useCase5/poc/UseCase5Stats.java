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
package cacheperf.comparisons.gemfirexd.useCase5.poc;

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
    insTerminal, insSource, insSourceMessageLog, insSingleTicket, insTicketHistory,
    insSourceMessageResult, insTicketsToPool, selTerminalStatus, insTerminalBalanceTransactionLog,
    selectTicketByTSN, updTicketForPay;
  }

  /** <code>UseCase5Stats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  protected static final String COMMITS_COMPLETED = "commitsCompleted";
  protected static final String COMMIT_TIME = "commitTime";
  protected static final String CB_SELLS_COMPLETED = "cbSellsCompleted";
  protected static final String CB_SELL_TIME = "cbSellTime";
  protected static final String CB_PAYS_COMPLETED = "cbPaysCompleted";
  protected static final String CB_PAY_TIME = "cbPayTime";

  protected static final String insTerminalStmt = "insTerminalStmtsExecuted";
  protected static final String insSourceStmt = "insSourceStmtsExecuted";
  protected static final String insSourceMessageLogStmt = "insSourceMessageLogStmtsExecuted";
  protected static final String insSingleTicketStmt = "insSingleTicketStmtsExecuted";
  protected static final String insTicketHistoryStmt = "insTicketHistoryStmtsExecuted";
  protected static final String insSourceMessageResultStmt = "insSourceMessageResultStmtsExecuted";
  protected static final String insTicketsToPoolStmt = "insTicketsToPoolStmtsExecuted";
  protected static final String selTerminalStatusStmt = "selTerminalStatusStmtsExecuted";
  protected static final String insTerminalBalanceTransactionLogStmt = "insTerminalBalanceTransactionLogStmtsExecuted";
  protected static final String selectTicketByTSNStmt = "selectTicketByTSNStmtsExecuted";
  protected static final String updTicketForPayStmt = "updTicketForPayStmtsExecuted";

  protected static final String insTerminalStmtTime = "insTerminalStmtTime";
  protected static final String insSourceStmtTime = "insSourceStmtTime";
  protected static final String insSourceMessageLogStmtTime = "insSourceMessageLogStmtTime";
  protected static final String insSingleTicketStmtTime = "insSingleTicketStmtTime";
  protected static final String insTicketHistoryStmtTime = "insTicketHistoryStmtTime";
  protected static final String insSourceMessageResultStmtTime = "insSourceMessageResultStmtTime";
  protected static final String insTicketsToPoolStmtTime = "insTicketsToPoolStmtTime";
  protected static final String selTerminalStatusStmtTime = "selTerminalStatusStmtTime";
  protected static final String insTerminalBalanceTransactionLogStmtTime = "insTerminalBalanceTransactionLogStmtTime";
  protected static final String selectTicketByTSNStmtTime = "selectTicketByTSNStmtTime";
  protected static final String updTicketForPayStmtTime = "updTicketForPayStmtTime";

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
        COMMITS_COMPLETED,
        "Number of commits completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        COMMIT_TIME,
        "Total time spent on commits (minus DML time).",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        CB_SELLS_COMPLETED,
        "Number of cash sells completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        CB_SELL_TIME,
        "Total time spent on cash sells that were completed (minus commit time).",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        CB_PAYS_COMPLETED,
        "Number of cash pays completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        CB_PAY_TIME,
        "Total time spent on cash pays that were completed (minus commit time).",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insTerminalStmt,
        "Number of insTerminalStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insTerminalStmtTime,
        "Total time spent executing insTerminalStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insSourceStmt,
        "Number of insSourceStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insSourceStmtTime,
        "Total time spent executing insSourceStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insSourceMessageLogStmt,
        "Number of insSourceMessageLogStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insSourceMessageLogStmtTime,
        "Total time spent executing insSourceMessageLogStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insSingleTicketStmt,
        "Number of insSingleTicketStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insSingleTicketStmtTime,
        "Total time spent executing insSingleTicketStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insTicketHistoryStmt,
        "Number of insTicketHistoryStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insTicketHistoryStmtTime,
        "Total time spent executing insTicketHistoryStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insSourceMessageResultStmt,
        "Number of insSourceMessageResultStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insSourceMessageResultStmtTime,
        "Total time spent executing insSourceMessageResultStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insTicketsToPoolStmt,
        "Number of insTicketsToPoolStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insTicketsToPoolStmtTime,
        "Total time spent executing insTicketsToPoolStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selTerminalStatusStmt,
        "Number of selTerminalStatusStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selTerminalStatusStmtTime,
        "Total time spent executing selTerminalStatusStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insTerminalBalanceTransactionLogStmt,
        "Number of insTerminalBalanceTransactionLogStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insTerminalBalanceTransactionLogStmtTime,
        "Total time spent executing insTerminalBalanceTransactionLogStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectTicketByTSNStmt,
        "Number of selectTicketByTSNStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectTicketByTSNStmtTime,
        "Total time spent executing selectTicketByTSNStmt.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        updTicketForPayStmt,
        "Number of updTicketForPayStmt executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        updTicketForPayStmtTime,
        "Total time spent executing updTicketForPayStmt.",
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

  public void endCommit(long start) {
    statistics().incInt(COMMITS_COMPLETED, 1);
    statistics().incLong(COMMIT_TIME, NanoTimer.getTime() - start);
  }

  public long startCBSell() {
    return NanoTimer.getTime();
  }

  public void endCBSell(long start) {
    statistics().incInt(CB_SELLS_COMPLETED, 1);
    statistics().incLong(CB_SELL_TIME, NanoTimer.getTime() - start);
  }

  public long startCBPay() {
    return NanoTimer.getTime();
  }

  public void endCBPay(long start) {
    statistics().incInt(CB_PAYS_COMPLETED, 1);
    statistics().incLong(CB_PAY_TIME, NanoTimer.getTime() - start);
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
      case insTerminal:
        statistics().incInt(insTerminalStmt, amount);
        statistics().incLong(insTerminalStmtTime, elapsed);
        break;
      case insSource:
        statistics().incInt(insSourceStmt, amount);
        statistics().incLong(insSourceStmtTime, elapsed);
        break;
      case insSourceMessageLog:
        statistics().incInt(insSourceMessageLogStmt, amount);
        statistics().incLong(insSourceMessageLogStmtTime, elapsed);
        break;
      case insSingleTicket:
        statistics().incInt(insSingleTicketStmt, amount);
        statistics().incLong(insSingleTicketStmtTime, elapsed);
        break;
      case insTicketHistory:
        statistics().incInt(insTicketHistoryStmt, amount);
        statistics().incLong(insTicketHistoryStmtTime, elapsed);
        break;
      case insSourceMessageResult:
        statistics().incInt(insSourceMessageResultStmt, amount);
        statistics().incLong(insSourceMessageResultStmtTime, elapsed);
        break;
      case insTicketsToPool:
        statistics().incInt(insTicketsToPoolStmt, amount);
        statistics().incLong(insTicketsToPoolStmtTime, elapsed);
        break;
      case selTerminalStatus:
        statistics().incInt(selTerminalStatusStmt, amount);
        statistics().incLong(selTerminalStatusStmtTime, elapsed);
        break;
      case insTerminalBalanceTransactionLog:
        statistics().incInt(insTerminalBalanceTransactionLogStmt, amount);
        statistics().incLong(insTerminalBalanceTransactionLogStmtTime, elapsed);
        break;
      case selectTicketByTSN:
        statistics().incInt(selectTicketByTSNStmt, amount);
        statistics().incLong(selectTicketByTSNStmtTime, elapsed);
        break;
      case updTicketForPay:
        statistics().incInt(updTicketForPayStmt, amount);
        statistics().incLong(updTicketForPayStmtTime, elapsed);
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
    if (histogram != null) {
      incHistogram(histogram, elapsed);
    }
  }
}
