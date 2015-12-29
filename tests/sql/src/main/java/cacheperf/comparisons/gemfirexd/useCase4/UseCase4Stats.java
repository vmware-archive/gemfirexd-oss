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

package cacheperf.comparisons.gemfirexd.useCase4;

import cacheperf.comparisons.gemfirexd.useCase4.UseCase4Prms.QueryType;
import cacheperf.comparisons.gemfirexd.QueryPerfException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to UseCase4.
 */
public class UseCase4Stats extends PerformanceStatistics {

  /** <code>UseCase4Stats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  protected static final String accountProfiles = "accountprofiles";
  protected static final String accountProfileTime = "accountprofileTime";
  protected static final String accounts = "accounts";
  protected static final String accountTime = "accountTime";
  protected static final String orders = "orders";
  protected static final String orderTime = "orderTime";
  protected static final String holdings = "holdings";
  protected static final String holdingTime = "holdingTime";
  protected static final String quotes = "quotes";
  protected static final String quoteTime = "quoteTime";

  protected static final String queries = "queries";
  protected static final String queryTime = "queryTime";

  protected static final String holdingAgg = "holdingAggExecuted";
  protected static final String holdingAggResults = "holdingAggResults";
  protected static final String holdingAggNotFound = "holdingAggNotFound";
  protected static final String holdingAggTime = "holdingAggTime";
  protected static final String portSumm = "portSummExecuted";
  protected static final String portSummResults = "portSummResults";
  protected static final String portSummNotFound = "portSummNotFound";
  protected static final String portSummTime = "portSummTime";
  protected static final String mktSumm = "mktSummExecuted";
  protected static final String mktSummResults = "mktSummResults";
  protected static final String mktSummNotFound = "mktSummNotFound";
  protected static final String mktSummTime = "mktSummTime";
  protected static final String holdingCount = "holdingCountsExecuted";
  protected static final String holdingCountResults = "holdingCountResults";
  protected static final String holdingCountNotFound = "holdingCountNotFound";
  protected static final String holdingCountTime = "holdingCountTime";
  protected static final String uptClosedOrderSelfJoin = "uptClosedOrderSelfJoinExecuted";
  protected static final String uptClosedOrderSelfJoinResults = "uptClosedOrderSelfJoinResults";
  protected static final String uptClosedOrderSelfJoinNotFound = "uptClosedOrderSelfJoinNotFound";
  protected static final String uptClosedOrderSelfJoinTime = "uptClosedOrderSelfJoinTime";
  protected static final String uptClosedOrder = "uptClosedOrderExecuted";
  protected static final String uptClosedOrderResults = "uptClosedOrderResults";
  protected static final String uptClosedOrderNotFound = "uptClosedOrderNotFound";
  protected static final String uptClosedOrderTime = "uptClosedOrderTime";
  protected static final String findOrderByStatus = "findOrderByStatusExecuted";
  protected static final String findOrderByStatusResults = "findOrderByStatusResults";
  protected static final String findOrderByStatusNotFound = "findOrderByStatusNotFound";
  protected static final String findOrderByStatusTime = "findOrderByStatusTime";
  protected static final String findOrderByAccAccId = "findOrderByAccAccIdExecuted";
  protected static final String findOrderByAccAccIdResults = "findOrderByAccAccIdResults";
  protected static final String findOrderByAccAccIdNotFound = "findOrderByAccAccIdNotFound";
  protected static final String findOrderByAccAccIdTime = "findOrderByAccAccIdTime";
  protected static final String findOrderIdAndAccAccId = "findOrderIdAndAccAccIdExecuted";
  protected static final String findOrderIdAndAccAccIdResults = "findOrderIdAndAccAccIdResults";
  protected static final String findOrderIdAndAccAccIdNotFound = "findOrderIdAndAccAccIdNotFound";
  protected static final String findOrderIdAndAccAccIdTime = "findOrderIdAndAccAccIdTime";
  protected static final String findOrderCntAccAccId = "findOrderCntAccAccIdExecuted";
  protected static final String findOrderCntAccAccIdResults = "findOrderCntAccAccIdResults";
  protected static final String findOrderCntAccAccIdNotFound = "findOrderCntAccAccIdNotFound";
  protected static final String findOrderCntAccAccIdTime = "findOrderCntAccAccIdTime";
  protected static final String findOrderCntAccAccIdAndStatus = "findOrderCntAccAccIdAndStatusExecuted";
  protected static final String findOrderCntAccAccIdAndStatusResults = "findOrderCntAccAccIdAndStatusResults";
  protected static final String findOrderCntAccAccIdAndStatusNotFound = "findOrderCntAccAccIdAndStatusNotFound";
  protected static final String findOrderCntAccAccIdAndStatusTime = "findOrderCntAccAccIdAndStatusTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>UseCase4Stats</code>.
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
        accountProfiles,
        "Number of accountprofiles created.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        accountProfileTime,
        "Total time spent creating accountprofiles.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        accounts,
        "Number of accounts created.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        accountTime,
        "Total time spent creating accounts.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        orders,
        "Number of orders created.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        orderTime,
        "Total time spent creating orders.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        holdings,
        "Number of holdings created.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        holdingTime,
        "Total time spent creating holdings.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        quotes,
        "Number of quotes created.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        quoteTime,
        "Total time spent creating quotes.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        queries,
        "Number of queries executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        queryTime,
        "Total time spent executing queries.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingAgg,
        "Number of holdingAgg statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingAggResults,
        "Number of holdingAgg results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingAggNotFound,
        "Number of holdingAgg statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        holdingAggTime,
        "Total time spent executing holdingAgg statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        portSumm,
        "Number of portSumm statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        portSummResults,
        "Number of portSumm results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        portSummNotFound,
        "Number of portSumg statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        portSummTime,
        "Total time spent executing portSumm statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        mktSumm,
        "Number of mktSumm statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        mktSummResults,
        "Number of mktSumm results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        mktSummNotFound,
        "Number of mktSumm statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        mktSummTime,
        "Total time spent executing mktSumm statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingCount,
        "Number of holdingCount statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingCountResults,
        "Number of holdingCount results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        holdingCountNotFound,
        "Number of holdingCount statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        holdingCountTime,
        "Total time spent executing holdingCount statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrderSelfJoin,
        "Number of uptClosedOrderSelfJoin statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrderSelfJoinResults,
        "Number of uptClosedOrderSelfJoin results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrderSelfJoinNotFound,
        "Number of uptClosedOrderSelfJoin statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        uptClosedOrderSelfJoinTime,
        "Total time spent executing uptClosedOrderSelfJoin statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrder,
        "Number of uptClosedOrder statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrderResults,
        "Number of uptClosedOrder results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        uptClosedOrderNotFound,
        "Number of uptClosedOrder statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        uptClosedOrderTime,
        "Total time spent executing uptClosedOrder statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByStatus,
        "Number of findOrderByStatus statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByStatusResults,
        "Number of findOrderByStatus results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByStatusNotFound,
        "Number of findOrderByStatus statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        findOrderByStatusTime,
        "Total time spent executing findOrderByStatus statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByAccAccId,
        "Number of findOrderByAccAccId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByAccAccIdResults,
        "Number of findOrderByAccAccId results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderByAccAccIdNotFound,
        "Number of findOrderByAccAccId statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        findOrderByAccAccIdTime,
        "Total time spent executing findOrderByAccAccId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderIdAndAccAccId,
        "Number of findOrderIdAndAccAccId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderIdAndAccAccIdResults,
        "Number of findOrderIdAndAccAccId results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderIdAndAccAccIdNotFound,
        "Number of findOrderIdAndAccAccId statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        findOrderIdAndAccAccIdTime,
        "Total time spent executing findOrderIdAndAccAccId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccId,
        "Number of findOrderCntAccAccId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccIdResults,
        "Number of findOrderCntAccAccId results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccIdNotFound,
        "Number of findOrderCntAccAccId statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        findOrderCntAccAccIdTime,
        "Total time spent executing findOrderCntAccAccId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccIdAndStatus,
        "Number of findOrderCntAccAccIdAndStatus statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccIdAndStatusResults,
        "Number of findOrderCntAccAccIdAndStatus results found.",
        "executions",
        largerIsBetter
      ),
      factory().createIntCounter
      (
        findOrderCntAccAccIdAndStatusNotFound,
        "Number of findOrderCntAccAccIdAndStatus statements with data not found.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        findOrderCntAccAccIdAndStatusTime,
        "Total time spent executing findOrderCntAccAccIdAndStatus statements.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static UseCase4Stats getInstance() {
    UseCase4Stats tps =
         (UseCase4Stats)getInstance(UseCase4Stats.class, SCOPE);
    tps.incVMCount();
    return tps;
  }
  public static UseCase4Stats getInstance(String name) {
    UseCase4Stats tps =
         (UseCase4Stats)getInstance(UseCase4Stats.class, SCOPE, name);
    tps.incVMCount();
    return tps;
  }
  public static UseCase4Stats getInstance( String name, String trimspecName ) {
    UseCase4Stats tps =
         (UseCase4Stats)getInstance(UseCase4Stats.class, SCOPE, name, trimspecName);
    tps.incVMCount();
    return tps;
  }

/////////////////// Construction / initialization ////////////////

  public UseCase4Stats( Class cls, StatisticsType type, int scope,
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

// accountprofiles -------------------------------------------------------------

  public long startAccountProfile() {
    return NanoTimer.getTime();
  }
  public void endAccountProfile(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(accountProfiles, amount);
    statistics().incLong(accountProfileTime, elapsed);
  }

// accounts --------------------------------------------------------------------

  public long startAccount() {
    return NanoTimer.getTime();
  }
  public void endAccount(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(accounts, amount);
    statistics().incLong(accountTime, elapsed);
  }

// orders ----------------------------------------------------------------------

  public long startOrder() {
    return NanoTimer.getTime();
  }
  public void endOrder(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(orders, amount);
    statistics().incLong(orderTime, elapsed);
  }

// holdings --------------------------------------------------------------------

  public long startHolding() {
    return NanoTimer.getTime();
  }
  public void endHolding(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(holdings, amount);
    statistics().incLong(holdingTime, elapsed);
  }

// quotes ----------------------------------------------------------------------

  public long startQuote() {
    return NanoTimer.getTime();
  }
  public void endQuote(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(quotes, amount);
    statistics().incLong(quoteTime, elapsed);
  }

// statements ------------------------------------------------------------------

  public long startQuery() {
    return NanoTimer.getTime();
  }

  public void endQuery(QueryType queryType, long start, int results,
                       HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(queries, 1);
    statistics().incLong(queryTime, elapsed);
    switch (queryType) {
      case holdingAgg:
        statistics().incInt(holdingAgg, 1);
        statistics().incLong(holdingAggTime, elapsed);
        if (results == 0) {
          statistics().incInt(holdingAggNotFound, 1);
        } else {
          statistics().incInt(holdingAggResults, results);
        }
        break;
      case portSumm:
        statistics().incInt(portSumm, 1);
        statistics().incLong(portSummTime, elapsed);
        if (results == 0) {
          statistics().incInt(portSummNotFound, 1);
        } else {
          statistics().incInt(portSummResults, results);
        }
        break;
      case mktSumm:
        statistics().incInt(mktSumm, 1);
        statistics().incLong(mktSummTime, elapsed);
        if (results == 0) {
          statistics().incInt(mktSummNotFound, 1);
        } else {
          statistics().incInt(mktSummResults, results);
        }
        break;
      case holdingCount:
        statistics().incInt(holdingCount, 1);
        statistics().incLong(holdingCountTime, elapsed);
        if (results == 0) {
          statistics().incInt(holdingCountNotFound, 1);
        } else {
          statistics().incInt(holdingCountResults, results);
        }
        break;
      case uptClosedOrderSelfJoin:
        statistics().incInt(uptClosedOrderSelfJoin, 1);
        statistics().incLong(uptClosedOrderTime, elapsed);
        if (results == 0) {
          statistics().incInt(uptClosedOrderSelfJoinNotFound, 1);
        } else {
          statistics().incInt(uptClosedOrderSelfJoinResults, results);
        }
        break;
      case uptClosedOrder:
        statistics().incInt(uptClosedOrder, 1);
        statistics().incLong(uptClosedOrderTime, elapsed);
        if (results == 0) {
          statistics().incInt(uptClosedOrderNotFound, 1);
        } else {
          statistics().incInt(uptClosedOrderResults, results);
        }
        break;
      case findOrderByStatus:
        statistics().incInt(findOrderByStatus, 1);
        statistics().incLong(findOrderByStatusTime, elapsed);
        if (results == 0) {
          statistics().incInt(findOrderByStatusNotFound, 1);
        } else {
          statistics().incInt(findOrderByStatusResults, results);
        }
        break;
      case findOrderByAccAccId:
        statistics().incInt(findOrderByAccAccId, 1);
        statistics().incLong(findOrderByAccAccIdTime, elapsed);
        if (results == 0) {
          statistics().incInt(findOrderByAccAccIdNotFound, 1);
        } else {
          statistics().incInt(findOrderByAccAccIdResults, results);
        }
        break;
      case findOrderIdAndAccAccId:
        statistics().incInt(findOrderIdAndAccAccId, 1);
        statistics().incLong(findOrderIdAndAccAccIdTime, elapsed);
        if (results == 0) {
          statistics().incInt(findOrderIdAndAccAccIdNotFound, 1);
        } else {
          statistics().incInt(findOrderIdAndAccAccIdResults, results);
        }
        break;
      case findOrderCntAccAccId:
        statistics().incInt(findOrderCntAccAccId, 1);
        statistics().incLong(findOrderCntAccAccIdTime, elapsed);
        if (results == 0) {
          statistics().incInt(findOrderCntAccAccIdNotFound, 1);
        } else {
          statistics().incInt(findOrderCntAccAccIdResults, results);
        }
        break;
      case findOrderCntAccAccIdAndStatus:
        statistics().incInt(findOrderCntAccAccIdAndStatus, 1);
        statistics().incLong(findOrderCntAccAccIdAndStatusTime, elapsed);
        if (results == 0) {
          statistics().incInt(findOrderCntAccAccIdAndStatusNotFound, 1);
        } else {
          statistics().incInt(findOrderCntAccAccIdAndStatusResults, results);
        }
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
    incHistogram(histogram, elapsed);
  }
}
