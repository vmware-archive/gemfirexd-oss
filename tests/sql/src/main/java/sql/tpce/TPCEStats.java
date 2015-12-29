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
package sql.tpce;

import cacheperf.comparisons.gemfirexd.QueryPerfException;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;

import perffmwk.PerformanceStatistics;
import sql.poc.useCase2.perf.QueryPerfStats;

public class TPCEStats extends PerformanceStatistics {
  private static final int SCOPE = THREAD_SCOPE;

  // query statistics
  //protected static final String TRADEORDER = "tradeorder";
  //protected static final String QUERY_TIME     = "queryTime";
  //protected static final String SELECTQUERIES         = "selectqueries";
  //protected static final String SELECT_QUERY_TIME     = "selectqueryTime";
  protected static final String TRADEORDERTXN = "tradeordertxn";
  protected static final String TRADE_ORDER_TXN_TIME = "tradeOrderTxnTime";
  protected static final String TRADERESULTTXN = "traderesulttxn";
  protected static final String TRADE_RESULT_TXN_TIME = "tradeResultTxnTime";
  protected static final String MARKETFEEDTXN = "marketfeedtxn";
  protected static final String MARKET_FEED_TXN_TIME = "marketFeedTxnTime";
  
  protected static final String MARKET_FEED_TXN_IN_PROGRESS = "marketFeedTxnInProgress";
  protected static final String TRADE_ORDER_TXN_IN_PROGRESS = "tradeOrderTxnInProgress";
  protected static final String TRADE_RESULT_TXN_IN_PROGRESS = "tradeResultTxnInProgress";
  
  protected static final String MARKET_FEED_TXN_COMPLETED = "marketFeedTxnCompleted";
  protected static final String TRADE_ORDER_TXN_COMPLETED = "tradeOrderTxnCompleted";
  protected static final String TRADE_RESULT_TXN_COMPLETED = "tradeResultTxnCompleted";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>QueryPerfStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntCounter
        ( 
         TRADE_ORDER_TXN_COMPLETED,
         "Number of trade_order txn completed.",
         "operations",
          largerIsBetter
         ),
      factory().createLongCounter
        ( 
         TRADE_ORDER_TXN_TIME,
         "Total time spent performing trade_order txn.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntGauge
         (
           TRADE_ORDER_TXN_IN_PROGRESS,
           "The number of trade_order txn in progress.",
           "operations"
         ),
      factory().createIntCounter
         ( 
          TRADE_RESULT_TXN_COMPLETED,
          "Number of trade_result txn completed.",
          "operations",
           largerIsBetter
          ),
       factory().createLongCounter
         ( 
          TRADE_RESULT_TXN_TIME,
          "Total time spent performing trade_result txn.",
          "nanoseconds",
          !largerIsBetter
          ),
      factory().createIntGauge
          (
            TRADE_RESULT_TXN_IN_PROGRESS,
            "The number of trade_result txn in progress.",
            "operations"
          ),
       factory().createIntCounter
          ( 
           MARKET_FEED_TXN_COMPLETED,
           "Number of market_feed txn completed.",
           "operations",
            largerIsBetter
           ),
        factory().createLongCounter
          ( 
           MARKET_FEED_TXN_TIME,
           "Total time spent performing market_feed txn.",
           "nanoseconds",
           !largerIsBetter
           ),
        factory().createIntGauge
           (
             MARKET_FEED_TXN_IN_PROGRESS,
             "The number of market_feed txn in progress.",
             "operations"
           ),
    };
  }

  public static TPCEStats getInstance() {
    return (TPCEStats) getInstance( TPCEStats.class, SCOPE );
  }
  public static TPCEStats getInstance(String name) {
    return (TPCEStats) getInstance(TPCEStats.class, SCOPE, name);
  }
  public static TPCEStats getInstance( String name, String trimspecName ) {
    return (TPCEStats) getInstance(TPCEStats.class, SCOPE, name, trimspecName );
  }

  /////////////////// Construction / initialization ////////////////

  public TPCEStats( Class<?> cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getTradeOrder() {
     return statistics().getInt(TRADEORDERTXN);
   }
   public long getTradeOrderTime() {
     return statistics().getLong(TRADE_ORDER_TXN_TIME);
   }
   
   public int getTradeResultQueries() {
     return statistics().getInt(TRADERESULTTXN);
   }
   public long getTradeResultTime() {
     return statistics().getLong(TRADE_RESULT_TXN_TIME);
   }
   
   public int getMarketFeedQueries() {
     return statistics().getInt(MARKETFEEDTXN);
   }
   public long getMarketFeedTime() {
     return statistics().getLong(MARKET_FEED_TXN_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the count on the trade_order txn
   */
  public void incTradeOrder() {
    incTradeOrder(1);
  }
  /**
   * increase the count on the trade_order txn by the supplied amount
   */
  public void incTradeOrder(int amount) {
    statistics().incInt(TRADEORDERTXN, amount);
  }
  /**
   * increase the time on the trade_order txn by the supplied amount
   */
  public void incTradeOrderTime(long amount) {
    statistics().incLong(TRADE_ORDER_TXN_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the trade_order txn
   */
  public long startTradeOrder() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the trade_order txn started 
   */
  public void endTradeOrder(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(TRADEORDERTXN, 1);
    long elapsed = ts-start;
    statistics().incLong(TRADE_ORDER_TXN_TIME, elapsed);
  }
  
  
  /**
   * increase the count on the trade_result txn
   */
  public void incTradeResult() {
    incTradeResult(1);
  }
  /**
   * increase the count on the trade_result txn by the supplied amount
   */
  public void incTradeResult(int amount) {
    statistics().incInt(TRADERESULTTXN, amount);
  }
  /**
   * increase the time on the trade_result txn by the supplied amount
   */
  public void incTradeResultTime(long amount) {
    statistics().incLong(TRADE_RESULT_TXN_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the trade_result txn
   */
  public long startTradeResult() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the trade_result txn started 
   */
  public void endTradeResult(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(TRADERESULTTXN, 1);
    long elapsed = ts-start;
    statistics().incLong(TRADE_RESULT_TXN_TIME, elapsed);
  }
  
  /**
   * increase the count on the market_feed txn
   */
  public void incMarketFeed() {
    incMarketFeed(1);
  }
  /**
   * increase the count on the market_feed txn by the supplied amount
   */
  public void incMarketFeed(int amount) {
    statistics().incInt(MARKETFEEDTXN, amount);
  }
  /**
   * increase the time on the market_feed txn by the supplied amount
   */
  public void incMarketFeedTime(long amount) {
    statistics().incLong(MARKET_FEED_TXN_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the market_feed txn
   */
  public long startMarketFeed() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the market_feed txn started 
   */
  public void endMarketFeed(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(MARKETFEEDTXN, 1);
    long elapsed = ts-start;
    statistics().incLong(MARKET_FEED_TXN_TIME, elapsed);
  }
  
  public long startTransaction(int txnType) {
    switch (txnType) {
      case TPCETest.TRADE_ORDER:
        statistics().incInt(TRADE_ORDER_TXN_IN_PROGRESS, 1);
        break;
      case TPCETest.TRADE_RESULT:
        statistics().incInt(TRADE_RESULT_TXN_IN_PROGRESS, 1);
        break;
      case TPCETest.MARKET_FEED:
        statistics().incInt(MARKET_FEED_TXN_IN_PROGRESS, 1);
        break;

      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
    return NanoTimer.getTime();
  }
  
  public void endTransaction(int txType, long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    switch (txType) {
      case TPCETest.TRADE_ORDER:
        statistics().incInt(TRADE_ORDER_TXN_IN_PROGRESS, -1);
        statistics().incInt(TRADE_ORDER_TXN_COMPLETED, 1);
        statistics().incLong(TRADE_ORDER_TXN_TIME, elapsed);
        break;
      case TPCETest.TRADE_RESULT:
        statistics().incInt(TRADE_RESULT_TXN_IN_PROGRESS, -1);
        statistics().incInt(TRADE_RESULT_TXN_COMPLETED, 1);
        statistics().incLong(TRADE_RESULT_TXN_TIME, elapsed);
        break;
      case TPCETest.MARKET_FEED:
        statistics().incInt(MARKET_FEED_TXN_IN_PROGRESS, -1);
        statistics().incInt(MARKET_FEED_TXN_COMPLETED, 1);
        statistics().incLong(MARKET_FEED_TXN_TIME, elapsed);
        break;

      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
  }
  
}
