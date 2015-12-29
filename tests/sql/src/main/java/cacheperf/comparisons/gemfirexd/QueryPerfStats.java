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

package cacheperf.comparisons.gemfirexd;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class QueryPerfStats extends PerformanceStatistics {

  /** <code>QueryPerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  // query statistics
  protected static final String PREPARES       = "prepares";
  protected static final String PREPARE_TIME   = "prepareTime";
  
  protected static final String QUERIES        = "queries";
  protected static final String QUERY_TIME     = "queryTime";
  
  protected static final String CREATES         = "creates";
  protected static final String CREATE_TIME     = "createTime";
  
  protected static final String UPDATES         = "updates";
  protected static final String UPDATE_TIME     = "updateTime";
  
  protected static final String COMMITS         = "commits";
  protected static final String COMMIT_TIME     = "commitTime";
  
  protected static final String ORMS            = "ORMs";
  protected static final String ORM_TIME        = "ORMTime";
 
  protected static final String RESULTS         = "results";
  
  protected static final String EMPTY_TABLE_MEM_USE =  "emptyTableMemUse";
  protected static final String LOADED_TABLE_MEM_USE = "loadedTableMemUse";
  protected static final String TABLE_MEM_USE = "tableMemUse";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>QueryPerfStats</code>.
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
        CREATES,
        "Number of creates completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        CREATE_TIME,
        "Total time spent performing creates.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        PREPARES,
        "Number of statements prepared.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        PREPARE_TIME,
        "Total time spent preparing statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        QUERIES,
        "Number of queries completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        QUERY_TIME,
        "Total time spent performing queries.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        UPDATES,
        "Number of updates completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        UPDATE_TIME,
        "Total time spent performing updates.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        COMMITS,
        "Number of statements committed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        COMMIT_TIME,
        "Total time spent committing.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        ORMS,
        "Number of ORMs completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      ( 
        ORM_TIME,
        "Total time spent performing ORMs.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      ( 
        RESULTS,
        "Result set size.",
        "rows",
        largerIsBetter
      ),
      factory().createLongGauge
      ( 
        EMPTY_TABLE_MEM_USE,
        "Memory used when tables are empty.",
        "bytes",
        !largerIsBetter
      ),
      factory().createLongGauge
      ( 
        LOADED_TABLE_MEM_USE,
        "Memory used when table data is loaded",
        "bytes",
        !largerIsBetter
      ),
      factory().createLongGauge
      ( 
        TABLE_MEM_USE,
        "Memory used by table data",
        "bytes",
        !largerIsBetter
      )
    };
  }

  public static QueryPerfStats getInstance() {
    QueryPerfStats qps =
         (QueryPerfStats)getInstance(QueryPerfStats.class, SCOPE);
    qps.incVMCount();
    return qps;
  }
  public static QueryPerfStats getInstance(String name) {
    QueryPerfStats qps =
         (QueryPerfStats)getInstance(QueryPerfStats.class, SCOPE, name);
    qps.incVMCount();
    return qps;
  }
  public static QueryPerfStats getInstance( String name, String trimspecName ) {
    QueryPerfStats qps =
         (QueryPerfStats)getInstance(QueryPerfStats.class, SCOPE, name, trimspecName);
    qps.incVMCount();
    return qps;
  }

  /////////////////// Construction / initialization ////////////////

  public QueryPerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

  public int getCreates() {
    return statistics().getInt(CREATES);
  }
  public long getCREATETime() {
    return statistics().getLong(CREATE_TIME);
  }
  
  public int getPrepares() {
    return statistics().getInt(PREPARES);
  }
  public long getPrepareTime() {
    return statistics().getLong(PREPARE_TIME);
  }
  
  public int getQueries() {
    return statistics().getInt(QUERIES);
  }
  public long getQUERYTime() {
    return statistics().getLong(QUERY_TIME);
  }
  
  public int getUpdates() {
    return statistics().getInt(UPDATES);
  }
  public long getUPDATETime() {
    return statistics().getLong(UPDATE_TIME);
  }
  
  public int getCommits() {
    return statistics().getInt(COMMITS);
  }
  public long getCommitTime() {
    return statistics().getLong(COMMIT_TIME);
  }
  
  public int getORMs() {
    return statistics().getInt(ORMS);
  }
  public long getORMTime() {
    return statistics().getLong(ORM_TIME);
  }
  
  public long getEmptyTableMemUse() {
    return statistics().getLong(EMPTY_TABLE_MEM_USE);
  }
  
  public long getLoadedTableMemUse() {
    return statistics().getLong(LOADED_TABLE_MEM_USE);
  }
  
  public long getTableMemUse() {
    return statistics().getLong(TABLE_MEM_USE);
  }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the time on the optional histogram by the supplied amount
   */
  public void incHistogram(HistogramStats histogram, long amount) {
    if (histogram != null) {
      histogram.incBin(amount);
    }
  }

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

   /**
    * increase the count on the queries
    */
   public void incCreates() {
     incCreates(1);
   }
   /**
    * increase the count on the queries by the supplied amount
    */
   public void incCreates(int amount) {
     statistics().incInt(CREATES, amount);
   }
   /**
    * increase the time on the queries by the supplied amount
    */
   public void incCreateTime(long amount) {
     statistics().incLong(CREATE_TIME, amount);
   }
   /**
    * @return the timestamp that marks the start of the query
    */
   public long startCreate() {
     return NanoTimer.getTime();
   }
   /**
    * @param start the timestamp taken when the queries started 
    */
   public void endCreate(long start, HistogramStats histogram) {
     long ts = NanoTimer.getTime();
     statistics().incInt(CREATES, 1);
     long elapsed = ts-start;
     statistics().incLong(CREATE_TIME, elapsed);
    incHistogram(histogram, elapsed);
   }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the create started
   */
  public void endCreate(long start, int amount, HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCreates(amount);
    incCreateTime(elapsed);
    incHistogram(histogram, elapsed);
  }

   /**
    * increase the count on the prepares
    */
   public void incPrepares() {
     incPrepares(1);
   }
   /**
    * increase the count on the prepares by the supplied amount
    */
   public void incPrepares(int amount) {
     statistics().incInt(PREPARES, amount);
   }
   /**
    * increase the time on the prepares by the supplied amount
    */
   public void incPrepareTime(long amount) {
     statistics().incLong(PREPARE_TIME, amount);
   }
   /**
    * @return the timestamp that marks the start of the prepare
    */
   public long startPrepare() {
     return NanoTimer.getTime();
   }
   /**
    * @param start the timestamp taken when the prepares started 
    */
   public void endPrepare(long start, HistogramStats histogram) {
     long ts = NanoTimer.getTime();
     statistics().incInt(PREPARES, 1);
     long elapsed = ts-start;
     statistics().incLong(PREPARE_TIME, elapsed);
     incHistogram(histogram, elapsed);
   }
 
/////////////////// Updating Query stats /////////////////////////
   /**
    * increase the count on the queries
    */
   public void incQueries() {
     incQueries(1);
   }
   /**
    * increase the count on the queries by the supplied amount
    */
   public void incQueries(int amount) {
     statistics().incInt(QUERIES, amount);
   }
   /**
    * increase the time on the queries by the supplied amount
    */
   public void incQueryTime(long amount) {
     statistics().incLong(QUERY_TIME, amount);
   }
   /**
    * @return the timestamp that marks the start of the query
    */
   public long startQuery() {
     return NanoTimer.getTime();
   }
   /**
    * @param start the timestamp taken when the queries started 
    */
   public void endQuery(long start, HistogramStats histogram) {
     endQuery(start, 0, histogram);
   }
   /**
    * @param start the timestamp taken when the queries started 
    */
   public void endQuery(long start, int results, HistogramStats histogram) {
     long ts = NanoTimer.getTime();
     statistics().incInt(QUERIES, 1);
     long elapsed = ts-start;
     statistics().incLong(QUERY_TIME, elapsed);
     if (results > 0) statistics().incInt(RESULTS, results);
     incHistogram(histogram, elapsed);
   }
 
   // Update Stats
   
   /**
    * increase the count on the updates
    */
   public void incUpdates() {
     incUpdates(1);
   }
   /**
    * increase the count on the updates by the supplied amount
    */
   public void incUpdates(int amount) {
     statistics().incInt(UPDATES, amount);
   }
   /**
    * increase the time on the updates by the supplied amount
    */
   public void incUpdateTime(long amount) {
     statistics().incLong(UPDATE_TIME, amount);
   }
   /**
    * @return the timestamp that marks the start of the query
    */
   public long startUpdate() {
     return NanoTimer.getTime();
   }
   /**
    * @param start the timestamp taken when the updates started 
    */
   public void endUpdate(long start, HistogramStats histogram) {
     long ts = NanoTimer.getTime();
     statistics().incInt(UPDATES, 1);
     long elapsed = ts-start;
     statistics().incLong(UPDATE_TIME, elapsed);
     incHistogram(histogram, elapsed);
   }

  /**
   * increase the count on the commits
   */
  public void incCommits() {
    incCommits(1);
  }
  /**
   * increase the count on the commits by the supplied amount
   */
  public void incCommits(int amount) {
    statistics().incInt(COMMITS, amount);
  }
  /**
   * increase the time on the commits by the supplied amount
   */
  public void incCommitTime(long amount) {
    statistics().incLong(COMMIT_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the commit
   */
  public long startCommit() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the commit started 
   */
  public void endCommit(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(COMMITS, 1);
    long elapsed = ts-start;
    statistics().incLong(COMMIT_TIME, elapsed);
  }
 
   /**
    * increase the count on the ORMs
    */
   public void incORMs() {
     incORMs(1);
   }
   /**
    * increase the count on the ORMs by the supplied amount
    */
   public void incORMs(int amount) {
     statistics().incInt(ORMS, amount);
   }
   /**
    * increase the time on the ORMs by the supplied amount
    */
   public void incORMTime(long amount) {
     statistics().incLong(ORM_TIME, amount);
   }
   /**
    * @return the timestamp that marks the start of the ORM
    */
   public long startORM() {
     return NanoTimer.getTime();
   }
   /**
    * @param start the timestamp taken when the ORMs started 
    */
   public void endORM(long start, HistogramStats histogram) {
     long ts = NanoTimer.getTime();
     statistics().incInt(ORMS, 1);
     long elapsed = ts-start;
     statistics().incLong(ORM_TIME, elapsed);
     incHistogram(histogram, elapsed);
   }
   
   public void setEmptyTableMemUse(long emptyTableMemUse) {
     statistics().setLong(EMPTY_TABLE_MEM_USE, emptyTableMemUse); 
   }
   
   public void setLoadedTableMemUse(long loadedTableMemUse) {
     long emptyTableMemUse = statistics().getLong(EMPTY_TABLE_MEM_USE);
     statistics().setLong(LOADED_TABLE_MEM_USE, loadedTableMemUse); 
     statistics().setLong(TABLE_MEM_USE, loadedTableMemUse - emptyTableMemUse); 
   }
}
