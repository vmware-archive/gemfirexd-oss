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

package cacheperf.comparisons.query;

//import cacheperf.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
//import java.util.*;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class QueryPerfStats extends PerformanceStatistics {

  /** <code>QueryPerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  // query statistics
  protected static final String QUERIES         = "queries";
  protected static final String QUERY_TIME     = "queryTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>QueryPerfStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
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
         )
    };
  }

  public static QueryPerfStats getInstance() {
    return (QueryPerfStats) getInstance( QueryPerfStats.class, SCOPE );
  }
  public static QueryPerfStats getInstance(String name) {
    return (QueryPerfStats) getInstance(QueryPerfStats.class, SCOPE, name);
  }
  public static QueryPerfStats getInstance( String name, String trimspecName ) {
    return (QueryPerfStats) getInstance(QueryPerfStats.class, SCOPE, name, trimspecName );
  }

  /////////////////// Construction / initialization ////////////////

  public QueryPerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getQueries() {
     return statistics().getInt(QUERIES);
   }
   public long getQUERYTime() {
     return statistics().getLong(QUERY_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

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
  public void endQuery(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(QUERIES, 1);
    long elapsed = ts-start;
    statistics().incLong(QUERY_TIME, elapsed);
  }
}
