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

package cacheperf.comparisons.putAll;

//import cacheperf.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
//import java.util.*;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class PutAllPerfStats extends PerformanceStatistics {

  /** <code>PutAllPerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  // putAll statistics
  protected static final String PUTALL_ENTRIES  = "putAllEntries";
  protected static final String PUTALL_TIME     = "putAllTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>PutAllPerfStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createLongCounter
        ( 
         PUTALL_ENTRIES,
         "Number of putAll entries completed.",
         "entries",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         PUTALL_TIME,
         "Total time spent performing putAll operation.",
         "nanoseconds",
	 !largerIsBetter
         )
    };
  }

  public static PutAllPerfStats getInstance() {
    return (PutAllPerfStats) getInstance( PutAllPerfStats.class, SCOPE );
  }
  public static PutAllPerfStats getInstance(String name) {
    return (PutAllPerfStats) getInstance(PutAllPerfStats.class, SCOPE, name);
  }
  public static PutAllPerfStats getInstance( String name, String trimspecName ) {
    return (PutAllPerfStats) getInstance(PutAllPerfStats.class, SCOPE, name, trimspecName );
  }

  /////////////////// Construction / initialization ////////////////

  public PutAllPerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public long getPutAllEntries() {
     return statistics().getLong(PUTALL_ENTRIES);
   }
   public long getPutAllTime() {
     return statistics().getLong(PUTALL_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the count on the entries
   */
  public void incPutAllEntries() {
    incPutAllEntries(1);
  }
  /**
   * increase the count on the entries by the supplied amount
   */
  public void incPutAllEntries(int amount) {
    statistics().incLong(PUTALL_ENTRIES, amount);
  }
  /**
   * increase the time on the putAll operation by the supplied amount
   */
  public void incPutAllTime(long amount) {
    statistics().incLong(PUTALL_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the putAll operations
   */
  public long startPutAll() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the putAll operation started 
   */
  public void endPutAll(long start, int mapSize) {
    long ts = NanoTimer.getTime();
    statistics().incLong(PUTALL_ENTRIES, mapSize);
    long elapsed = ts-start;
    statistics().incLong(PUTALL_TIME, elapsed);
  }
}
