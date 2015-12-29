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
package cacheLoader.hc;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.PerformanceStatistics;

/**
 * Perflication-defined statistics.
 */
public class PerfStats extends PerformanceStatistics {

  /** <code>PerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  /** The name of the "operationsCompleted" statistic */
  private static final String OPERATIONS_COMPLETED =
    "operationsCompleted";
  
  /** The name of the "operationTime" statistic */
  private static final String OPERATION_TIME = "operationTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>PerfStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntCounter
        ( 
         OPERATIONS_COMPLETED,
         "Number of operations completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OPERATION_TIME,
         "Total time spent doing operations.",
         "nanoseconds",
         !largerIsBetter
         )
    };
  }

  public static PerfStats getInstance() {
    return (PerfStats) getInstance( PerfStats.class, SCOPE );
  }
  public static PerfStats getInstance(String name) {
    return (PerfStats) getInstance(PerfStats.class, SCOPE, name);
  }
  public static PerfStats getInstance( String name, String trimspecName ) {
    return (PerfStats) getInstance( PerfStats.class, SCOPE, name, trimspecName );
  }

  /////////////////// Construction / initialization ////////////////

  public PerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getOperationsCompleted() {
     return statistics().getInt(OPERATIONS_COMPLETED);
   }
   public long getOperationTime() {
     return statistics().getLong(OPERATION_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the count on the operation
   */
  public void incOperationsCompleted() {
    incOperationsCompleted(1);
  }
  /**
   * increase the count on the operation by the supplied amount
   */
  public void incOperationsCompleted(int amount) {
    statistics().incInt(OPERATIONS_COMPLETED, amount);
  }
  /**
   * increase the time on the operation by the supplied amount
   */
  public void incOperationTime(long amount) {
    statistics().incLong(OPERATION_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startOperation() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the operation started 
   */
  public void endOperation(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(OPERATIONS_COMPLETED, 1);
    statistics().incLong(OPERATION_TIME, ts-start);
  }
}
