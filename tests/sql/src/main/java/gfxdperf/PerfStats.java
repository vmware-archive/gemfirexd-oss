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

package gfxdperf;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;

import distcache.DistCache;

import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class PerfStats extends PerformanceStatistics {

  /** <code>PerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  public static final String OPS = "operations";
  public static final String OP_TIME = "operationTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>PerfStats</code>
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
         OPS,
         "Number of operations completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OP_TIME,
         "Total time spent doing operations.",
         "nanoseconds",
	 !largerIsBetter
         ),
    };
  }

  public static PerfStats getInstance() {
    PerfStats cps =
         (PerfStats)getInstance(PerfStats.class, SCOPE);
    cps.incVMCount();
    return cps;
  }
  public static PerfStats getInstance(int scope) {
    PerfStats cps =
         (PerfStats)getInstance(PerfStats.class, scope);
    cps.incVMCount();
    return cps;
  }
  public static PerfStats getInstance(String name) {
    PerfStats cps =
         (PerfStats)getInstance(PerfStats.class, SCOPE, name);
    cps.incVMCount();
    return cps;
  }
  public static PerfStats getInstance( String name, String trimspecName ) {
    PerfStats cps =
         (PerfStats)getInstance(PerfStats.class, SCOPE, name, trimspecName);
    cps.incVMCount();
    return cps;
  }

  /////////////////// Construction / initialization ////////////////

  public PerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getOps() {
     return statistics().getInt(OPS);
   }
   public long getOpTime() {
     return statistics().getLong(OP_TIME);
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
   * sets the count on the ops to the supplied amount
   * (for statless clients only)
   */
  protected void setOps(int amount) {
    statistics().setInt(OPS, amount);
  }
  /**
   * sets the time on the ops to the supplied amount
   * (for statless clients only)
   */
  protected void setOpTime(long amount) {
    statistics().setLong(OP_TIME, amount);
  }
}
