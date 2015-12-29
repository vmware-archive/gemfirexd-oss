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

package cacheperf.comparisons.gemfirexd.misc;

import cacheperf.comparisons.gemfirexd.QueryPerfException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to Carter.
 */
public class CarterStats extends PerformanceStatistics {

  /** <code>CarterStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  protected static final String SALES        = "sales";
  protected static final String SALE_TIME    = "saleTime";
  protected static final String SALE_ROLLBACKS = "saleRollbacks";
  protected static final String SALE_ROLLBACK_TIME = "saleRollbackTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>CarterStats</code>.
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
        SALES,
        "Number of sales recorded.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        SALE_TIME,
        "Total time spent recording sales.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        SALE_ROLLBACKS,
        "Number of sales rolled back.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        SALE_ROLLBACK_TIME,
        "Total time spent rolling back sales.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static CarterStats getInstance() {
    CarterStats cstats =
         (CarterStats)getInstance(CarterStats.class, SCOPE);
    cstats.incVMCount();
    return cstats;
  }
  public static CarterStats getInstance(String name) {
    CarterStats cstats =
         (CarterStats)getInstance(CarterStats.class, SCOPE, name);
    cstats.incVMCount();
    return cstats;
  }
  public static CarterStats getInstance( String name, String trimspecName ) {
    CarterStats cstats =
         (CarterStats)getInstance(CarterStats.class, SCOPE, name, trimspecName);
    cstats.incVMCount();
    return cstats;
  }

/////////////////// Construction / initialization ////////////////

  public CarterStats( Class cls, StatisticsType type, int scope,
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

// customers -------------------------------------------------------------------

  public long startSale() {
    return NanoTimer.getTime();
  }
  public void endSale(long start, boolean committed) {
    long elapsed = NanoTimer.getTime() - start;
    if (committed) {
      statistics().incInt(SALES, 1);
      statistics().incLong(SALE_TIME, elapsed);
    } else {
      statistics().incInt(SALE_ROLLBACKS, 1);
      statistics().incLong(SALE_ROLLBACK_TIME, elapsed);
    }
  }
}
