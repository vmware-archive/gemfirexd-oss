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

package dlock;

//import cacheperf.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
//import java.util.*;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class DLSPerfStats extends PerformanceStatistics {

  /** <code>DLSPerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  // dlock.locks statistics
  protected static final String LOCKS          = "locks";
  protected static final String LOCK_TIME     = "lockTime";

  // dlock.unlock statistics
  protected static final String UNLOCKS         = "unlocks";
  protected static final String UNLOCK_TIME     = "unlockTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>DLSPerfStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntCounter
        ( 
         LOCKS,
         "Number of locks completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         LOCK_TIME,
         "Total time spent acquiring locks.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         UNLOCKS,
         "Number of unlocks completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         UNLOCK_TIME,
         "Total time spent releasing locks.",
         "nanoseconds",
	 !largerIsBetter
         )
    };
  }

  public static DLSPerfStats getInstance() {
    return (DLSPerfStats) getInstance( DLSPerfStats.class, SCOPE );
  }
  public static DLSPerfStats getInstance(String name) {
    return (DLSPerfStats) getInstance(DLSPerfStats.class, SCOPE, name);
  }
  public static DLSPerfStats getInstance( String name, String trimspecName ) {
    return (DLSPerfStats) getInstance( DLSPerfStats.class, SCOPE, name, trimspecName );
  }

  /////////////////// Construction / initialization ////////////////

  public DLSPerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getLocks() {
     return statistics().getInt(LOCKS);
   }
   public long getLockTime() {
     return statistics().getLong(LOCK_TIME);
   }
   public int getUnlocks() {
     return statistics().getInt(UNLOCKS);
   }
   public long getUnlockTime() {
     return statistics().getLong(UNLOCK_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the count on the locks
   */
  public void incLocks() {
    incLocks(1);
  }
  /**
   * increase the count on the locks by the supplied amount
   */
  public void incLocks(int amount) {
    statistics().incInt(LOCKS, amount);
  }
  /**
   * increase the time on the locks by the supplied amount
   */
  public void incLockTime(long amount) {
    statistics().incLong(LOCK_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the locks
   */
  public long startLock() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the locks started 
   */
  public void endLock(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(LOCKS, 1);
    long elapsed = ts-start;
    statistics().incLong(LOCK_TIME, elapsed);
  }

  /**
   * increase the count on the unlocks
   */
  public void incUnlocks() {
    incUnlocks(1);
  }
  /**
   * increase the count on the unlocks by the supplied amount
   */
  public void incUnlocks(int amount) {
    statistics().incInt(UNLOCKS, amount);
  }
  /**
   * increase the time on the unlocks by the supplied amount
   */
  public void incUnlockTime(long amount) {
    statistics().incLong(UNLOCK_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the unlocks
   */
  public long startUnlock() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the unlocks started 
   */
  public void endUnlock(long start) {
    long ts = NanoTimer.getTime();
    statistics().incInt(UNLOCKS, 1);
    long elapsed = ts-start;
    statistics().incLong(UNLOCK_TIME, elapsed);
  }
}
