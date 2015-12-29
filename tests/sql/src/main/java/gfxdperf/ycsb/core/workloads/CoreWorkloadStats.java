/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
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
package gfxdperf.ycsb.core.workloads;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;

import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to YCSB.
 */
public class CoreWorkloadStats extends PerformanceStatistics {

  protected static final String READS       = "reads";
  protected static final String READ_TIME   = "readTime";
  protected static final String UPDATES     = "updates";
  protected static final String UPDATE_TIME = "updateTime";
  protected static final String INSERTS     = "inserts";
  protected static final String INSERT_TIME = "insertTime";
  protected static final String DELETES     = "deletes";
  protected static final String DELETE_TIME = "deleteTime";

  /**
   * Returns the statistic descriptors for <code>CoreWorkloadStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
      factory().createIntCounter
      (
        READS,
        "Number of reads done.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        READ_TIME,
        "Total time spent doing reads.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        UPDATES,
        "Number of updatges done.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        UPDATE_TIME,
        "Total time spent doing updatges.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        INSERTS,
        "Number of inserts done.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        INSERT_TIME,
        "Total time spent doing inserts.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        DELETES,
        "Number of deletes done.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        DELETE_TIME,
        "Total time spent doing deletes.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static CoreWorkloadStats getInstance() {
    return (CoreWorkloadStats)getInstance(CoreWorkloadStats.class, THREAD_SCOPE);
  }

//------------------------------------------------------------------------------
// constructors

  public CoreWorkloadStats(Class cls, StatisticsType type, int scope,
                   String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

//------------------------------------------------------------------------------
// reads

  public long startRead() {
    return NanoTimer.getTime();
  }

  public long endRead(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(READS, amount);
    statistics().incLong(READ_TIME, elapsed);
    return elapsed;
  }

//------------------------------------------------------------------------------
// updates

  public long startUpdate() {
    return NanoTimer.getTime();
  }

  public long endUpdate(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(UPDATES, amount);
    statistics().incLong(UPDATE_TIME, elapsed);
    return elapsed;
  }

//------------------------------------------------------------------------------
// inserts

  public long startInsert() {
    return NanoTimer.getTime();
  }

  public long endInsert(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(INSERTS, amount);
    statistics().incLong(INSERT_TIME, elapsed);
    return elapsed;
  }

//------------------------------------------------------------------------------
// deletes

  public long startDelete() {
    return NanoTimer.getTime();
  }

  public long endDelete(long start, int amount) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(DELETES, amount);
    statistics().incLong(DELETE_TIME, elapsed);
    return elapsed;
  }
}
