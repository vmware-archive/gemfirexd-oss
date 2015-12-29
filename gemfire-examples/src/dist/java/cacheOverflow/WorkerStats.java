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
package cacheOverflow;

import com.gemstone.gemfire.*;

/**
 * {@linkplain Statistics Statistics} about the work performed by a
 * {@link Worker}.
 *
 * @author GemStone Systems, Inc.
 *
 * @since 3.2
 */
public class WorkerStats {

  /** The statistic type of the <code>WorkerStats</code> */
  private static StatisticsType type;

  /** The offset of the "bytesAdded" statistic */
  private static int bytesAddedOffset;

  ///////////////////////  Instance Fields  //////////////////////

  /** The actual object that keeps track of stats */
  private final Statistics stats;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Initializes the statistics type using the given
   * <code>StatisticsTypeFactory</code>.  Note that this method is
   * synchronized because we only want the statistics type created
   * once. 
   */
  private static synchronized void
    initializeStatsType(StatisticsTypeFactory factory) {
    
    if (type != null) {
      return;
    }

    String typeDesc = "Statistics about a Worker thread";

    String bytesAddedDesc =
      "The number of bytes added to the overflow region";

    type = factory.createType(WorkerStats.class.getName(), typeDesc,
      new StatisticDescriptor[] {
        factory.createLongCounter("bytesAdded", bytesAddedDesc,
                                  "bytes", true /* largerBetter */)
      });

    bytesAddedOffset = type.nameToDescriptor("bytesAdded").getId();
  }

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>WorkerStats</code> for the worker with the
   * given id and registers it with the given
   * <code>StatisticsFactory</code>.
   */
  WorkerStats(int workerId, StatisticsFactory factory) {
    initializeStatsType(factory);

    this.stats = factory.createStatistics(type, "Worker " + workerId);
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Increments the total number of bytes added to the region by a
   * given amount.
   */
  void incBytesAdded(long bytesAdded) {
    this.stats.incLong(bytesAddedOffset, bytesAdded);
  }

}
