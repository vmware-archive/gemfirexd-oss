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

package com.pivotal.gemfirexd.internal.engine.db;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

/**
 * @author kneeraj
 * 
 */
public class IndexPersistenceStats {

  public static final String typeName = "IndexPersistenceStats";

  private static final StatisticsType type;

  /** Time taken to load index */
  protected static int indexLoadTimeId;

  /** Time taken to start the node completely */
  protected static int nodeStartTimeId;

  protected static final String INDEX_LOAD_TIME = "IndexLoadTime";

  protected static final String NODE_START_TIME = "NodeStartTime";

  /**
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(
        typeName,
        "Stats for loading from Persisted index file",
        new StatisticDescriptor[] {
            f.createLongGauge(INDEX_LOAD_TIME,
                "Time taken to load index from persisted files.",
                "milliseconds", false),
            f.createLongGauge(NODE_START_TIME, "Time taken to start a node.",
                "milliseconds", false) });

    // Initialize id fields
    indexLoadTimeId = type.nameToId(INDEX_LOAD_TIME);
    nodeStartTimeId = type.nameToId(NODE_START_TIME);

  }

  private Statistics stats;

  public void init(StatisticsFactory f) {
    this.stats = f.createAtomicStatistics(type, typeName);    
  }

  public void close() {
    this.stats.close();
  }

  public void endIndexLoad(long start) {
    long ts = System.currentTimeMillis();

    // Increment event queue time
    long elapsed = ts - start;
    this.stats.incLong(indexLoadTimeId, elapsed);
  }
  
  public void endNodeUp(long start) {
    long ts = System.currentTimeMillis();

    // Increment event queue time
    long elapsed = ts - start;
    this.stats.incLong(nodeStartTimeId, elapsed);
  }
}
