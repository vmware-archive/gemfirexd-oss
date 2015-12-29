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
package gfxdperf.tpch.oracle;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;

import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to Oracle.
 */
public class OracleStats extends PerformanceStatistics {

  protected static final String IMPORT_TABLE_ROWS = "importTableRows";
  protected static final String IMPORT_TABLE_TIME = "importTableTime";
  protected static final String CREATE_INDEX = "createIndex";
  protected static final String CREATE_INDEX_TIME = "createIndexTime";

  /**
   * Returns the statistic descriptors for <code>OracleStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
      factory().createIntCounter
      (
        IMPORT_TABLE_ROWS,
        "Number of rows imported in this table.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        IMPORT_TABLE_TIME,
        "Total time spent importing rows into this table.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        CREATE_INDEX,
        "Number of indexes created.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        CREATE_INDEX_TIME,
        "Total time spent creating indexes.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static OracleStats getInstance() {
    return (OracleStats)getInstance(OracleStats.class, THREAD_SCOPE);
  }

//------------------------------------------------------------------------------
// constructors

  public OracleStats(Class cls, StatisticsType type, int scope,
                   String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

//------------------------------------------------------------------------------
// operations

  public long startImportTable() {
    return NanoTimer.getTime();
  }

  public void endImportTable(long start, int rows) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(IMPORT_TABLE_ROWS, rows);
    statistics().incLong(IMPORT_TABLE_TIME, elapsed);
  }

  public long startCreateIndex() {
    return NanoTimer.getTime();
  }

  public void endCreateIndex(long start) {
    long elapsed = NanoTimer.getTime() - start;
    statistics().incInt(CREATE_INDEX, 1);
    statistics().incLong(CREATE_INDEX_TIME, elapsed);
  }
}
