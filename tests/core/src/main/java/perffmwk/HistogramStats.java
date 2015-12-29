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

package perffmwk;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import hydra.Log;
import perffmwk.PerformanceStatistics;

/**
 * Implements a histogram with configurable bin values.  See {@link
 * HistogramStatsPrms#enable} to enable the histogram and {@link
 * HistogramStats#binValues} to configure the bins.
 */
public class HistogramStats extends PerformanceStatistics {

  private static final int SCOPE = THREAD_SCOPE;
  private static final boolean logHighLatencyWarnings =
                               HistogramStatsPrms.logHighLatencyWarnings();

  private static final String OPERATIONS = "operations";

  private static long[] binVals;
  private static String[] binNames;

//------------------------------------------------------------------------------
// static methods

  /**
   * Returns the statistic descriptors for <code>HistogramStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    binVals = HistogramStatsPrms.getBinVals();
    binNames = new String[binVals.length + 1];
    StatisticDescriptor binDescriptors[] =
                        new StatisticDescriptor[binVals.length + 2];
    for (int i = 0; i <= binVals.length; i++) {
      if (i == 0) {
        binNames[i] = "opsLessThan" + binVals[i] + "ns";
        binDescriptors[i] = factory().createLongCounter(binNames[i],
          "Number of ops taking less than " + binVals[i] + " ns.",
          "ops", true);
      } else if (i == binVals.length) {
        binNames[i] = "opsMoreThan" + binVals[binVals.length-1] + "ns";
        binDescriptors[i] = factory().createLongCounter(binNames[i],
          "Number of ops taking more than " + binVals[i-1] + " ns.",
          "ops", false);
      } else {
        binNames[i] = "opsLessThan" + binVals[i] + "ns";
        binDescriptors[i] = factory().createLongCounter(binNames[i],
          "Number of ops taking more than " + binVals[i-1] +
          " and less than " + binVals[i] + " ns.",
          "ops", true);
      }
    }
    // include the total operations
    binDescriptors[binVals.length + 1] =
      factory().createLongCounter(OPERATIONS,
        "Total number of ops included in the histogram", "ops", true);
    return binDescriptors;
  }

  /**
   * Creates an instance with thread scope using the thread name as
   * the display name.
   */
  public static HistogramStats getInstance() {
    return (HistogramStats)getInstance(HistogramStats.class, SCOPE);
  }

  /**
   * Creates an instance with the specified scope using the thread name as
   * the display name.
   */
  public static HistogramStats getInstance(int scope) {
    return (HistogramStats)getInstance(HistogramStats.class, scope);
  }

  /**
   * Creates an instance with thread scope using the specified display name.
   */
  public static HistogramStats getInstance(String name) {
    return (HistogramStats)getInstance(HistogramStats.class, SCOPE, name);
  }

  /**
   * Creates an instance with thread scope using the specified display name
   * and associated with the specified trim specification.
   */
  public static HistogramStats getInstance(String name, String trimspecName) {
    return (HistogramStats)getInstance(HistogramStats.class, SCOPE, name,
                                       trimspecName);
  }

//------------------------------------------------------------------------------
// instance methods

  public HistogramStats(Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

  /**
   * For a given time t, increment the appropriate time bin by a count of 1.
   */
  public void incBin(long t) {
    statistics().incLong(OPERATIONS, 1);
    for (int i = 0; i < binVals.length; i++) {
      if (t < binVals[i]) {
        statistics().incLong(binNames[i], 1);
        return;
      }
    }
    if (logHighLatencyWarnings) {
      Log.getLogWriter().warning("HIGH LATENCY: " + t);
    }
  }
}
