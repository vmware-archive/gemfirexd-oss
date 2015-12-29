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

package hydra.timeserver;

import perffmwk.PerformanceStatistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;

/**
 * Statistics for recording clock skew and drift.
 */

public class ClockSkewStats extends PerformanceStatistics {

  private static final int SCOPE = VM_SCOPE;

  protected static final String CLOCKSKEW = "clockSkew";
  protected static final String LATENCY = "latency";

  //// STATIC METHODS

  public static StatisticDescriptor[] getStatisticDescriptors() {
    return new StatisticDescriptor[]
           {              
             factory().createLongGauge
             (
               CLOCKSKEW,
               "Clock skew relative to hydra master",
               "nanoseconds"
             ),
             factory().createLongGauge
             (
               LATENCY,
               "Latency in the clock skew requests to the hydra master (a measure of the error in the clock skew)",
               "nanoseconds"
             ),
           };
  }
  public static ClockSkewStats getInstance() {
    return (ClockSkewStats) getInstance(ClockSkewStats.class, SCOPE);
  }
  public static ClockSkewStats getInstance(String name) {
    return (ClockSkewStats) getInstance(ClockSkewStats.class, SCOPE, name);
  }
  public static ClockSkewStats getInstance(String name, String trimspecName) {
    return (ClockSkewStats) getInstance(ClockSkewStats.class, SCOPE, name, trimspecName);
  }

  //// CONSTRUCTORS

  public ClockSkewStats(Class cls, StatisticsType type, int scope, String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

  //// INSTANCE METHODS

  public void setClockSkew(long amount) {
    statistics().setLong(CLOCKSKEW, amount);
  }
  
  public void setLatency(long amount) {
    statistics().setLong(LATENCY, amount);
  }
}
