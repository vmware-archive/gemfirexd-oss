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
package com.gemstone.gemfire.management.internal.beans.stats;

import com.gemstone.gemfire.management.internal.beans.MetricsCalculator;

/**
 * 
 * @author rishim
 *
 */
public class StatsAverageLatency {

  private String numberKey;
  private String timeKey;
  private StatType numKeyType;

  private MBeanStatsMonitor monitor;

  public StatsAverageLatency(String numberKey, StatType numKeyType,
      String timeKey, MBeanStatsMonitor monitor) {
    this.numberKey = numberKey;
    this.numKeyType = numKeyType;
    this.timeKey = timeKey;
    this.monitor = monitor;
  }

  public long getAverageLatency() {
    if (numKeyType.equals(StatType.INT_TYPE)) {
      int numberCounter = monitor.getStatistic(numberKey).intValue();
      long timeCounter = monitor.getStatistic(timeKey).longValue();
      return MetricsCalculator.getAverageLatency(numberCounter, timeCounter);
    } else {
      long numberCounter = monitor.getStatistic(numberKey).longValue();
      long timeCounter = monitor.getStatistic(timeKey).longValue();
      return MetricsCalculator.getAverageLatency(numberCounter, timeCounter);
    }

  }

}
