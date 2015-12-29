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
package com.gemstone.gemfire.internal.statistics;

import java.util.List;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.StatisticsManager;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestStatisticsSampler implements StatisticsSampler {
  
  private final StatisticsManager manager;
  
  public TestStatisticsSampler(StatisticsManager manager) {
    this.manager = manager;
  }
  
  @Override
  public int getStatisticsModCount() {
    return this.manager.getStatListModCount();
  }
  
  @Override
  public Statistics[] getStatistics() {
    @SuppressWarnings("unchecked")
    List<Statistics> statsList = (List<Statistics>)this.manager.getStatsList();
    synchronized (statsList) {
      return (Statistics[])statsList.toArray(new Statistics[statsList.size()]);
    }
  }

  @Override
  public LogWriterI18n getLogger() {
    return this.manager.getLogWriterI18n();
  }
  
  @Override
  public boolean waitForSample(long timeout) throws InterruptedException {
    return false;
  }
  
  @Override
  public SampleCollector waitForSampleCollector(long timeout) throws InterruptedException {
    return null;
  }
}
