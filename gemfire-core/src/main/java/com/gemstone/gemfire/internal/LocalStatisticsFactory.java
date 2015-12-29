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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A standalone implementation of {@link StatisticsFactory}.
 * It can be used in contexts that do not have the GemFire product
 * or in vm's that do not have a distributed system nor a gemfire connection.
 *
 * @author Darrel Schneider
 * @author Kirk Lund
 */
public class LocalStatisticsFactory extends AbstractStatisticsFactory
    implements StatisticsFactory, StatisticsManager {
  
  public static final String STATS_DISABLE_NAME_PROPERTY = "stats.disable";

  private final SimpleStatSampler sampler;
  private final boolean statsDisabled;

  public LocalStatisticsFactory(CancelCriterion stopper) {
    super(initId(), initName(), initStartTime(), initLogWriter());
    
    this.statsDisabled = Boolean.getBoolean(STATS_DISABLE_NAME_PROPERTY);
    if (statsDisabled) {
      this.sampler = null;
      getLogWriterI18n().config(LocalizedStrings.LocalStatisticsFactory_STATISTIC_COLLECTION_IS_DISABLED_USE_DSTATSDISABLEFALSE_TO_TURN_ON_STATISTICS);
    } else {
      this.sampler = new SimpleStatSampler(stopper, this);
      this.sampler.start();
    }
  }
  
  protected static long initId() {
    return Thread.currentThread().hashCode();
  }
  
  protected static String initName() {
    return System.getProperty("stats.name", Thread.currentThread().getName());
  }
  
  protected static long initStartTime() {
    return System.currentTimeMillis();
  }
  
  protected static LogWriterI18n initLogWriter() {
    int logLevel = LogWriterImpl.levelNameToCode(
        System.getProperty("stats.log-level", "config"));
    return new PureLogWriter(logLevel);
  }

  protected SimpleStatSampler getStatSampler() {
    return this.sampler;
  }

  @Override
  public void close() {
    if (this.sampler != null) {
      this.sampler.stop();
    }
  }
  
  @Override
  protected Statistics createOsStatistics(StatisticsType type, String textId, long numericId, int osStatFlags) {
    if (this.statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }
    return super.createOsStatistics(type, textId, numericId, osStatFlags);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    if (this.statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }
    return super.createAtomicStatistics(type, textId, numericId);
  }
}
