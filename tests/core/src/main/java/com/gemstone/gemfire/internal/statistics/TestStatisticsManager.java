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

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.AbstractStatisticsFactory;
import com.gemstone.gemfire.internal.OsStatisticsFactory;
import com.gemstone.gemfire.internal.StatisticsManager;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestStatisticsManager extends AbstractStatisticsFactory 
    implements StatisticsManager, OsStatisticsFactory {

  public TestStatisticsManager(long id, String name, long startTime, LogWriterI18n log) {
    super(id, name, startTime, log);
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId,
      long numericId, int osStatFlags) {
    // TODO ?
    return null;
  }
}
