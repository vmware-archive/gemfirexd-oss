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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;
//import java.io.*;
import java.util.*;

/**
 * Defines the contract that a statistics factory must implement
 * for its Statistics instances. This is an internal implementation
 * specific interface.
 *
 * @author Darrel Schneider
 *
 */
public interface StatisticsManager extends StatisticsFactory {
  /**
   * Called when the Statistics instance <code>s</code> is closed.
   */
  public void destroyStatistics(Statistics s);
  /**
   * Returns a name that can be used to identify the manager
   */
  public String getName();
  /**
   * Returns a numeric id that can be used to identify the manager
   */
  public long getId();
  /**
   * Returns the start time of this manager.
   */
  public long getStartTime();
  /**
   * Returns the manager's log writer.
   */
  public LogWriterI18n getLogWriterI18n();
  /**
   * Returns a value that changes any time a Statistics instance is added
   * or removed from this manager.
   */
  public int getStatListModCount();
  /**
   * Returns a list of all the Statistics this manager is currently managing.
   */
  public List<Statistics> getStatsList();
  
  /**
   * Returns the current number of statistics instances.
   */
  public int getStatisticsCount();

  /**
   * Returns the statistics resource instance given its id.
   */
  public Statistics findStatistics(long id);

  /**
   * Returns true if the specified statistic resource still exists.
   */
  public boolean statisticsExists(long id);

  /**
   * Returns an array of all the current statistic resource instances.
   */
  public Statistics[] getStatistics();
}
