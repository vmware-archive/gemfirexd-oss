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

package conflation;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */
public class ConflationPrms extends BasePrms {

  //----------------------------------------------------------------------------
  //  Interest registration
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The total number of keys to register interest in.  No default.
   * Not for use with oneof or range.
   */
  public static Long interestTotalSize;
  public static int getInterestTotalSize() {
    Long key = interestTotalSize;
    int val = tab().intAt(key);
    if (val < 0 ) {
      String s = BasePrms.nameForKey(key) + " must be non-negative";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The number of keys to register interest in at one time.  Default is 1.
   * Not for use with oneof or range.
   */
  public static Long interestBatchSize;
  public static int getInterestBatchSize() {
    Long key = interestBatchSize;
    int val = tab().intAt(key, 1);
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Task content
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The percentage of the time to do destroys before updates.  Default is 0.
   */
  public static Long destroyPercentage;
  public static int getDestroyPercentage() {
    Long key = destroyPercentage;
    int val = tab().intAt(key, 0);
    if (val < 0 || val > 100) {
      String s = BasePrms.nameForKey(key) + " must be between 0 and 100";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Task granularity
  //----------------------------------------------------------------------------

  /**
   * (long)
   * The time in milliseconds to run a task that uses it.  Default is 60000.
   */
  public static Long taskGranularityMs;
  public static long getTaskGranularityMs() {
    Long key = taskGranularityMs;
    long val = tab().longAt(key, 60000);
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Sleep controls
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The time in milliseconds to sleep.  Default is 0.
   */
  public static Long sleepMs;
  public static int getSleepMs() {
    Long key = sleepMs;
    int val = tab().intAt(key, 0);
    if (val < 0 ) {
      String s = BasePrms.nameForKey(key) + " must be non-negative";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The time in milliseconds to sleep in the listener.  Default is 0.
   */
  public static Long listenerSleepMs;
  public static int getListenerSleepMs() {
    Long key = listenerSleepMs;
    int val = tab().intAt(key, 0);
    if (val < 0 ) {
      String s = BasePrms.nameForKey(key) + " must be non-negative";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Monotonic values
  //----------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to feed data with monotonically increasing values for each key.
   * Defaults to false.  Not for use with oneof or range.
   */
  public static Long feedMonotonically;
  public static boolean feedMonotonically() {
    Long key = feedMonotonically;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to validate that updated values are monotonically increasing.
   * Defaults to false.  Not for use with oneof or range.
   */
  public static Long validateMonotonic;
  public static boolean validateMonotonic() {
    Long key = validateMonotonic;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to validate that updated values increase in increments of 1.
   * Defaults to false.  Not for use with oneof or range.
   */
  public static Long validateStrict;
  public static boolean validateStrict() {
    Long key = validateStrict;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  //----------------------------------------------------------------------------
  //  Conflation validation
  //----------------------------------------------------------------------------

  /**
   * (String)
   * Type of stat used in {@link ConflationClient#validateConflationStats}.
   */
  public static Long conflationStatType;
  public static String getConflationStatType() {
    Long key = conflationStatType;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      throw new HydraConfigException(nameForKey(key) + " is not specified");
    }
    return val;
  }

  /**
   * (String)
   * Stat to check in {@link ConflationClient#validateConflationStat}.
   */
  public static Long conflationStat;
  public static String getConflationStat() {
    Long key = conflationStat;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      throw new HydraConfigException(nameForKey(key) + " is not specified");
    }
    return val;
  }

  /**
   * (boolean)
   * Whether the test expects to see conflation in {@link
   * ConflationClient#validateConflationStat}.
   */
  public static Long expectNonZeroConflationStat;
  public static boolean expectNonZeroConflationStat() {
    Long key = expectNonZeroConflationStat;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (int)
   * The time to wait for a <b>change</b> in the number of events accounted for
   * so far before timing out.  Default is 60 seconds.  Use {@link
   * hydra.Prms@maxResultWaitSec} to control the time to wait for <b>all</b>
   * events to be accounted for.
   */
  public static Long maxEventWaitSec;
  public static int getMaxEventWaitSec() {
    Long key = maxEventWaitSec;
    int val = tab().intAt(key, 60);
    if (val < 0 ) {
      String s = BasePrms.nameForKey(key) + " must be non-negative";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(ConflationPrms.class);
  }
  public static void main(String args[]) {
      dumpKeys();
  }
}
