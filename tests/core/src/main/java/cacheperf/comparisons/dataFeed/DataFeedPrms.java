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

package cacheperf.comparisons.dataFeed;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */

public class DataFeedPrms extends BasePrms {

  /**
   * (int)
   * Number of operations per second to throttle to.  Defaults to 0, which is
   * no throttling.
   */
  public static Long throttledOpsPerSec;
  public static int getThrottledOpsPerSec() {
    Long key = throttledOpsPerSec;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (boolean)
   * Whether to stash entry keys for the duration of the test or generate a new
   * one for each operation.  Defaults to true.
   */
  public static Long useFixedKeys;
  public static boolean useFixedKeys() {
    Long key = useFixedKeys;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether to use a fixed entry value for the duration of the test or generate
   * a new one for each operation.  Defaults to true.
   */
  public static Long useFixedVal;
  public static boolean useFixedVal() {
    Long key = useFixedVal;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(DataFeedPrms.class);
  }
  public static void main(String args[]) {
      dumpKeys();
  }
}
