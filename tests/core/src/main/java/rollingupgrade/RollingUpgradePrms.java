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
package rollingupgrade;

import hydra.BasePrms;

public class RollingUpgradePrms extends BasePrms {

  /** (int) The number of entries to load for each thread running the task.
   */
  public static Long numToLoad;  
  public static int getNumToLoad() {
    Long key = numToLoad;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }
   
  public static Long useCacheXml;
  public static boolean getUseCacheXml() {
    Long key = useCacheXml;
    boolean value = tasktab().booleanAt(key, false);
    return value;
  }

  public static Long excludeLocatorThreadsFromTotalThreads;
  public static boolean getExcludeLocatorThreadsFromTotalThreads() {
    Long key = excludeLocatorThreadsFromTotalThreads;
    boolean value = tasktab().booleanAt(key, false);
    return value;
  }
  
  /** (long) The length of time for performOps task in seconds. This
   *  can be used to specify granularity of a task. */
  public static Long opsTaskGranularitySec;  
  // ================================================================================
  static {
    BasePrms.setValues(RollingUpgradePrms.class);
  }
}
