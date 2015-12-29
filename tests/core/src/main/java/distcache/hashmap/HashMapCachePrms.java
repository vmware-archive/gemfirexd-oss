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

package distcache.hashmap;

import hydra.BasePrms;

/**
 *  A class used to store keys for test configuration settings.
 */
public class HashMapCachePrms extends BasePrms {

  //----------------------------------------------------------------------------
  // hashmap configuration 
  //----------------------------------------------------------------------------

  /**
   * Initial capacity.  Defaults to 16.
   */
  public static Long initialCapacity;
  public static int getInitialCapacity() {
    Long key = initialCapacity;
    return tasktab().intAt(key, tab().intAt(key, 16));
  }

  /**
   * Load factor.  Defaults to 0.75.
   */
  public static Long loadFactor;
  public static double getLoadFactor() {
    Long key = loadFactor;
    return tasktab().doubleAt(key, tab().doubleAt(key, 0.75));
  }

  /**
   * Concurrency level.  Defaults to 16.
   */
  public static Long concurrencyLevel;
  public static int getConcurrencyLevel() {
    Long key = concurrencyLevel;
    return tasktab().intAt(key, tab().intAt(key, 16));
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
    setValues(HashMapCachePrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}
