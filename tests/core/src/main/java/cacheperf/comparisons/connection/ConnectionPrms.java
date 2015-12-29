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

package cacheperf.comparisons.connection;

import hydra.BasePrms;

/**
 * A class used to store keys for connection test configuration.
 */
public class ConnectionPrms extends BasePrms {

  /**
   * (boolean)
   * Whether to bounce the VM.  Defaults to false.
   */
  public static Long bounceVm;
  public static boolean bounceVm() {
    Long key = bounceVm;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to create a cache and region.  Defaults to true.
   */
  public static Long createCacheAndRegion;
  public static boolean createCacheAndRegion() {
    Long key = createCacheAndRegion;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether to delete the system files, and directory if {@link #bounceVm}
   * is true.  Defaults to true.
   */
  public static Long deleteSystemFiles;
  public static boolean deleteSystemFiles() {
    Long key = deleteSystemFiles;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether to log times for various operations.  Defaults to false.
   */
  public static Long logTimes;
  public static boolean logTimes() {
    Long key = logTimes;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to use cache XML to create a cache and region when {@link
   * #createCacheAndRegion} is true.  Requires previous execution of
   * {@link ConnectionClient#generateCacheXmlFileTask}.  Defaults to false.
   */
  public static Long useCacheXml;
  public static boolean useCacheXml() {
    Long key = useCacheXml;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to use acquire a connection after creating the region.
   * Defaults to true.
   */
  public static Long acquireConnection;
  public static boolean acquireConnection() {
    Long key = acquireConnection;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(ConnectionPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}
