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
package examples.dist.cacheOverflow;

import hydra.*;

/**
 * Hydra configuration parameters for the {@link
 * cacheOverflow.CacheOverflow CacheOverflow} example tests.
 *
 * @author David Whitlock
 *
 * @since 3.2.1
 */
public class CacheOverflowPrms extends BasePrms {

  static {
    BasePrms.setValues( CacheOverflowPrms.class );
  }

  /** (boolean) Does the test backup the regions to disk?
   *
   * @see #getBackup */
  public static Long backup;

  /** (boolean) Are writes to disk synchronous or asynchronous?
   *
   * @see #isSynchronous */
  public static Long synchronous;

  /** (int) The number of threads that add to the region
   *
   * @see #getThreads */
  public static Long threads;

  /** (int) The number of arrays that are added by each thread
   *
   * @see #getArrays */
  public static Long arrays;

  /** (int) The overflow threshold for the example's region 
   *
   * @see #getOverflowThreshold */
  public static Long overflowThreshold;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns whether or not backup regions should be used.  By
   * default, backups are not used.
   */
  public static boolean getBackup() {
    return TestConfig.tab().booleanAt(backup, false);
  }

  /**
   * Returns whether or not writes to disks are synchronous.  By
   * default, disk writes are not synchronous.
   */
  public static boolean isSynchronous() {
    return TestConfig.tab().booleanAt(synchronous, false);
  }

  /**
   * Returns the number of threads that add to the region.  By
   * default, 3 threads add to the region.
   */
  public static int getThreads() {
    return TestConfig.tab().intAt(threads, 3);
  }

  /**
   * Returns the number of arrays that are added by each thread.  By
   * default, 10000 arrays are added by each thread.
   */
  public static int getArrays() {
    return TestConfig.tab().intAt(arrays, 10000);
  }

  /**
   * Returns the overflow threshold of the region in megabytes.  By
   * default, the overflow threshold is 50 megabytes.
   */
  public static int getOverflowThreshold() {
    return TestConfig.tab().intAt(overflowThreshold, 50);
  }

}
