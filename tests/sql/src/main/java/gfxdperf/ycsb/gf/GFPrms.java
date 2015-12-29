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
package gfxdperf.ycsb.gf;

import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * A class used to store keys for test configuration settings.
 */
public class GFPrms extends BasePrms {
  
  static {
    setValues(GFPrms.class);
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of buckets in the region. Applies only to partitioned regions.
   */
  public static Long bucketCount;

  public static int getBucketCount() {
    Long key = bucketCount;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val < 0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s); 
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * Whether to throw an exception and fail the test if the load for any
   * region is imbalanced. This includes buckets, primary buckets, and data.
   * Defaults to true. Only used by tasks that check balance.
   */
  public static Long failOnLoadImbalance;

  public static boolean getFailOnLoadImbalance() {
    Long key = failOnLoadImbalance;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
 
//------------------------------------------------------------------------------

  /**
   * Whether to log individual operations. Defaults to false.
   */
  public static Long logOps;

  public static boolean logOps() {
    Long key = logOps;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (int)
   * Partitioned region redundancy. Applies only to partitioned regions.
   * Defaults to 1.
   */
  public static Long partitionRedundancy;

  public static int getPartitionRedundancy() {
    Long key = partitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of threads doing the workload. Used to pass this information to
   * server threads for monitoring the workload.
   */
  public static Long threadCount;

  public static int getThreadCount() {
    Long key = threadCount;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val == -1) {
      String s = BasePrms.nameForKey(threadCount) + " is missing";
      throw new HydraConfigException(s);
    }
    return val;
  }
}
