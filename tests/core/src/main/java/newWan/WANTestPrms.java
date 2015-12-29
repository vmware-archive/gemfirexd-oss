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

package newWan;

import hydra.*;

/**
 * A class used to store keys for wan configuration configuration.
 */

public class WANTestPrms extends BasePrms {
  /**
   * (String) A edge cache configuration name from {@link hydra.CachePrms#names}.
   */
  public static Long clientCacheConfig;
  public static String getClientCacheConfig(){
    Long key = clientCacheConfig;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    return val;
  }
  
  /**
   * (String) Sender queue region listener. Default to null.
   */
  public static Long senderQueueRegionListener;
  public static String getSenderQueueRegionListener(){
    Long key = senderQueueRegionListener;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    return val;
  }
  
  /** Default value for parameter {@link #sleepSec}. */
  public static final int DEFAULT_SLEEP_SEC = 0;
  
  /**
   *  (int)
   *  The number of seconds to sleep between cache operations.  Defaults to
   *  {@link #DEFAULT_SLEEP_SEC}.
   */
  public static Long sleepSec;
  public static int getSleepSec() {
    Long key = sleepSec;
    int val = tasktab().intAt(key, tab().intAt(key, DEFAULT_SLEEP_SEC));
    if (val >= 0) {
      return val;
    } else {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (String)
   * Type of object to use as a value.  Defaults to null.
   */
  public static Long objectType;
  public static String getObjectType() {
    Long key = objectType;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (int)
   * Max Number of operations allowed in test. Defaults to 1000
   * It should be multiple of {@link WANTestPrms#iterations} 
   */
  public static Long maxOperations;
  public static long getMaxOperations() {
    Long key = maxOperations;
    return tasktab().longAt(key, tab().longAt(key, 1000));
  }
  
  /**
   * (int)
   * Number of entries to create in loops that use this.
   * Defaults to 1.
   */
  public static Long batchSize;
  public static int getBatchSize() {
    Long key = batchSize;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }
  
  /**
   * (int)
   * Number of iterations to carry out on each key.  Defaults to 100.
   */
  public static Long iterations;
  public static int getIterations() {
    Long key = iterations;
    int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
    return val;
  }
  
  /**
   * (int)
   * Seconds to wait for queues to drain. 
   */
  public static Long secToWaitForQueue;
  public static int getSecToWaitForQueue() {
    Long key = secToWaitForQueue;
    return tasktab().intAt(key, tab().intAt(key, 7200));
  }
  
  /**
   * (String)
   * ClientName of member of wan site which needs to be recycle. 
   */
  public static Long memberClientName;
  public static String getMemberClientName() {
    Long key = memberClientName;
    return tasktab().stringAt( key, tab().stringAt( key, null ) );
  }
  
  /**
   * (boolean)
   * is new wan configuration. Default to false. 
   */
  public static Long isNewWanConfig;
  public static boolean isNewWanConfig() {
    Long key = isNewWanConfig;
    return tasktab().booleanAt( key, tab().booleanAt( key, false));
  }

  /**
   * (boolean)
   * Validate only parent region. Default to false. 
   */
  public static Long onlyParentRegion;
  public static boolean onlyParentRegion() {
    Long key = onlyParentRegion;
    return tasktab().booleanAt( key, tab().booleanAt( key, false));
  }
  
  static {
    setValues(WANTestPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

