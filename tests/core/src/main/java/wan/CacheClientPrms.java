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

package wan;

import com.gemstone.gemfire.cache.InterestResultPolicy;
import hydra.*;

/**
 * A class used to store keys for client cache configuration.
 */

public class CacheClientPrms extends BasePrms {

  /**
   * (String) A cache configuration name from {@link hydra.CachePrms#names}.
   */
  public static Long cacheConfig;

  /**
   * (String) A region configuration name from {@link hydra.RegionPrms#names}.
   */
  public static Long regionConfig;


  /**
   * (String)
   * Type of InterestResultPolicy to use in registerInterest call.
   * Valid values are keys, keysValues, keys and none.  Defaults to keysValues.
   */
  public static Long interestPolicy;
  public static InterestResultPolicy getInterestPolicy() {
    Long key = interestPolicy;
    String val = tasktab().stringAt( key, tab().stringAt( key, "keysValues" ) );

    if (val.equalsIgnoreCase("keys")) {
      return (InterestResultPolicy.KEYS);
    } else if (val.equalsIgnoreCase("keysValues")) {
      return (InterestResultPolicy.KEYS_VALUES);
    } else if (val.equalsIgnoreCase("none")) {
      return (InterestResultPolicy.NONE);
    } else {
      throw new HydraConfigException("Invalid InterestResultPolicy " + interestPolicy);
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
   * Number of iterations to carry out on each key.  Defaults to 1000.
   */
  public static Long iterations;
  public static int getIterations() {
    Long key = iterations;
    int val = tasktab().intAt( key, tab().intAt( key, 1000 ) );
    return val;
  }

  /**
   * (int)
   * Number of entries to create when populating the edge clients cache.
   * Defaults to 100.
   */
  public static Long numEntries;
  public static int getNumEntries() {
    Long key = numEntries;
    int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
    return val;
  }

  /**
   * (int)
   * Number number of entries for putAll.  Defaults to 1.
   * This is also used by putSequentialKeyUsingPutAll as the maximum mapSize for putAll
   */
  public static Long numPutAllEntries;
  public static int getNumPutAllEntries() {
    Long key = numPutAllEntries;
    int val = tasktab().intAt(key, tab().intAt(key, 1));
    return val;
  }
  /**
   * (Vector)
   * Clients write regionSizes out to the BB SharedMap (HydraCloseTask_regionSizeToBB) for ENDTASK validation.
   * 
   * HydraEndTask_regionSizesEqual uses this to find the BB entries of regionSizes
   * in the BB sharedMap.
   *
   */
  public static Long clientsToCompare;

  /** Default value for parameter {@link #sleepSec}. */
  public static final int DEFAULT_SLEEP_SEC = 0;

  /**
   *  (int)
   *  The number of seconds to sleep between cache operations.  Defaults to
   *  {@link #DEFAULT_SLEEP_SEC}.
   */
  public static Long sleepSec;

  /**
   * Returns the value of the {@link #sleepSec} parameter as an int.
   */
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
   * Returns the name of the bridgeServer to target for a kill
   * From {@link hydra.ClientPrms#names}.
   *
   * @see WANClient.recycleServer
   */
  public static Long bridgeKillTarget;
  public static String getBridgeKillTarget() {
    Long key = bridgeKillTarget;
    String val = tasktab().stringAt(key, tab().stringAt( key, null ) );
    return val;
  }

  public static Long secToWaitForQueue;
  public static int getSecToWaitForQueue() {
    return tab().intAt(secToWaitForQueue);
  }

  static {
    setValues(CacheClientPrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}
