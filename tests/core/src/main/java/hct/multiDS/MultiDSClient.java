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

package hct.multiDS;

import com.gemstone.gemfire.cache.*;
import hydra.*;
import java.util.*;
import util.TestException;

/**
 * Illustrates whether an edge client can work with regions connected to
 * servers in different distributed systems.
 */
public class MultiDSClient {

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * Opens the cache defined by {@link hydra.ConfigPrms#cacheConfig}.  Starts
   * a bridge server defined by {@link hydra.ConfigPrms#bridgeConfig}, if given.
   */
  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  /**
   * Creates the region named {@link hct.multiDS.multiDSPrms#regionName}
   * and defined by {@link hydra.ConfigPrms#regionConfig}.
   */
  public static void createRegionTask() {
    String regionConfig = ConfigPrms.getRegionConfig();
    String regionName = MultiDSPrms.getRegionName();
    RegionHelper.createRegion(regionName, regionConfig);
  }

  /**
   * Closes the cache.
   */
  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }

  /**
   * Puts data into the region named {@link hct.multiDS.multiDSPrms#regionName}.
   * Puts {@link hct.multiDS.MultiDSPrms#numKeys} entries, using as the key the
   * region name plus the entry number, with value the integer entry number.
   */
  public static void putDataTask() {
    String regionName = MultiDSPrms.getRegionName();
    int numKeys = MultiDSPrms.getNumKeys();
    Region region = RegionHelper.getRegion(regionName);
    Log.getLogWriter().info("Putting " + numKeys + " keys into region "
        + regionName + ":\n"
        + RegionHelper.regionAttributesToString(region.getAttributes()));
    for (int i = 0; i < numKeys; i++) {
      String key = regionName + i;
      region.put(key, new Integer(i));
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("Put " + key + "=" + i);
      }
    }
  }

  /**
   * Verifies that all {@link hct.multiDS.MultiDSPrms#numKeys} entries in the
   * region named {@link hct.multiDS.MultiDSPrms#regionName} are present, and
   * no more.
   */
  public static void validateDataTask() {
    String regionName = MultiDSPrms.getRegionName();
    int numKeys = MultiDSPrms.getNumKeys();
    Region region = RegionHelper.getRegion(regionName);
    Log.getLogWriter().info("Validating " + numKeys + " entries in region "
        + regionName + ":\n"
        + RegionHelper.regionAttributesToString(region.getAttributes()));
    Set keys = region.keys();
    if (keys.size() != numKeys) {
      String s = "Expected " + numKeys + " but found " + keys.size();
      throw new TestException(s);
    }
    for (int i = 0; i < numKeys; i++) {
      String key = regionName + i;
      Integer val = (Integer)region.get(key);
      if (val == null) {
        String s = "Missing entry at key=" + key;
        throw new TestException(s);
      } else if (val.intValue() != i) {
        String s = "Wrong entry at key=" + key + " value=" + val;
        throw new TestException(s);
      }
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("Validated " + key + "=" + i);
      }
    }
  }
}
