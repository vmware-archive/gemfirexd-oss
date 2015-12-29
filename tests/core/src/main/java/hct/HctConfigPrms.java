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

package hct;

import hydra.*;

/**
 *  A class used to store keys for test configuration settings.
 */

public class HctConfigPrms extends BasePrms {

  //----------------------------------------------------------------------------
  // cacheConfig 
  //----------------------------------------------------------------------------

  /**
   * An entry in {@link hydra.CachePrms#names} giving the cache configuration.
   */
  public static Long cacheConfig;
  public static String getCacheConfig() {
    Long key = cacheConfig;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = nameForKey(key) + " not found: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  // regionConfig 
  //----------------------------------------------------------------------------

  /**
   * An entry in {@link hydra.RegionPrms#names} giving the region configuration.
   */
  public static Long regionConfig;
  public static String getRegionConfig() {
    Long key = regionConfig;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = nameForKey(key) + " not found: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  // bridgeConfig 
  //----------------------------------------------------------------------------

  /**
   * An entry in {@link hydra.BridgePrms#names} giving the bridge configuration.
   */
  public static Long bridgeConfig;
  public static String getBridgeConfig() {
    Long key = bridgeConfig;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = nameForKey(key) + " not found: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
    setValues(HctConfigPrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}
