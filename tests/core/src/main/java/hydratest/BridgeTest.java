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
package hydratest;

import hydra.*;
import com.gemstone.gemfire.cache.Region;

public class BridgeTest {

  static Region r;

  public static void initBridge() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  public static void initEdge() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    r = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  public static synchronized void initNonSingletonEdge() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    if (r == null) {
      r = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    }
  }

  public static void doGet() {
    r.get("frip");
  }

  public static void recycleEdgeConnection() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      CacheHelper.closeCache();
      DistributedSystemHelper.disconnect();
    } 
    initEdge();
  }

  public static synchronized void recycleNonSingletonEdgeConnection() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      CacheHelper.closeCache();
      DistributedSystemHelper.disconnect();
      r = null;
    }
  }
}
