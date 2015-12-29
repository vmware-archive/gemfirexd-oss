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

package hydratest.version.replication;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.GemFireVersion;
import hydra.*;

public class ReplicationClient {

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void createRegionTask() {
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  public static void reportCacheTask() {
    Cache c = CacheHelper.getCache();
    String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
             + ", the Cache is " + c
             + ", the resource manager is "
             + VersionHelper.getResourceManager(c)
             + ", the gateway conflict resolver is "
             + VersionHelper.getGatewayConflictResolver(c);
    Log.getLogWriter().info(s);
  }

  public static void reportDiskAttributesTask() {
    Region r = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    String s = "In GemFire version " + GemFireVersion.getGemFireVersion() + ", "
             + VersionHelper.getDiskAttributes(r);
    Log.getLogWriter().info(s);
  }

  public static void reportEvictionAttributesTask() {
    Region r = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    RegionAttributes ra = r.getAttributes();
    EvictionAttributes ea = ra.getEvictionAttributes();
    if (ea == null) {
      throw new HydraConfigException("Eviction is not configured");
    }
    else {
      String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
               + ", the EvictionAttributes are" + ea
               + ", the maximum is "
               + VersionHelper.getMaximum(ea)
               + ", the interval is "
               + VersionHelper.getInterval(ea)
               + ", the object sizer is "
               + VersionHelper.getObjectSizer(ea)
               + ", the eviction action is "
               + ea.getAction();
      Log.getLogWriter().info(s);
    }
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
