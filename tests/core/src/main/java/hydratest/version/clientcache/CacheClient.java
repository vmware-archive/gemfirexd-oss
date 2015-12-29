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

package hydratest.version.clientcache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.internal.GemFireVersion;
import hydra.*;

public class CacheClient {

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void openClientCacheTask() {
    ClientCacheHelper.createCache(ConfigPrms.getClientCacheConfig());
  }

  public static void openClientCacheFromXmlTask() {
    String fn = "cache_" + RemoteTestModule.getMyVmid() + ".xml";
    ClientCacheHelper.generateCacheXmlFile(ConfigPrms.getClientCacheConfig(),
                                           ConfigPrms.getClientRegionConfig(),
                                           fn);
    ClientCacheHelper.createCacheFromXml(fn);
  }

  public static void createRegionTask() {
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  public static void createClientRegionTask() {
    ClientRegionHelper.createRegion(ConfigPrms.getClientRegionConfig());
  }

  public static void reportRegionTask() {
    String regionConfig = ConfigPrms.getRegionConfig();
    RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
    Region r = RegionHelper.getRegion(rd.getRegionName());
    RegionAttributes ra = r.getAttributes();
    Log.getLogWriter().info("Region " + rd.getName() + "\n"
                           + RegionHelper.regionAttributesToString(ra));
  }

  public static void reportClientRegionTask() {
    String regionConfig = ConfigPrms.getClientRegionConfig();
    ClientRegionDescription crd =
          ClientRegionHelper.getClientRegionDescription(regionConfig);
    Region r = ClientRegionHelper.getRegion(crd.getRegionName());
    RegionAttributes ra = r.getAttributes();
    Log.getLogWriter().info("Client region " + crd.getName() + "\n"
                           + ClientRegionHelper.regionAttributesToString(ra));
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }

  public static void closeClientCacheTask() {
    ClientCacheHelper.closeCache();
  }
}
