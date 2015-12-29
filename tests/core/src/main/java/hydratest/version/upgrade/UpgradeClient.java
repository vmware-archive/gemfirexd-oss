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

package hydratest.version.upgrade;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.GemFireVersion;
import hydra.*;

public class UpgradeClient {

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void reportCacheTask() {
    Cache c = CacheHelper.getCache();
    String s = "UPGRADE: GemFire Version " + GemFireVersion.getGemFireVersion();
    Log.getLogWriter().info(s);
  }

  public static void bounceTask() throws ClientVmNotFoundException {
    MasterController.sleepForMs(UpgradePrms.getSleepSec()*1000);
    ClientVmMgr.stopAsync("Killing self at version: " + GemFireVersion.getGemFireVersion(), ClientVmMgr.NICE_KILL, ClientVmMgr.IMMEDIATE);
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
