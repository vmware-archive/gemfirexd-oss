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

package hydratest.version.partition;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.internal.GemFireVersion;
import hydra.*;

public class PartitionClient {

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void createAsyncEventQueueTask() {
    String result = VersionHelper.createAsyncEventQueue(ConfigPrms.getAsyncEventQueueConfig());
    Log.getLogWriter().info("Task created async event queue: " + result);
  }

  public static void createRegionTask() {
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  public static void reportPartitionAttributesTask() {
    Region r = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    RegionAttributes ra = r.getAttributes();
    PartitionAttributes pa = ra.getPartitionAttributes();
    if (pa == null) {
      throw new HydraConfigException("Partitioning not configured");
    }
    else {
      String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
               + " GemFirePrms deltaPropagation is "
               + VersionHelper.getDeltaPropagation(
                 DistributedSystemHelper.getDistributedSystem().getProperties())
               + ", RegionAttributes cloningEnabled is "
               + VersionHelper.getCloningEnabled(ra)
               + ", the PartitionAttributes are " + pa
               + ", the partition resolver is "
               + VersionHelper.getPartitionResolver(pa)
               + ", the colocated with is "
               + VersionHelper.getColocatedWith(pa)
               + ", the recovery delay is "
               + VersionHelper.getRecoveryDelay(pa)
               + ", the startup recovery delay is "
               + VersionHelper.getStartupRecoveryDelay(pa);
      Log.getLogWriter().info(s);
    }
  }

  public static void reportPoolConfigurationTask() {
    Region r = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    RegionAttributes ra = r.getAttributes();
    String poolName = ra.getPoolName();
    if (poolName == null) {
      throw new HydraConfigException("Pool not configured");
    }
    else {
      Pool pool = PoolHelper.getPool(poolName);
      String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
               + ", the pool configuration is " + PoolHelper.poolToString(pool)
               + ", the multiuser authentication is "
               + VersionHelper.getMultiuserAuthentication(pool)
               + ", the PR single hop enabled is "
               + VersionHelper.getPRSingleHopEnabled(pool);
      Log.getLogWriter().info(s);
    }
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
