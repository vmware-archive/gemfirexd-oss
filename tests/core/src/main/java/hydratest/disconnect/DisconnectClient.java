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
package hydratest.disconnect;

import com.gemstone.gemfire.cache.Region;
import hydra.*;

/**
 * A client that tests the hydra {@link hydra.ClientVmMgr} API for disconnects.
 */
public class DisconnectClient {

  public static final String REGION_NAME = "region";
  public static HydraThreadLocal localregion = new HydraThreadLocal();

  public static void startLocatorTask() {
    DistributedSystemHelper.createLocator();
    DistributedSystemHelper.startLocatorAndDS();
  }

  public static void connectTask() {
    DistributedSystemHelper.connect();
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    Region region = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
    localregion.set(region);
  }

  public static void disconnectTask() throws ClientVmNotFoundException {
    MasterController.sleepForMs(5000);
    String clientName = TestConfig.tab().stringAt(DisconnectPrms.clientName);
    if (TestConfig.tab().booleanAt(DisconnectPrms.onDemand)) {
      ClientVmInfo info = ClientVmMgr.stop(
        "testing synchronous disconnect using client name",
        ClientVmMgr.NICE_DISCONNECT, ClientVmMgr.ON_DEMAND,
        new ClientVmInfo(null, clientName, null)
      );
      MasterController.sleepForMs(11000);
      info = ClientVmMgr.start(
        "testing synchronous reconnect using client vm info", info
      );
    } else {
      ClientVmInfo info = ClientVmMgr.stop(
        "testing synchronous disconnect using client name",
        ClientVmMgr.NICE_DISCONNECT, ClientVmMgr.IMMEDIATE,
        new ClientVmInfo(null, clientName, null)
      );
    }
  }

  public static void putTask() {
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    Region region = (Region)localregion.get();
    long end = System.currentTimeMillis() + 10000;
    while (System.currentTimeMillis() < end) {
      region.put(Integer.valueOf(tid), Integer.valueOf(tid));
    }
  }

  public static void logTask() {
    Log.getLogWriter().info("LOG TASK");
  }
}
