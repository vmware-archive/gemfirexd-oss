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

package hydratest.locators;

import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import perffmwk.*;
import java.util.List;

/**
 * Tasks for testing hydra locator support.
 */
public class LocatorClient {

  public static void createLocatorWhenAlreadyMasterManagedTask() {
    try {
      DistributedSystemHelper.createLocator();
      String s = "Failed to prevent client from creating client-managed locator"
               + " when locators are already master-managed";
      throw new HydraRuntimeException(s); // fail with no exception
    } catch (HydraConfigException e) {
      String err = "Locators are being managed by the hydra master controller";
      if (e.getMessage().indexOf(err) == -1) {
        throw e; // fail with unexpected exception
      } // else pass
    }
  }

  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Starts the first locator using an admin distributed system.
   */
  public static void startFirstLocatorAndAdminDSTask() {
    long locatorNumber = LocatorBlackboard.getInstance().getSharedCounters()
                         .incrementAndRead(LocatorBlackboard.locatorNumber);
    if (locatorNumber == 1) { // I am the first
      DistributedSystemHelper.startLocatorAndAdminDS();
    }
  }

  /**
   * Starts the first locator using an non-admin distributed system.
   */
  public static void startFirstLocatorAndDSTask() {
    long locatorNumber = LocatorBlackboard.getInstance().getSharedCounters()
                         .incrementAndRead(LocatorBlackboard.locatorNumber);
    if (locatorNumber == 1) { // I am the first
      DistributedSystemHelper.startLocatorAndDS();
    }
  }

  public static void startLocatorAndAdminDSTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  public static void startLocatorAndDSTask() {
    DistributedSystemHelper.startLocatorAndDS();
  }

  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }

  public static void bounceLocatorTask() throws ClientVmNotFoundException {
    ClientVmInfo info = new ClientVmInfo(null, "loc*", null);
    Log.getLogWriter().info("Sleeping 2 seconds before locator stop");
    MasterController.sleepForMs(2000);
    info = ClientVmMgr.stop("Stopping locator: " + info,
           ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, info);
    Log.getLogWriter().info("Sleeping 2 seconds before locator start");
    MasterController.sleepForMs(2000);
    ClientVmMgr.start("Restarting locator: " + info, info);
  }

  public static void connectTask() {
    DistributedSystemHelper.connect();
  }

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String gatewaySenderConfig = ConfigPrms.getGatewaySenderConfig();
    if (gatewaySenderConfig != null) {
      GatewaySenderHelper.createAndStartGatewaySenders(gatewaySenderConfig);
    }
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void createCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void createRegionTask() {
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }

  public static void createGatewayHubTask() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
    GatewayHubHelper.createGatewayHub(gatewayHubConfig);
  }

  public static void addGatewaysTask() {
    String gatewayConfig = ConfigPrms.getGatewayConfig();
    GatewayHubHelper.addGateways(gatewayConfig);
  }

  public static void startGatewayHubTask() {
    GatewayHubHelper.startGatewayHub();
  }

  /**
   * TASK to validate the expected number of members.
   */
  public static void validateExpectedMembersTask() {
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    int actual = ds == null ? 0
               : DistributedSystemHelper.getMembers().size();
    int expected = LocatorPrms.getExpectedMembers();
    if (actual == expected) {
      String s = "Have expected " + expected + " members";
      Log.getLogWriter().info(s);
    } else {
      String s = "Expected " + expected + " members, found " + actual;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * TASK to validate that subscribers received all publisher put events.
   */
  public static void validateExpectedEventsTask() {
    int puts = readIntStat("cacheperf.CachePerfStats", "puts");
    int events = readIntStat("cacheperf.CachePerfStats", "updateEvents");
    if (events == puts) {
      Log.getLogWriter().info("Had " + events + " puts and events");
    } else {
      String s = "Had " + puts + " puts but " + events + " events";
      throw new HydraRuntimeException(s);
    }
  }
  private static int readIntStat(String statType, String statName) {
    MasterController.sleepForMs(10000); // allow archiver catch up
    String spec = "* " // search all archives
         + statType + " "
         + "* " // match all instances
         + statName + " "
         + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
         + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE + " "
         + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;

    List aList = PerfStatMgr.getInstance().readStatistics(spec, true);
    int value = 0;
    for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      value += stat.getMax();
    }
    return value;
  }
}
