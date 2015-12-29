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

package hydratest.grid;

import com.gemstone.gemfire.cache.*;
import hydra.*;
import java.util.*;

/**
 * Tasks for testing hydra grid support.
 */
public class GridClient {

  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  public static void startLocatorAndAdminDSTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  public static void generateCacheXmlTask() {
    String cacheXmlFile = getCacheXmlFile();
    DistributedSystemHelper.connectWithXml(cacheXmlFile);

    // Generate the XML file
    CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(),
                                     null /* dynamicRegionConfig */,
                                     ConfigPrms.getRegionConfig(),
                                     null /* regionNames */,
                                     ConfigPrms.getBridgeConfig(),
                                     ConfigPrms.getPoolConfig(),
                                     ConfigPrms.getDiskStoreConfig(),
                                     ConfigPrms.getGatewaySenderConfig(),
                                     ConfigPrms.getGatewayReceiverConfig(),
                                     ConfigPrms.getAsyncEventQueueConfig(),
                                     null /* functions */,
                                     cacheXmlFile);
  }

  public static void generateCacheXmlFcnTask() {
    String cacheXmlFile = getCacheXmlFile();
    DistributedSystemHelper.connectWithXml(cacheXmlFile);

    // Generate the XML file
    CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(),
                                     null /* dynamicRegionConfig */,
                                     ConfigPrms.getRegionConfig(),
                                     null /* regionNames */,
                                     ConfigPrms.getBridgeConfig(),
                                     ConfigPrms.getPoolConfig(),
                                     ConfigPrms.getDiskStoreConfig(),
                                     GridPrms.getFunctions(),
                                     cacheXmlFile);
  }

  public static void generateCacheXmlMultiTask() {
    String cacheXmlFile = getCacheXmlFile();
    DistributedSystemHelper.connectWithXml(cacheXmlFile);

    // Generate the XML file
    CacheHelper.generateCacheXmlFileNoCheck(ConfigPrms.getCacheConfig(),
                                            null, /* dynamicRegionConfig */
                                            GridPrms.getRegionConfigs(),
                                            null, /* regionNames */
                                            ConfigPrms.getBridgeConfig(),
                                            GridPrms.getPoolConfigs(),
                                            cacheXmlFile);
  }

  public static void openCacheWithXmlTask() {
    String cacheXmlFile = getCacheXmlFile();

    // Create the cache using the XML file
    Cache cache = CacheHelper.createCacheFromXml(cacheXmlFile);
    Region region = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    Log.getLogWriter().info("Created region " + RegionPrms.DEFAULT_REGION_NAME + " with region attributes " + RegionHelper.regionAttributesToString(region.getAttributes()));
  }

  public static void openCacheWithXmlMultiTask() {
    String cacheXmlFile = getCacheXmlFile();

    // Create the cache using the XML file
    Cache cache = CacheHelper.createCacheFromXml(cacheXmlFile);
    Set regions = CacheHelper.getCache().rootRegions();
    for (Iterator i = regions.iterator(); i.hasNext();) {
      Region region = (Region)i.next();
      Log.getLogWriter().info("Created region " + region.getName() + " with region attributes " + RegionHelper.regionAttributesToString(region.getAttributes()));
    }
  }

  public static void putDataTask() {
    Region r = RegionHelper.getRegion(RegionPrms.DEFAULT_REGION_NAME);
    for (int i = 0; i < 10000; i++) {
      r.put(new Integer(i), new Integer(i));
    }
  }

  public static void putDataMultiTask() {
    Set regions = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info("Putting data in regions: " + regions);
    for (int i = 0; i < 10000; i++) {
      for (Iterator it = regions.iterator(); it.hasNext();) {
        Region region = (Region)it.next();
        region.put(new Integer(i), new Integer(i));
      }
    }
  }

  private static String getCacheXmlFile() {
    return System.getProperty("user.dir") + "/"
         + "vm_" + RemoteTestModule.getMyVmid() + "_"
         + System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY) + ".xml";
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
