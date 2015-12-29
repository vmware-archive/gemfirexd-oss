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

package cacheServer.wan;

import com.gemstone.gemfire.cache.Cache;
import wan.*;

import hydra.CacheHelper;
import hydra.CacheServerHelper;
import hydra.CachePrms;
import hydra.ClientPrms;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigHashtable;
import hydra.ConfigPrms;
import hydra.DiskStoreHelper;
import hydra.DistributedSystemHelper;
import hydra.EdgeHelper;
import hydra.GatewayHubHelper;
import hydra.GatewayHubBlackboard;
import hydra.GemFireDescription;
import hydra.GemFirePrms;
import hydra.GsRandom;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedMap;

/**
 * Uses gemfire/bin/cacheServer (stop/start) instead of Application specific CacheServer Gateways
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class WANClient extends wan.WANClient {

  protected static Cache xmlCache;   // CacheCreation object created by CacheHelper methods
  static String gatewayXmlFilename;

  //============================================================================
  // INITTASKS
  //============================================================================

  /**
   * Initializes a peer cache using using the {@link CacheClientPrms} to generate the CacheCreation object.
   * This is subsequently used by startPeerGateway to create the cacheXmlFile use use by the test.
   */
  public static synchronized void initPeerGateway() {
    if (xmlCache == null) {
       String gatewayHubConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayHubConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayHubConfig, null));
       String cacheConfig = TestConfig.tasktab().stringAt(CacheClientPrms.cacheConfig, TestConfig.tab().stringAt(CacheClientPrms.cacheConfig, null));
   
       int myVmid = RemoteTestModule.getMyVmid();
       String clientName = RemoteTestModule.getMyClientName();
       gatewayXmlFilename = "vm_" + myVmid + "_" + clientName + ".xml";
   
       xmlCache = CacheHelper.startCacheXmlGenerationForGateway(cacheConfig, null, gatewayHubConfig, gatewayXmlFilename);
     }
  }

  public static synchronized void startPeerGateway() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
       String gatewayHubConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayHubConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayHubConfig, null));
       String cacheConfig = TestConfig.tasktab().stringAt(CacheClientPrms.cacheConfig, TestConfig.tab().stringAt(CacheClientPrms.cacheConfig, null));
       String gatewayConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayConfig, null));
       String regionConfig = TestConfig.tasktab().stringAt(CacheClientPrms.regionConfig, TestConfig.tab().stringAt(CacheClientPrms.regionConfig, null));
       String diskStoreConfig = ConfigPrms.getDiskStoreConfig();

       try {
          CacheHelper.finishCacheXmlGenerationForGateway(xmlCache, cacheConfig, gatewayHubConfig, gatewayConfig, regionConfig, null, null, null, diskStoreConfig, null, gatewayXmlFilename);
       } catch (HydraRuntimeException e) {
          String errStr = e.toString();
          if (errStr.indexOf("Cache XML file was already created") >= 0) {
             // may exist if invoked on restart (dynamic stop/start)
          } else {
             throw e;
          }
       }
       CacheHelper.createCacheFromXml(gatewayXmlFilename);
     }
  }

  /**
   * Initializes a gateway server cache using the {@link CacheServerPrms} to generate the CacheCreation object.
   * This is subsequently used by startServerGatewayWithCacheServer to create the cacheXmlFile use use by cacheserver.
   */
  public static synchronized void initServerGateway() {
    if (xmlCache == null) {
       String gatewayHubConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayHubConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayHubConfig, null));
       String cacheConfig = TestConfig.tasktab().stringAt(CacheServerPrms.cacheConfig, TestConfig.tab().stringAt(CacheServerPrms.cacheConfig, null));
   
       int myVmid = RemoteTestModule.getMyVmid();
       String clientName = RemoteTestModule.getMyClientName();
       gatewayXmlFilename = "vm_" + myVmid + "_" + clientName + ".xml";
   
       xmlCache = CacheHelper.startCacheXmlGenerationForGateway(cacheConfig, null, gatewayHubConfig, gatewayXmlFilename);
     }
  }

  /**
   * Starts a cacheServer using the {@link CacheServerPrms} to generate the cache.xml.
   */
  static private final String cacheServerStr = "cacheserver";
  public static synchronized void startServerGateway() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
       String gatewayHubConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayHubConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayHubConfig, null));
       String cacheConfig = TestConfig.tasktab().stringAt(CacheServerPrms.cacheConfig, TestConfig.tab().stringAt(CacheServerPrms.cacheConfig, null));
       String gatewayConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayConfig, null));
       String regionConfig = TestConfig.tasktab().stringAt(CacheServerPrms.regionConfig, TestConfig.tab().stringAt(CacheServerPrms.regionConfig, null));
       String bridgeConfig = TestConfig.tasktab().stringAt(CacheServerPrms.bridgeConfig, TestConfig.tab().stringAt(CacheServerPrms.bridgeConfig, null));
       String diskStoreConfig = ConfigPrms.getDiskStoreConfig();
   
       try {
          CacheHelper.finishCacheXmlGenerationForGateway(xmlCache, cacheConfig, gatewayHubConfig, gatewayConfig, regionConfig, null, bridgeConfig, null, diskStoreConfig, null, gatewayXmlFilename);
       } catch (HydraRuntimeException e) {
          String errStr = e.toString();
          if (errStr.indexOf("Cache XML file was already created") >= 0) {
             // may exist if invoked on restart (dynamic stop/start)
          } else {
             throw e;
          }
       }
       CacheHelper.createCacheFromXml(gatewayXmlFilename);
       Log.getLogWriter().info("Starting up Gateway with cache.xml file " + gatewayXmlFilename);
     }
  }

  /**
   * Starts a cacheServer using the {@link CacheServerPrms} to generate the cache.xml.
   */
  public static synchronized void startServerGatewayWithCacheServer() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
       String gatewayHubConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayHubConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayHubConfig, null));
       String cacheConfig = TestConfig.tasktab().stringAt(CacheServerPrms.cacheConfig, TestConfig.tab().stringAt(CacheServerPrms.cacheConfig, null));
       String gatewayConfig = TestConfig.tasktab().stringAt(CacheServerPrms.gatewayConfig, TestConfig.tab().stringAt(CacheServerPrms.gatewayConfig, null));
       String regionConfig = TestConfig.tasktab().stringAt(CacheServerPrms.regionConfig, TestConfig.tab().stringAt(CacheServerPrms.regionConfig, null));
       String bridgeConfig = TestConfig.tasktab().stringAt(CacheServerPrms.bridgeConfig, TestConfig.tab().stringAt(CacheServerPrms.bridgeConfig, null));
       String diskStoreConfig = ConfigPrms.getDiskStoreConfig();
   
       try {
          CacheHelper.finishCacheXmlGenerationForGateway(xmlCache, cacheConfig, gatewayHubConfig, gatewayConfig, regionConfig, null, bridgeConfig, null, diskStoreConfig, null, gatewayXmlFilename);
       } catch (HydraRuntimeException e) {
          String errStr = e.toString();
          if (errStr.indexOf("Cache XML file was already created") >= 0) {
             // may exist if invoked on restart (dynamic stop/start)
          } else {
             throw e;
          }
       }
       startCacheServer();
     }
  }

  public static void startCacheServer() {
    int myVmid = RemoteTestModule.getMyVmid();
    String clientName = RemoteTestModule.getMyClientName();
    String serverName = "vm_" + myVmid + "_" + clientName + "_" + cacheServerStr;
    Log.getLogWriter().info("Starting up Gateway CacheServer " + serverName + " with cache.xml file " + gatewayXmlFilename);
    GemFireDescription gfd = TestConfig.getInstance().getClientDescription( clientName ).getGemFireDescription();  
    String offHeapMemorySize = gfd.getOffHeapMemorySize();
    String[] extraArgs = null;
    if (offHeapMemorySize != null) {
      extraArgs = new String[] { " off-heap-memory-size=" + offHeapMemorySize };
    } else {
      extraArgs = new String[0];
    }
    CacheServerHelper.startCacheServer(serverName, gatewayXmlFilename, extraArgs);
  }

  public static void stopCacheServer() {
    int myVmid = RemoteTestModule.getMyVmid();
    String clientName = RemoteTestModule.getMyClientName();
    String serverName = "vm_" + myVmid + "_" + clientName + "_" + cacheServerStr;
    CacheServerHelper.stopCacheServer(serverName);
  }

}
