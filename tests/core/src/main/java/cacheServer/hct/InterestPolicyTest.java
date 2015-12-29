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
package cacheServer.hct; 

import hydra.*;
import hct.*;
import durableClients.*;

public class InterestPolicyTest extends hct.InterestPolicyTest {

static long killInterval = 60000;

// ======================================================================== 
// additional methods using gemfire/bin/cachserver start/stop
// ======================================================================== 

/**
 * Initializes the BridgeServer using the gemfire start command
 */
public static void InitTask_startCacheServer() {
   if (testInstance == null) {
      testInstance = new InterestPolicyTest();
      ((InterestPolicyTest)testInstance).startCacheServer();
   }
}

/**
 *  Create an xml file (for the given regionConfig).
 *
 *  @param regionConfig - The name of a region description.
 */
static private final String cacheServerStr = "cacheserver";

protected void startCacheServer() {
   // create xml from Cache/RegionPrms
   int myVmid = RemoteTestModule.getMyVmid();
   String clientName = RemoteTestModule.getMyClientName();
   String serverName = "vm_" + myVmid + "_" + clientName + "_" + cacheServerStr;
   String xmlFileName = serverName + ".xml";

   try {
     CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(), null, ConfigPrms.getRegionConfig(), null, ConfigPrms.getBridgeConfig(), null, ConfigPrms.getDiskStoreConfig(), null, xmlFileName);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // may exist if invoked on restart (dynamic stop/start)
      } else {
         throw e;
      }
   }
   
   Log.getLogWriter().info("Starting up cacheServer " + serverName + " with cache.xml file " + xmlFileName);
   GemFireDescription gfd = TestConfig.getInstance().getClientDescription( clientName ).getGemFireDescription();  
   String offHeapMemorySize = gfd.getOffHeapMemorySize();
   String[] extraArgs = null;
   if (offHeapMemorySize != null) {
     extraArgs = new String[] { " off-heap-memory-size=" + offHeapMemorySize };
   } else {
     extraArgs = new String[0];
   }
   CacheServerHelper.startCacheServer(serverName, xmlFileName, extraArgs);
}

/**
 *  Restart the CacheServer (previously started in this VM)
 *  Note that the xml file has already been created and the port assigned.
 */
protected void restartCacheServer() {
   // create xml from Cache/RegionPrms
   int myVmid = RemoteTestModule.getMyVmid();
   String clientName = RemoteTestModule.getMyClientName();
   String serverName = "vm_" + myVmid + "_" + clientName + "_" + cacheServerStr;
   String xmlFileName = serverName + ".xml";

   Log.getLogWriter().info("Starting up cacheServer " + serverName + " with cache.xml file " + xmlFileName);
   CacheServerHelper.startCacheServer(serverName, xmlFileName);
}

public static void HydraTask_stopCacheServer() {
   ((InterestPolicyTest)testInstance).stopCacheServer();
}

public void stopCacheServer() {
   int myVmid = RemoteTestModule.getMyVmid();
   String clientName = RemoteTestModule.getMyClientName();
   String serverName = "vm_" + myVmid + "_" + clientName + "_" + cacheServerStr;
   CacheServerHelper.stopCacheServer(serverName);
}

/**
 * Hydra task for recycling the server
 * 
 * @throws ClientVmNotFoundException
 */
public static void killCacheServer() throws ClientVmNotFoundException {

  long now = System.currentTimeMillis();
  Long lastKill = (Long)BBoard.getInstance().getSharedMap().get(
      "lastKillTime");
  long diff = now - lastKill.longValue();

  if (diff < killInterval) {
    Log.getLogWriter().info("No kill executed");
    return;
  }
  else {
    BBoard.getInstance().getSharedMap().put("lastKillTime", new Long(now));
  }

  ((InterestPolicyTest)testInstance).stopCacheServer();
  int sleepSec = TestConfig.tab().intAt(DurableClientsPrms.restartWaitSec, 1);
  Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds");
  MasterController.sleepForMs(sleepSec * 1000);

  ((InterestPolicyTest)testInstance).restartCacheServer();
  MasterController.sleepForMs(sleepSec * 1000);

  return;
}

}
