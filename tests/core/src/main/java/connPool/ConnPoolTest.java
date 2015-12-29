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
package connPool;

import java.util.*;
import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ConnPoolTest {
    
/* The singleton instance of ConnPoolTest in this VM */
static protected ConnPoolTest testInstance;
    
protected static final String VmIDStr = "VmId_";

public synchronized static void HydraTask_createLocator() {
  DistributedSystemHelper.createLocator();
}

public synchronized static void HydraTask_startLocator() {
  DistributedSystemHelper.startLocatorAndAdminDS();
}

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of ParRegTest in this VM.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new ConnPoolTest();
      testInstance.initializeRegion("clientRegion");
   }
}
    
/** Creates and initializes a region in a server.
 */
public synchronized static void HydraTask_initializeServer() {
   if (testInstance == null) {
      testInstance = new ConnPoolTest();
      testInstance.initializeRegion("dataStoreRegion");
      BridgeHelper.startBridgeServer("bridge");
   }
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
protected void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   try {
      PoolDescription poolDescr = RegionHelper.getRegionDescription(regDescriptName).getPoolDescription();
      if (poolDescr != null) {
         CacheHelper.generateCacheXmlFile("cache1", regDescriptName, null, poolDescr.getName(), xmlFile);
      } else {
         CacheHelper.generateCacheXmlFile("cache1", regDescriptName, xmlFile);
      }
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // ok; we use this to reinitialize returning VMs, so the xml file is already there
      } else {
         throw e;
      }
   }   
   RegionHelper.createRegion(regDescriptName);
}
    
/** Creates and initializes the edge client VM using the ClientCacheFactory (vs. CacheFactory)
 */
public synchronized static void HydraTask_initializeClientCache() {
   if (testInstance == null) {
      testInstance = new ConnPoolTest();
      testInstance.initializeClientCache("clientCache", "clientRegion");
   }
}

/**
 *  Create a ClientCache (via ClientCacheFactory) and a Region (via 
 *  RegionFactory) with the given region description.
 *
 *  @param regDescriptName The name of a region description.
 */
protected void initializeClientCache(String cacheConfig, String regionConfig) {
   ClientCache c = ClientCacheHelper.createCache(cacheConfig);

   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   try {
      ClientCacheHelper.generateCacheXmlFile(cacheConfig, regionConfig, xmlFile);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // ok; we use this to reinitialize returning VMs, so the xml file is already there
      } else {
         throw e;
      }
   }

   Region r = ClientRegionHelper.createRegion(regionConfig);
   RegionAttributes ratts = r.getAttributes();
   Log.getLogWriter().info("Created region " + r.getName() + " with attributes " + RegionHelper.regionAttributesToString(ratts));
}
    
}
