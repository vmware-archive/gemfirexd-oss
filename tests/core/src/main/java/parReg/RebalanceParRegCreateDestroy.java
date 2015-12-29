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
package parReg;

import java.util.*;
import java.io.*;

import util.*;
import hydra.*;
import hydra.blackboard.*;
import perffmwk.*;

import rebalance.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * 
 * RebalanceParRegCreateDestroy extensions
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class RebalanceParRegCreateDestroy extends ParRegCreateDestroy {

// ======================================================================
// Cache API Rebalance support
// ======================================================================

/** Creates and initializes the singleton instance of RebalanceParRegCreateDestroy in this VM.
 */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new RebalanceParRegCreateDestroy();
      testInstance.initialize();
   }
   Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
   Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
}

/** Creates and initializes the singleton instance of RebalanceParRegCreateDestroy in
 *  a bridge server.
 */ 
public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new RebalanceParRegCreateDestroy();
      testInstance.initialize();
      BridgeHelper.startBridgeServer("bridge");
   }
   isBridgeClient = false;
   Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
   Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
}

/**
 *  Initialize this test instance
 */
protected void initialize() {
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   isBridgeConfiguration = bridgeNames != null;
   Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
   for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));
      if (regionDescriptName.startsWith("bridge")) { // this is a server region description
         bridgeRegionDescriptNames.add(regionDescriptName);
      } else { 
         // skip the cacheServerRegion (for cacheserver -rebalance vm)
         if (!regionDescriptName.equalsIgnoreCase("cacheServerRegion")) {
            // is either a bridge client or a peer
            regionDescriptNames.add(regionDescriptName);
         }
      }
   }
   if (isBridgeConfiguration) {
      if (bridgeRegionDescriptNames.size() != regionDescriptNames.size()) {
         throw new TestException("Error in test configuration; need equal number of region descriptions for bridge servers and bridge clients");
      }
   }
   Log.getLogWriter().info("bridgeRegionDescriptNames is " + bridgeRegionDescriptNames);
   Log.getLogWriter().info("regionDescriptNames is " + regionDescriptNames);
   theCache = CacheHelper.createCache("cache1");
   destroyThreshold = (int)(regionDescriptNames.size() * 0.8);
   Log.getLogWriter().info("destroyThreshold is " + destroyThreshold);
   Log.getLogWriter().info("ENTRIES_TO_PUT is " + ENTRIES_TO_PUT);

   // Add this VM to the list of VMs available for recycling
   parReg.ParRegBB.getBB().getSharedMap().put(RecoveryStopStart.DataStoreVmStr + RemoteTestModule.getMyVmid(), new Integer(RemoteTestModule.getMyVmid()));

}

/** Creates all PRs in this test (ignoring RegionExistsExceptions) and
 *  performs a Rebalance.
 */
public synchronized static void HydraTask_rebalanceTask() {
   if (testInstance == null) {
      testInstance = new RebalanceParRegCreateDestroy();
      testInstance.initialize();
   }

   // Ensure we have as many test regions as possible before rebalancing
   // method ignores RegionExists Exceptions
   testInstance.doCreateRegionsOnly();
   rebalance.RebalanceTest.HydraTask_rebalanceTask();
}

// ======================================================================
// CacheServer start -rebalance support
// ======================================================================

static private final String cacheServerStr = "cacheserver";

// STARTTASK to generate cacheserver xml file 
public synchronized static void HydraTask_createXml() {
   String key = cacheServerStr;
   String xmlFileName = key + ".xml";

   CacheHelper.createCache(ConfigPrms.getCacheConfig());
   String regionDescriptName = ConfigPrms.getRegionConfig();
   List regionNames = new ArrayList(TestConfig.tab().vecAt(RegionPrms.regionName));
   // Don't add the 'dummy' cacheServerRegion to the xml file
   int index = regionNames.indexOf("cacheServerRegion");
   regionNames.remove(index);

   try {
     // create all regions (with the one RegionDescription)
     // cacheServer -rebalance is an accessor only (localMaxMemory = 0)
     CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(), regionDescriptName, regionNames, xmlFileName);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // initially created by STARTTASK
      } else {
         throw e;
      }
   }   
}

// TASK to start a cacheserver via the gemfire script using xml
// created by initCacheServerXml in earlier STARTTASK
public synchronized static void HydraTask_rebalanceWithCacheServer() {

   if (testInstance == null) {
      testInstance = new RebalanceParRegCreateDestroy();
   }

   // What's the current state of the PR
   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("HydraTask_rebalanceWithCacheServer() expects hydra client to have created cache and PR via initialization tasks");
   }

   // configure regions (so we can get PartitionedRegionDetails before)
   testInstance.doCreateRegionsOnly();

   ResourceManager rm = myCache.getResourceManager();
   Set<PartitionRegionInfo> before = PartitionRegionHelper.getPartitionRegionInfo(myCache);

   // clear rebalance/recovery counters
   RebalanceBB.getBB().getSharedCounters().zero(RebalanceBB.recoveryRegionCount);

   // Launch cacheServer to perform a rebalance
   String[] extraArgs = RebalancePrms.getExtraCacheServerArgs();
   String xmlFileName = cacheServerStr + ".xml";

   long numCacheServers = RebalanceBB.getBB().getSharedCounters().incrementAndRead(RebalanceBB.numCacheServers);
   String serverName = cacheServerStr + "_" + numCacheServers;
   Log.getLogWriter().info("Starting up cacheServer " + serverName + " with cache.xml file " + xmlFileName + " for Rebalance");
   GemFireDescription gfd = TestConfig.getInstance().getClientDescription( RemoteTestModule.getMyClientName() ).getGemFireDescription();  
   String offHeapMemorySize = gfd.getOffHeapMemorySize();
   if (offHeapMemorySize != null) {
     extraArgs = Arrays.copyOf(extraArgs, extraArgs.length+1);
     extraArgs[extraArgs.length-1] = " off-heap-memory-size=" + offHeapMemorySize;
   }

   // protect cacheserver from region destroys during startup 
   // ResourceObserver will clear on rebalanceStarted
   RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.criticalSection);
   CacheServerHelper.startCacheServer(serverName, xmlFileName, extraArgs);

   RebalanceUtil.waitForRecovery();

   // How do things look after cacheServer rebalanced
   // configure regions (so we can get PartitionedRegionDetails before)
   testInstance.doCreateRegionsOnly();

   String regionPath = null;
   String regionName = null;
   for (PartitionRegionInfo prdBefore : before ) {
      Log.getLogWriter().info("Before Rebalance\n" + RebalanceUtil.partitionedRegionDetailsToString(prdBefore));
      regionPath = prdBefore.getRegionPath();
      regionName = RebalanceUtil.getRegionName(prdBefore);
      Region region = myCache.getRegion(regionName);
      PartitionRegionInfo prdAfter = PartitionRegionHelper.getPartitionRegionInfo(region);
      Log.getLogWriter().info("After Rebalance \n" + RebalanceUtil.partitionedRegionDetailsToString(prdAfter));

      if (RebalancePrms.verifyBalance()) {
         RebalanceUtil.isBalanceImproved(regionName, prdBefore.getPartitionMemberInfo(), prdAfter.getPartitionMemberInfo());
         Log.getLogWriter().info("PR for region " + regionName + " is balanced");
      }
   }

   // stop (or kill) the cacheServer
   CacheServerHelper.stopCacheServer(serverName);

}

/** Task to create and destroy partitioned regions, unless the cacheserver start -rebalance is starting up
 */
public static void HydraTask_createDestroy() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   try {
     do {
       long criticalSection = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.criticalSection);
       if (criticalSection == 0) {
          testInstance.doCreateDestroy();
       } else {
          MasterController.sleepForMs(1000);
       }
     } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   } finally {
     if (isBridgeConfiguration) {
       for (Iterator ri = theCache.rootRegions().iterator(); ri.hasNext(); ) {
         Region r = (Region) ri.next();
         ClientHelper.release(r);
       }
     }
   }
}


/** Task to start a rebalance operation, do entry ops and 
 *  execute a cancel rebalance 
 */
public static void HydraTask_rebalanceOps() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
     ((RebalanceParRegCreateDestroy)testInstance).doRebalanceOps();
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** Start rebalance and randomly cancel an active rebalance
 */
protected void doRebalanceOps() {
   Cache myCache = CacheHelper.getCache();
   ResourceManager rm = myCache.getResourceManager();

   // start rebalance
   RebalanceFactory factory = rm.createRebalanceFactory();
   Log.getLogWriter().info("Starting rebalance ...");
   RebalanceOperation newOp = factory.start();
   Log.getLogWriter().info("Rebalance started with operation " + newOp.toString()); 
 
   // cancel a random rebalance operation
   Object[] opArray = rm.getRebalanceOperations().toArray();
   if (opArray.length > 0) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, opArray.length-1);
      RebalanceOperation op = (RebalanceOperation)opArray[randInt];
      Log.getLogWriter().info("Cancelling rebalance for operation " + op.toString());
 
      try {
         op.cancel();
      } catch(Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (!op.isCancelled()) {
         throw new TestException("Operation " + op.toString() + " was not cancelled as expected");
      }
      Log.getLogWriter().info("Operation cancelled (as expected)");
   } else {
      Log.getLogWriter().info("Cannot cancel rebalance, number of rebalance operations in progress = " + opArray.length);
   }
} 

// For all regions in the cache, put ENTRIES_TO_PUT unique entries
// into each region
public static void HydraTask_populateRegions() {
   ((RebalanceParRegCreateDestroy)testInstance).populateRegions();
}

protected void populateRegions() {
   Cache myCache = CacheHelper.getCache();
   Set regions = myCache.rootRegions();
   for (Iterator iter = regions.iterator(); iter.hasNext();) {
      Region aRegion = (Region)iter.next();
      Log.getLogWriter().info("Putting " + ENTRIES_TO_PUT + " entries into " + aRegion.getFullPath());
      for (int i = 1; i <= ENTRIES_TO_PUT; i++) {
         aRegion.put("" + System.currentTimeMillis(), new Integer(i));
      }
   }
}

public static void HydraTask_recycleDataStore() {
   // recycle another VM (create work for rebalance)
   util.StopStartVMs.stopStartOtherVMs(1);
}

}
