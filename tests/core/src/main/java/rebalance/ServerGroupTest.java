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
package rebalance;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceManagerStats;

import parReg.*;
import hydra.*;
import util.*;
import rebalance.*;
import recovDelay.*;

import java.util.*;


/** Test rebalance with server groups
 */
public class ServerGroupTest {
    
/* The singleton instance of CapacityTest in this VM */
static public ServerGroupTest testInstance;
    
protected final int numRegions = 7;  // the number of PRs to create
protected List<Region> regionList = new ArrayList();   // The pr regions under test
protected Region extraRegion;        // An extra region the grows while rebalancing
protected int regionNameIndex = 0;   // Used for creating unique names for multiple PRs

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of this test class.
 */
public synchronized static void HydraTask_initClient1() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ServerGroupTest();
      CacheHelper.createCache("cache1");
      for (int i = 1; i <= testInstance.numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("clientPrRegionGroup1"));
      }
   }
}
    
/** Creates and initializes the singleton instance of this test class.
 */
public synchronized static void HydraTask_initClient2() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ServerGroupTest();
      CacheHelper.createCache("cache1");
      for (int i = 1; i <= testInstance.numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("clientPrRegionGroup2"));
      }
      testInstance.extraRegion = RegionHelper.createRegion("clientExtraRegion");
   }
}
    
/** Creates and initializes the singleton instance of this test class.
 */
public synchronized static void HydraTask_initServer1() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ServerGroupTest();
      CacheHelper.createCache("cache1");
      for (int i = 1; i <= testInstance.numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("serverPrRegion"));
      }
      BridgeHelper.startBridgeServer("bridge1");
   }
}
    
/** Creates and initializes the singleton instance of this test class.
 */
public synchronized static void HydraTask_initServer2() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ServerGroupTest();
      CacheHelper.createCache("cache1");
      for (int i = 1; i <= testInstance.numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("serverPrRegion"));
      }
      testInstance.extraRegion = RegionHelper.createRegion("serverExtraRegion");
      BridgeHelper.startBridgeServer("bridge2");
   }
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 *
 *  @returns The created region.
 */
public Region initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String regionName = RegionHelper.getRegionDescription(regDescriptName).getRegionName();
   regionName = regionName + (++regionNameIndex);
   Region aRegion = RegionHelper.createRegion(regionName, regDescriptName);
   return aRegion;
}
    
// ========================================================================
// hydra task methods

/** Run a rebalance.
 */
public static void HydraTask_rebalance() {
   for (Region aRegion : testInstance.regionList) {
      Map[] mapArr = BucketState.getBucketMaps(aRegion);
      Log.getLogWriter().info("Before rebalance: For " + aRegion.getFullPath() + 
         ", Primary map: " + mapArr[0] + ", bucket map: " + mapArr[1]);
   }
   ResourceManager resMan = CacheHelper.getCache().getResourceManager();
   RebalanceFactory factory = resMan.createRebalanceFactory();
   try {
      Log.getLogWriter().info("Starting rebalancing");
      long startTime = System.currentTimeMillis();
      RebalanceOperation rebalanceOp = factory.start();
      RebalanceResults rebalanceResults = rebalanceOp.getResults();
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      Log.getLogWriter().info("Rebalance completed in " + duration + " ms");
      Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));
      for (Region aRegion : testInstance.regionList) {
         Map[] mapArr = BucketState.getBucketMaps(aRegion);
         Log.getLogWriter().info("After rebalance: For " + aRegion.getFullPath() + 
             ", Primary map: " + mapArr[0] + ", bucket map: " + mapArr[1]);
      }
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   if (RebalanceBB.getBB().getSharedMap().containsKey("TestIsDone")) {
      throw new StopSchedulingOrder("Test has finished");
   }
}

/** Put into the PR until the size of the vm has reached a certain
 *  percent of tenured heap.
 */
public static void HydraTask_loadPRs() {
   testInstance.loadUntilTenuredPercent(50, testInstance.regionList);
}

/** Put into the region(s) until the size of the vm has reached a certain
 *  percent of tenured heap.
 */
public static void HydraTask_loadExtraRegion() {
   List<Region> regList = new ArrayList();
   regList.add(testInstance.extraRegion); 
   try {
      testInstance.loadUntilTenuredPercent(80, regList);
   } catch (StopSchedulingTaskOnClientOrder ex) {
      RebalanceBB.getBB().getSharedMap().put("TestIsDone", new Boolean(true));
      throw ex;
   }
}

/** Put into the PR until the tenured heap is the given percent of capacity.
 */
protected void loadUntilTenuredPercent(int percent, List<Region> regList) {
   int numKeys = RebalancePrms.getNumKeys();
   boolean useHashKey = RebalancePrms.getUseHashKey();

   // do the load
   GsRandom rand = TestConfig.tab().getRandGen();
   RandomValues randomValues = new RandomValues();
   final long LOG_INTERVAL_MILLIS = 10000;
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   ResourceManagerStats stats = ((InternalResourceManager)(CacheHelper.getCache().getResourceManager())).getStats();
   long criticalThreshold = stats.getCriticalThreshold();
   long stopLoadThreshold = (long)((float)criticalThreshold * ((float)(percent)/(float)100));
   Log.getLogWriter().info("criticalThreshold is " + criticalThreshold +
                           ", load stops when tenured heap reaches " + stopLoadThreshold);
   int regionIndex = 0;
   do {
      Region aRegion = regList.get(regionIndex);
      regionIndex = (regionIndex+1) % regList.size();
      int randInt = rand.nextInt(1, 100);
      String keyStr = NameFactory.getNextPositiveObjectName();
      Object aKey = keyStr;
      if (useHashKey) {
         aKey = new HashKey();
         ((HashKey)aKey).key = keyStr;
      } 
      BaseValueHolder anObj = new ValueHolder(keyStr, randomValues);
      aRegion.create(aKey, anObj);

      // see if it's time to stop
      if (stats.getTenuredHeapUsed() >= stopLoadThreshold) {
         throw new StopSchedulingTaskOnClientOrder(
                   "Region(s) have exceeded " + percent + " percent of criticalThreshold " +
                   "; current tenured heap used " + stats.getTenuredHeapUsed() + 
                   " tenured heap limit to halt load: " + stopLoadThreshold +
                   "; current region size is " + aRegion.size() +
                   ", last key added: " + aKey +
                   ", last value added: " +
                   anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
                   ", extraObject " + TestHelper.toString(anObj.extraObject));
      }

      // log progress occasionally
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + 
             " entries into " + TestHelper.regionToString(aRegion, false) + 
             "; current tenured heap used " + stats.getTenuredHeapUsed() + 
             " tenured heap limit to halt load: " + stopLoadThreshold +
             ", last key added: " + aKey +
             ", last value added: " +
             anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
             ", extraObject " + TestHelper.toString(anObj.extraObject));
         lastLogTime = System.currentTimeMillis();
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

}
