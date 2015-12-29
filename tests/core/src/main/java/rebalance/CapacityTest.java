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
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import parReg.*;
import hydra.*;
import util.*;
import rebalance.*;
import recovDelay.*;

import java.util.*;


/** Test class for PR capacity and rebalancing.
 */
public class CapacityTest {
    
/* The singleton instance of CapacityTest in this VM */
static public CapacityTest testInstance;
    
protected Region aRegion;        // The region under test

protected static final String primaryMapKey = "primaryMap";
protected static final String bucketMapKey = "bucketMap";

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of ParRegTest in this VM.
 */
public synchronized static void HydraTask_initAccessor() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new CapacityTest();
      testInstance.initializeRegion("accessorRegion", 0);
      testInstance.initializeInstance();
   }
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM.
 */
public synchronized static void HydraTask_initDataStore() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new CapacityTest();
      int LMM = getLocalMaxMemory();
      testInstance.initializeRegion("dataStoreRegion", LMM);
      testInstance.initializeInstance();
      RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.numDataStores);
      RebalanceBB.getBB().getSharedMap().put(RebalanceBB.LocalMaxMemoryKey + RemoteTestModule.getMyVmid(),
                          new Integer(LMM));
   }
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM.
 */
public synchronized static void HydraTask_initEdge() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      CacheHelper.createCache("cache1");
      testInstance = new CapacityTest();
      testInstance.aRegion = RegionHelper.createRegion("accessorRegion");
      testInstance.initializeInstance();
   }
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM.
 */
public synchronized static void HydraTask_initBridge() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      CacheHelper.createCache("cache1");
      testInstance = new CapacityTest();
      testInstance.aRegion = RegionHelper.createRegion("dataStoreRegion");
      RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.numDataStores);
      BridgeHelper.startBridgeServer("bridge");
   }
}
    
/** For replacement test, create the PR, rebalance and verify that this
 *  vm hosts the equivalent capacity that it held before being stopped.
 */
public synchronized static void HydraTask_replaceDataStore() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize();
      testInstance = new CapacityTest();
      int LMM = getLocalMaxMemory();
      testInstance.initializeRegion("dataStoreRegion", LMM);
      testInstance.initializeInstance();
      testInstance.verifyReplacedCapacity();
   }
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 *  @param localMaxMemory The localMaxMemory to use for this region.
 */
public void initializeRegion(String regDescriptName, int localMaxMemory) {
   CacheHelper.createCache("cache1");
   AttributesFactory factory = RegionHelper.getAttributesFactory(regDescriptName);
   if (localMaxMemory != -1) {
      RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      PartitionAttributesFactory prFactory = new PartitionAttributesFactory(prAttr);
      prFactory.setLocalMaxMemory(localMaxMemory);
      factory.setPartitionAttributes(prFactory.create());
   }
   String regionName = (String)((TestConfig.tab().vecAt(RegionPrms.regionName)).get(0));
   aRegion = RegionHelper.createRegion(regionName, factory);
}
    
/**
 *  Initialize this test instance
 */
public void initializeInstance() {
   int hashKeyLimit = RebalancePrms.getHashKeyLimit();
   HashKey.hashLimit = hashKeyLimit;
   Log.getLogWriter().info("HashKey.hashLimit is: " + HashKey.hashLimit);
}

/** Return a value for localMaxMemory for this vm.
 *  
 */
protected static int getLocalMaxMemory() {
   int lmm = -1;
   String lmmStr = RebalancePrms.getLocalMaxMemory();
   String searchStr = "incrementBy_";
   int index = lmmStr.indexOf(searchStr);
   if (index >= 0) { // string is of the form incrementBy_N
      int increment = Integer.valueOf(lmmStr.substring(searchStr.length(), lmmStr.length()));
      int lmmCounter = (int)(ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.LocalMaxMemoryCounter));
      lmm = increment * lmmCounter;
   } else if (lmmStr.equalsIgnoreCase("default")) {
      lmm = -1;
   } else {
      lmm = Integer.valueOf(lmmStr);
   }
   Log.getLogWriter().info("LocalMaxMemory is " + lmm);
   return lmm;
}

// ========================================================================
// hydra task methods

/** Put into the PR until all data store vms exceed local max memory.
 */
public static void HydraTask_loadUntilFull() {
   testInstance.loadUntilFull();
}

/** Put into the PR until RebalancePrms-numKeys has been reached.
 */
public static void HydraTask_loadUntilNumKeys() {
   testInstance.loadUntilNumKeys();
}

/** Add a new vm (adds capacity) to the ds and rebalance with verification.
 */
public static void HydraTask_addCapacity() {
   testInstance = new CapacityTest();
   testInstance.addCapacity();
}

/** Verify all keys/values are present
 */
public static void HydraTask_verifyRegionSize() {
   if (testInstance == null) { // vm not added to pr capacity yet
      Log.getLogWriter().info("Not verifying keys/values because this vm has not yet been initialized");
   } else {
      testInstance.verifyRegionSize();
   }
}

/** Stop a vm, write the current PR state to the bb before stopping.
 */
public static void HydraTask_stopStart() {
   testInstance.stopStart();
}

/** Rebalance the PR.
 */
public static void HydraTask_rebalance() {
   long controller = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.rebalance);
   if (controller == 1) {
      ParRegUtil.doRebalance();
   } 
}

/** Verify that a vm with localMaxMemory N has half the data as a vm with
 *  localMaxMemory 2N.
 */
public synchronized static void HydraTask_verifyVariedLMM() {
   testInstance.verifyLmmBasedOnIdealSize();
   testInstance.verifyLmmBasedOnRelativeSize();
}

/** Record the local size of this VM's PR (this is the number of entries
 *  in this vm's PR, not the number of entries in the entire pr).
 */
public static void HydraTask_recordLocalSize() {
   long localSize = ((PartitionedRegion)testInstance.aRegion).getLocalSize();
   Log.getLogWriter().info("Local size is " + localSize);
   RebalanceBB.getBB().getSharedMap().put(RebalanceBB.LocalSizeKey + RemoteTestModule.getMyVmid(),
                   new Long(localSize));
}

// ========================================================================
// 

/** Put into the PR until local max memory is exceeded in all vms.
 */
protected void loadUntilFull() {
   // do the load
   RandomValues randomValues = new RandomValues();
   final long LOG_INTERVAL_MILLIS = 10000;
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   long numDataStores = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.numDataStores);
   do {
      String key = NameFactory.getNextPositiveObjectName();
      BaseValueHolder anObj = new ValueHolder(key, randomValues);
      aRegion.create(key, anObj);

      // log progress occasionally
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + 
             " entries into " + TestHelper.regionToString(aRegion, false) + 
             ", last key added: " + key +
             ", last value added: " +
             anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
             ", extraObject " + TestHelper.toString(anObj.extraObject));
         lastLogTime = System.currentTimeMillis();

         // see if it is time to stop
         long counter = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.numExceededLMMAlerts);
         if (counter >= numDataStores) {
            throw new StopSchedulingTaskOnClientOrder("All vms have exceeded local max memory" +
                      "; current region size is " + aRegion.size() +
                      ", last key added: " + key +
                      ", last value added: " +
                      anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
                      ", extraObject " + TestHelper.toString(anObj.extraObject));
         }
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   Object error = RebalanceBB.getBB().getSharedMap().get(RebalanceBB.ErrorKey);
   if (error != null) {
      throw new TestException(error.toString());
   }
}

/** Put into the PR until RebalancePrm.numKeys has been reached.
 */
protected void loadUntilNumKeys() {
   int numKeys = RebalancePrms.getNumKeys();
   boolean useHashKey = RebalancePrms.getUseHashKey();

   // do the load
   RandomValues randomValues = new RandomValues();
   final long LOG_INTERVAL_MILLIS = 10000;
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
      String keyStr = NameFactory.getNextPositiveObjectName();
      Object aKey = keyStr;
      if (useHashKey) {
         aKey = new HashKey();
         ((HashKey)aKey).key = keyStr;
      } 
      BaseValueHolder anObj = new ValueHolder(keyStr, randomValues);
      aRegion.create(aKey, anObj);

      // see if it's time to stop
      if (aRegion.size() >= numKeys) {
         throw new StopSchedulingTaskOnClientOrder("Region has exceeded target number of keys " + 
                   numKeys + "; current region size is " + aRegion.size() +
                   ", last key added: " + aKey +
                   ", last value added: " +
                   anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
                   ", extraObject " + TestHelper.toString(anObj.extraObject));
      }

      // log progress occasionally
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + 
             " entries into " + TestHelper.regionToString(aRegion, false) + 
             ", last key added: " + aKey +
             ", last value added: " +
             anObj.getClass().getName() + " with myValue " + TestHelper.toString(anObj.myValue) + 
             ", extraObject " + TestHelper.toString(anObj.extraObject));
         lastLogTime = System.currentTimeMillis();
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** Bring a new vm into the ds and rebalance to create new capacity.
 */
protected void addCapacity() {
   PRObserver.installObserverHook();
   Integer myVmId = new Integer(RemoteTestModule.getMyVmid());

   // create the region (this adds capacity)
   String lmmStr = RebalancePrms.getLocalMaxMemory();
   if (lmmStr == null) { // RebalancePrms.localMaxMemory not set in bridge tests
      CacheHelper.createCache("cache1");
      aRegion = RegionHelper.createRegion("dataStoreRegion"); 
      BridgeHelper.startBridgeServer("bridge");
   } else {
      initializeRegion("dataStoreRegion", getLocalMaxMemory()); 
   }
   Log.getLogWriter().info("After creating region, region size is " + aRegion.size());
   long numDataStores = RebalanceBB.getBB().getSharedCounters().incrementAndRead(RebalanceBB.numDataStores);
   long numDataStoresBefore = numDataStores - 1;

   // wait for recovery to run in this vm
   if (aRegion.getAttributes().getPartitionAttributes().getRedundantCopies() > 0) {
      PRObserver.waitForRebalRecov(myVmId, 1, 1, null, null, false); 
   }

   // rebalance to cause this new vm to be used in the PR and gain buckets
   Map[] mapArr = BucketState.getBucketMaps(aRegion);
   Map primaryMap = mapArr[0]; 
   Map bucketMap = mapArr[1]; 
   Log.getLogWriter().info("Before rebalance, buckets: " + bucketMap + ", primaries: " + primaryMap);
   if (bucketMap.size() != numDataStoresBefore) {
      throw new TestException("Expected " + numDataStoresBefore + " vms to contain buckets, but " +
                bucketMap.size() + " vms contains buckets, bucketMap: " + bucketMap + ", primaryMap: " +
                primaryMap);
   }
   ParRegUtil.doRebalance();

   // verify after rebalance
   mapArr = BucketState.getBucketMaps(aRegion);
   primaryMap = mapArr[0]; 
   bucketMap = mapArr[1]; 
   Object value = primaryMap.get(myVmId);
   if (value == null) {
      primaryMap.put(myVmId, new Integer(0));
   }
   value = bucketMap.get(myVmId);
   if (value == null) {
      bucketMap.put(myVmId, new Integer(0));
   }
   Log.getLogWriter().info("After rebalance, buckets: " + bucketMap + ", primaries: " + primaryMap);
   if (((value == null) || ((Integer)value).intValue() == 0)) {
      throw new TestException("New capacity vm did not gain any buckets, bucketMap " + bucketMap +
                              ", primaryMap " + primaryMap);
   }
   if (bucketMap.size() != numDataStores) {
      throw new TestException("Expected " + numDataStores + " vms to contain buckets, but " +
                bucketMap.size() + " vms contains buckets, bucketMap: " + bucketMap + ", primaryMap: " +
                primaryMap);
   }
   verifyRegionSize();
}

/** Verify the region size.
 */
protected void verifyRegionSize() {
   long maxKey = NameFactory.getPositiveNameCounter();
   Log.getLogWriter().info("Verifying region, expecting " + maxKey + " entries...");
   int size = aRegion.size();
   if (size != maxKey) {
      throw new TestException("Expected region size to be " + maxKey + " but it is " + size + "\n");
   }
   Log.getLogWriter().info("Region size is " + size + " as expected");
}

/** Write the current state of buckets to the blackboard, then do
 *  a stop/start.
 */
protected void stopStart() {
   // write current state to blackboard
   Map[] mapArr = BucketState.getBucketMaps(aRegion);
   Map primaryMap = mapArr[0]; 
   Map bucketMap = mapArr[1]; 
   Log.getLogWriter().info("Before stopping, writing current state to blackboard, bucketMap: " +
       bucketMap + ", primaryMap: " + primaryMap);
   RebalanceBB.getBB().getSharedMap().put(bucketMapKey, bucketMap);
   RebalanceBB.getBB().getSharedMap().put(primaryMapKey, primaryMap);

   // stop and start vms; validation occurs in the init task of the restarted vm
   // which will be completed by the time the stopStartVMs call finishes
   Object[] tmpArr = StopStartVMs.getOtherVMs(1, "dataStore");
   List targetVM = (List)(tmpArr[0]); 
   List stopMode = (List)(tmpArr[1]); 
   StopStartVMs.stopStartVMs(targetVM, stopMode);
}

/** Verify that this restarted vm roughly replaced the capacity it had before.
 */
protected void verifyReplacedCapacity() {
   // wait for recovery to complete in this vm
   PRObserver.waitForRebalRecov(RemoteTestModule.getMyVmid(), 1, 1, null, null, false); 

   // rebalance
   ParRegUtil.doRebalance();

   // see if this vm gained back what it had before it was stopped
   Map[] mapArr = BucketState.getBucketMaps(aRegion);
   Map<Integer, Integer> primaryMapNow = mapArr[0]; 
   Map<Integer, Integer> bucketMapNow = mapArr[1]; 
   Map<Integer, Integer> bucketMapBefore = (Map)(RebalanceBB.getBB().getSharedMap().get(bucketMapKey));
   Map<Integer, Integer> primaryMapBefore = (Map)(RebalanceBB.getBB().getSharedMap().get(primaryMapKey));
   Integer myVmId = RemoteTestModule.getMyVmid();
   int numBucketsBefore = bucketMapBefore.containsKey(myVmId) ? 
                              (bucketMapBefore.get(myVmId)) : 
                              (bucketMapBefore.put(myVmId, new Integer(0)));
   int numBucketsNow = bucketMapNow.containsKey(myVmId) ? 
                              (bucketMapNow.get(myVmId)) : 
                              (bucketMapNow.put(myVmId, new Integer(0)));
   int numPrimariesBefore = primaryMapBefore.containsKey(myVmId) ? 
                              (primaryMapBefore.get(myVmId)) : 
                              (primaryMapBefore.put(myVmId, new Integer(0)));
   int numPrimariesNow = primaryMapNow.containsKey(myVmId) ? 
                              (primaryMapNow.get(myVmId)) : 
                              (primaryMapNow.put(myVmId, new Integer(0)));
   Log.getLogWriter().info("Before stopping/restarting this vm, bucketMap: " + bucketMapBefore +
       ", primaryMap: " + primaryMapBefore + 
       ", after restarting/rebalancing, bucketMap: " + bucketMapNow + ", primaryMap: " + primaryMapNow);
   int diff = Math.abs(numBucketsNow - numBucketsBefore);
   if (diff > 1) {
      throw new TestException("Number of buckets in this vm before stopping/restarting was " + 
            numBucketsBefore + ", but number of buckets in this vm after restarting/rebalancing is " + 
            numBucketsNow);
   }
   diff = Math.abs(numPrimariesNow - numPrimariesBefore);
   if (diff > 1) {
      throw new TestException("Number of primaries in this vm before stopping/restarting was " + 
            numPrimariesBefore + ", but number of primaries in this vm after restarting/rebalancing is " + 
            numPrimariesNow);
   }
}

/** Return a Map where the key is a vmId, and the value is that vm's
 *  localMaxMemory setting
 */
protected Map<Integer, Integer>  getLocalMaxMemoryMap() {
   Map<Integer, Integer> lmmMap = new TreeMap();
   Map sharedMap = RebalanceBB.getBB().getSharedMap().getMap(); Iterator it = sharedMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      if ((key instanceof String) && ((String)key).startsWith(RebalanceBB.LocalMaxMemoryKey)) {
         String keyStr = (String)key;
         Integer vmId = Integer.valueOf(keyStr.substring(RebalanceBB.LocalMaxMemoryKey.length(), keyStr.length()));
         lmmMap.put(vmId, (Integer)(sharedMap.get(key)));
      }
   }
   return lmmMap;
}

/** Return a Map where the key is a vmId, and the value is that vm's
 *  number of entries (its local size).
 */
protected Map<Integer, Long>  getLocalSizeMap() {
   Map<Integer, Long> localSizeMap = new TreeMap();
   Map sharedMap = RebalanceBB.getBB().getSharedMap().getMap();
   Iterator it = sharedMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      if ((key instanceof String) && ((String)key).startsWith(RebalanceBB.LocalSizeKey)) {
         String keyStr = (String)key;
         Integer vmId = Integer.valueOf(keyStr.substring(RebalanceBB.LocalSizeKey.length(), keyStr.length()));
         localSizeMap.put(vmId, (Long)(sharedMap.get(key)));
      }
   }
   return localSizeMap;
}

/** Verify the relative sizes of the data and local max memory in each vm relative
 *  to every other vm.
 */
protected void verifyLmmBasedOnRelativeSize() {
   long counter = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.numExceededLMMAlerts);
   if (counter > 0) {
      throw new TestException("Test config error; expect no alerts for exceeding local max memory, but received " + counter);
   }

   Map<Integer, Integer> lmmMap = getLocalMaxMemoryMap();
   Map<Integer, Long> localSizeMap = getLocalSizeMap();

   // for test sanity; make sure the maps have the same keys
   if (lmmMap.size() != localSizeMap.size()) {
      throw new TestException("Test error; lmmMap and localSizeMap are not the same size, lmmMap " +
                lmmMap + ", localSizeMap " + localSizeMap);
   }
   if (!(lmmMap.keySet().equals(localSizeMap.keySet()))) {
      throw new TestException("Test error; lmmMap and localSizeMap do not contain the same keys, " +
                " lmmMap: " + lmmMap + ", localSizeMap: " + localSizeMap);
   }
   Log.getLogWriter().info("localMaxMemoryMap: " + lmmMap + ", localSizeMap: " + localSizeMap);

   // now verify; we have the same keys in each map
   StringBuffer aStr = new StringBuffer();
   StringBuffer errStr = new StringBuffer();
   Iterator lmmIt = lmmMap.keySet().iterator();
   while (lmmIt.hasNext()) {
      int lmmVmId = ((Integer)lmmIt.next()).intValue();
      Iterator localSizeIt = localSizeMap.keySet().iterator();
      while (localSizeIt.hasNext()) {
         int localSizeVmId = ((Integer)localSizeIt.next()).intValue();
         if (localSizeVmId != lmmVmId) { // don't compare a vm to itself
            int lmm1 = lmmMap.get(lmmVmId);
            int lmm2 = lmmMap.get(localSizeVmId);
            if (lmm1 < lmm2) { // don't compare reversals (ie vm1 to vm2 id ok, but dont also do vm2 to vm1)
               long numEntries1 = localSizeMap.get(lmmVmId);
               long numEntries2 = localSizeMap.get(localSizeVmId); 
               float lmmFactor = (float)lmm2/(float)lmm1;
               float localSizeFactor = (float)numEntries2/(float)numEntries1;
               // numEntries2Ideal is the ideal number of entries for numEntries2 given
               // the value of lmm1, lmm2 and numEntries2
               float numEntries2Ideal = ((float)lmm2 * (float)numEntries1) / (float)lmm1;
               float idealPercent = ((float)numEntries2 / (float)numEntries2Ideal) * 100;
               String logStr = "\nVmId " + lmmVmId + " has localMaxMemory " + lmm1 + 
                  " and local num entries " + numEntries1 + "\n" +
                  "VmId " + localSizeVmId + " has localMaxMemory " + lmm2 + 
                  " and local num entries " + numEntries2 + "\n" +
                  "   vmId " + localSizeVmId + " localMaxMemory " + lmm2 + " is " + lmmFactor + 
                     " times bigger than vmId " + lmmVmId + " with localMaxMemory " 
                     + lmm1 + "\n" +
                  "   vmId " + localSizeVmId + " local size " + numEntries2 + " is " + localSizeFactor + 
                     " times the local size of vmId " + lmmVmId + " with num entries " 
                     + numEntries1 + "\n" +
                     "   ideal local size of vmId " + localSizeVmId + " is " + numEntries2Ideal + "\n" +
                     "   actual local size " + numEntries2 + " is " + idealPercent + 
                         " percent of ideal local size " + numEntries2Ideal + "\n";
               if ((idealPercent < 90) || (idealPercent > 110)) {
                  String aString = "VmId " + localSizeVmId + " is too far from the ideal number of entries; " + logStr;
                  Log.getLogWriter().info(aString);
                  errStr.append(aString);
               }
               aStr.append(logStr); 
            }
         }
      }
   }
   Log.getLogWriter().info(aStr.toString());
   if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
   }
}

/** Verify the actual number of entries in each vm is close to the expected
 *  ideal number of entries.
 */
protected void verifyLmmBasedOnIdealSize() {
   long counter = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.numExceededLMMAlerts);
   if (counter > 0) {
      throw new TestException("Test config error; expect no alerts for exceeding local max memory, but received " + counter);
   }
   Map<Integer, Integer> lmmMap = getLocalMaxMemoryMap();
   Map<Integer, Long> localSizeMap = getLocalSizeMap();
   Map<Integer, Integer> idealLocalSizeMap = getIdealLocalSizeMap();
   Log.getLogWriter().info("localMaxMemoryMap: " + lmmMap);
   Log.getLogWriter().info("localSizeMap: " + localSizeMap);
   Log.getLogWriter().info("idealLocalSizeMap: " + idealLocalSizeMap);

   // for test sanity; make sure the maps have the same keys
   if (lmmMap.size() != localSizeMap.size() || lmmMap.size() != idealLocalSizeMap.size()) {
      throw new TestException("Test error; lmmMap, localSizeMap, idealLocalSizeMap are not the same size, " +
            " lmmMap " + lmmMap + ", localSizeMap " + localSizeMap + ", idealLocalSizeMap " + idealLocalSizeMap);
   }
   if (!(lmmMap.keySet().equals(localSizeMap.keySet()))) {
      throw new TestException("Test error; lmmMap and localSizeMap do not contain the same keys, " +
                " lmmMap: " + lmmMap + ", localSizeMap: " + localSizeMap);
   }
   int sum = 0;
   for (Integer aValue: idealLocalSizeMap.values()) {
      sum+=aValue;
   } 
   int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
   Log.getLogWriter().info("sum of idealLocalSizeMap values is " + sum + ", region size is " + aRegion.size() +
       " redundantCopies is " + redundantCopies + ", total size of all copies " + 
       (aRegion.size() * (redundantCopies + 1)));

   // now verify
   StringBuffer aStr = new StringBuffer();
   StringBuffer errStr = new StringBuffer();
   for (Integer vmId: localSizeMap.keySet()) {
      Long actualLocalSize = localSizeMap.get(vmId);
      Integer idealLocalSize = idealLocalSizeMap.get(vmId);
      float idealPercent = ((float)actualLocalSize / (float)idealLocalSize) * 100;
      String aString = "For vmId " + vmId + " with localMaxMemory " + lmmMap.get(vmId) +
                           " actual local size is " + actualLocalSize + "\n" +
                       "   ideal local size is " + idealLocalSize + "\n" +
                       "   actual local size " + actualLocalSize + " is " + idealPercent + 
                       " percent of ideal local size " + idealLocalSize + "\n";
      if ((idealPercent < 90) || (idealPercent > 110)) {
         Log.getLogWriter().info("VmId " + vmId + " is too far from the ideal number of entries: " + aString);
         errStr.append(aString);
      }
      aStr.append(aString);
   }
   Log.getLogWriter().info(aStr.toString());
   if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
   }
}

/** Return a Map containing the ideal number of entries for a given
 *  vm's local max memory and the total number of keys currently in
 *  aRegion.
 *
 *  This calculation is based on the following:
 *  Let x = the number of keys in vm with localMaxMemory baseLmm. 
 *  Then the number of keys in all vms is represented by this equation 
 *  (one term for each vmId present in the system):
 *  (lmmForVmId_1/baseLmm)*x + (lmmForVmId_2/baseLmm)*x + 
 *           ...<one term for each vmId> = numKeysInRegion + numKeysInRedundantCopies
 *
 *  @returns A Map where keys are vmIds and values are the number
 *           of entries expected in that vm given the vm's localMaxMemory
 *           setting.
 */
protected Map<Integer, Integer> getIdealLocalSizeMap() {
   int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
   int totalCopies = redundantCopies + 1;
   Map<Integer, Integer> lmmMap = new TreeMap(getLocalMaxMemoryMap());
   Map<Integer, Long> localSizeMap = new TreeMap(getLocalSizeMap());
   float baseLmm = (float)(lmmMap.values().iterator().next());
   float total = (float)0;
   for (Integer lmm: lmmMap.values()) {
       total += (float)lmm / baseLmm; 
   }
   float x = (float)(aRegion.size() * totalCopies) / total;

   Map<Integer, Integer> resultMap = new TreeMap();
   for (Integer vmId: lmmMap.keySet()) {
      Integer lmmValue = lmmMap.get(vmId);
      int numKeysForThisLMM = (int)(((float)lmmValue / baseLmm) * x);
      resultMap.put(vmId, new Integer(numKeysForThisLMM));
   }
   return resultMap;
}

}
