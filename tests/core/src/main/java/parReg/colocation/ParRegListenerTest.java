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
package parReg.colocation;

import getInitialImage.InitImageBB;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.ParRegUtil;
import parReg.execute.ColocatingPartitionListener;
import util.NameFactory;
import util.PRObserver;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ParRegListenerTest extends ParRegColocation {

  ArrayList serverRegionDescriptions = new ArrayList();

  ArrayList clientRegionDescriptions = new ArrayList();

  Cache theCache;

  public static final int NUM_KEYS_TO_CREATE = 1000;

  private static ArrayList primaryBucketsInTheDataStore;

  private static String primaryBucketReferenceRegion;

  private static String bucketReferenceRegion;

  static protected ParRegListenerTest testInstance;

  /**
   * Hydra task for initializing a data store
   */
  public synchronized static void HydraTask_HA_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ParRegListenerTest();
      testInstance.initializeServerRegions();
      ParRegBB.getBB().getSharedMap().put(
          DataStoreVmStr + RemoteTestModule.getMyVmid(),
          new Integer(RemoteTestModule.getMyVmid()));
      BridgeHelper.startBridgeServer("bridge");
    }
  }

  public synchronized static void HydraTask_HA_accessorInitialize() {
    if (testInstance == null) {
      testInstance = new ParRegListenerTest();
      testInstance.initializeClientRegions();
    }
  }

  public synchronized static void HydraTask_registerInterest() {
    testInstance.registerInterest();
  }

  public synchronized static void HydraTask_loadRegions() {
    testInstance.loadRegions();
  }

  public synchronized static void HydraTask_logRegionsSize() {
    testInstance.logRegionsSize();
  }

  public static void HydraTask_verifyListenerInvocation() {
    testInstance.verifyListenerInvocation();
    InitImageBB.getBB().printSharedMap();
  }

  public static void HydraTask_verifyPrimaryCoLocation() {
    testInstance.verifyPrimaryBucketCoLocation();
  }
  
  public static void HydraTask_verifyEmptyRecreatedBuckets() {
    testInstance.verifyEmptyRecreatedBuckets();
  }
  
  public static void HydraTask_disconnect() {
    DistributedSystemHelper.disconnect();
    testInstance = null;
  }
  
  public void verifyEmptyRecreatedBuckets() {
    List regionList = (serverRegionDescriptions.size() != 0) ? serverRegionDescriptions
        : clientRegionDescriptions;
    Set recreatedBucketIdsSet = new HashSet();

    for (int i = 0; i < regionList.size(); i++) {
      String regionDescriptName = (String)(regionList.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      if ((aRegion.getPartitionListeners() != null)
          && aRegion.getPartitionListeners().length > 0) {
        ColocatingPartitionListener partitionListener = (ColocatingPartitionListener)aRegion
            .getPartitionListeners()[0];
        recreatedBucketIdsSet = partitionListener.getReCreatedBucketIds();
      }
      if (recreatedBucketIdsSet.size() > 0)
        break;
    }

    for (int j = 0; j < regionList.size(); j++) {
      String regionDescriptName = (String)(regionList.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      // if (!aRegion.getName().contains("testRegion4")){
      if ((aRegion.getColocatedWith() == null)
          && (aRegion.getPartitionListeners() == null || aRegion
              .getPartitionListeners().length == 0)) {
        hydra.Log.getLogWriter().info(
            "Verifying empty recreated for the region " + aRegion.getName()
                + " because the region does not has PR listener");
        verifyEmptyRecreatedBuckets(aRegion, recreatedBucketIdsSet);
      }
      // }
    }
  }
  
  public void verifyEmptyRecreatedBuckets(PartitionedRegion aRegion,
      Set<Integer> recreatedBucketIdsSet) {
    for (Integer bucketId : recreatedBucketIdsSet) {
      Set keySetOfBucket;
      try {
        keySetOfBucket = aRegion.getBucketKeys(bucketId);
      }
      catch (Exception e) {
        throw new TestException("Caught the exception during getBucketKeys ", e);
      }
      if (keySetOfBucket.size() != 0) {
        throw new TestException("Expected the bucket " + bucketId
            + " (which is recreated) to be empty but has the size of "
            + keySetOfBucket.size());
      }
    }
  }

  public void verifyListenerInvocation() {
    List regionList = (serverRegionDescriptions.size() != 0) ? serverRegionDescriptions
        : clientRegionDescriptions;

    for (int j = 0; j < regionList.size(); j++) {
      String regionDescriptName = (String)(regionList.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      // if (!aRegion.getName().contains("testRegion4")){
      if (aRegion.getPartitionListeners() != null) {
        hydra.Log.getLogWriter().info(
            "Verifying the listener invocation for the region "
                + aRegion.getName() + " because the region has PR listener");
        verifyListenerInvocation(aRegion);
      }
      // }
    }
  }

  public void verifyListenerInvocation(PartitionedRegion aRegion) {
    ArrayList primaryBucketList = (ArrayList)aRegion
        .getLocalPrimaryBucketsListTestOnly();
    InternalDistributedMember self = (InternalDistributedMember)theCache
        .getDistributedSystem().getDistributedMember();
    for (Object bucketId : primaryBucketList) {
      String listenerInvokedNode = (String)parReg.ParRegBB.getBB()
          .getSharedMap().get("Bucket_" + ((Integer)bucketId));
      if (!self.getId().equals(listenerInvokedNode)) {
        throw new TestException("This node has the primary bucket " + bucketId
            + " but latest PR listener invocation happened on node "
            + listenerInvokedNode);
      }
    }
  }

  public void verifyPrimaryBucketCoLocation() {
    List regionList = (serverRegionDescriptions.size() != 0) ? serverRegionDescriptions
        : clientRegionDescriptions;

    for (int j = 0; j < regionList.size(); j++) {
      String regionDescriptName = (String)(regionList.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      Log.getLogWriter().info(
          "Primary buckets here are for the region " + regionName + " is "
              + aRegion.getLocalPrimaryBucketsListTestOnly());
      // if (!aRegion.getName().contains("testRegion4")){
      verifyPrimaryBucketColocation(aRegion);
      // }
    }
  }

  protected static void verifyPrimaryBucketColocation(PartitionedRegion aRegion) {

    String regionName = aRegion.getName();
    ArrayList primaryBucketList = (ArrayList)aRegion
        .getLocalPrimaryBucketsListTestOnly();

    if (primaryBucketList == null) {
      if (((PartitionedRegion)aRegion).getLocalMaxMemory() == 0) {
        Log.getLogWriter().info(
            "This is an accessor and no need to verify colocation");
        return;
      }
      else {
        throw new TestException(
            "Bucket List returned null, but it is not an accessor");
      }
    }

    Log.getLogWriter().info(
        "Primary Buckets of " + aRegion.getName() + " "
            + primaryBucketList.toString());

    if (primaryBucketsInTheDataStore == null) {
      Log
          .getLogWriter()
          .info(
              " Setting the reference primary buckets in the Data Store for this vm with the Partitioned Region "
                  + aRegion.getName());
      primaryBucketsInTheDataStore = primaryBucketList;
      primaryBucketReferenceRegion = regionName;
    }
    else {
      Log.getLogWriter().info(
          "Reference primary buckets in the Data Store for this vm already set and is "
              + primaryBucketsInTheDataStore);
      Log.getLogWriter().info(
          " Verifying for the region " + regionName
              + " which has the buckets on this node "
              + aRegion.getLocalBucketsListTestOnly());

      Iterator iterator = primaryBucketList.iterator();

      while (iterator.hasNext()) {
        Integer currentPrimaryBucket = (Integer)iterator.next();
        if (primaryBucketsInTheDataStore.contains(currentPrimaryBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + primaryBucketReferenceRegion + " and "
                  + regionName + " have the bucket " + currentPrimaryBucket
                  + " in this node");
        }
        else {
          throw new TestException("Region " + regionName
              + " does not have its bucket " + currentPrimaryBucket
              + " colocated");
        }
      }

      Log.getLogWriter().info("Looking for missed buckets");

      iterator = primaryBucketsInTheDataStore.iterator();

      while (iterator.hasNext()) {
        Integer referenceRegionBucket = (Integer)iterator.next();
        if (primaryBucketList.contains(referenceRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + primaryBucketReferenceRegion + " and "
                  + regionName + " have the primary bucket "
                  + referenceRegionBucket + " in this node");
        }
        else {
          throw new TestException("Region " + regionName
              + " does not have its primary bucket " + referenceRegionBucket
              + " colocated");
        }
      }
    }

  }

  public void logRegionsSize() {
    List regionList = (serverRegionDescriptions.size() != 0) ? serverRegionDescriptions
        : clientRegionDescriptions;

    for (int j = 0; j < regionList.size(); j++) {
      String regionDescriptName = (String)(regionList.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      hydra.Log.getLogWriter().info(
          "Region size for the region " + aRegion.getName() + " is "
              + aRegion.size());
    }
  }

  public void loadRegions() {
    List regionList = (serverRegionDescriptions.size() != 0) ? serverRegionDescriptions
        : clientRegionDescriptions;
    loadRegions(regionList);
  }

  public void loadRegions(List regionList) {
    long createdKeysCount = InitImageBB.getBB().getSharedCounters().read(
        InitImageBB.NEW_KEY_COMPLETED);
    ;
    while (createdKeysCount <= NUM_KEYS_TO_CREATE) {
      createdKeysCount = InitImageBB.getBB().getSharedCounters()
          .incrementAndRead(InitImageBB.NEW_KEY_COMPLETED);
      // Object key = NameFactory.getNextPositiveObjectName();
      Object key = new Long(createdKeysCount);
      for (int j = 0; j < regionList.size(); j++) {
        String regionDescriptName = (String)(regionList.get(j));
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region aRegion = theCache.getRegion(regionName);
        RegionAttributes regionAttributes = aRegion.getAttributes();
        if (regionAttributes.getCacheLoader() == null) {
          Object value = new Long(createdKeysCount);
          aRegion.put(key, value);
        }
        else {
          // hydra.Log.getLogWriter().info(
          // "Calling get as this region " + aRegion.getName()
          // + " has a cache loader");
          // Object value = aRegion.get(key);
          // if (value == null) {
          // throw new TestException(
          // "Value cannot be null after cache loader invocation");
          // }
        }
      }
    }
    ;
  }

  void registerInterest() {
    for (int j = 0; j < clientRegionDescriptions.size(); j++) {
      String regionDescriptName = (String)(clientRegionDescriptions.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      ParRegUtil.registerInterest(aRegion);
      Log.getLogWriter().info(
          "registered interest for the region " + regionName);
    }
  }

  void initializeClientRegions() {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (Object regionName : regionNames) {
      String thisRegionName = (String)regionName;
      if (thisRegionName.startsWith("client")) {
        clientRegionDescriptions.add(thisRegionName);
      }
    }
    theCache = CacheHelper.createCache("cache1");

    PoolHelper.createPool("edgeDescript");
    createRegions(clientRegionDescriptions);
  }

  void initializeServerRegions() {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (Object regionName : regionNames) {
      String thisRegionName = (String)regionName;
      if (thisRegionName.startsWith("bridge")) {
        serverRegionDescriptions.add(thisRegionName);
      }
    }
    theCache = CacheHelper.createCache("cache1");
    createRegions(serverRegionDescriptions);
  }

  void createRegions(List regionDescriptionsList) {
    for (int j = 0; j < regionDescriptionsList.size(); j++) {
      String regionDescriptName = (String)(regionDescriptionsList.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      RegionHelper.createRegion(regionName, RegionHelper
          .getRegionAttributes(regionDescriptName));
      Log.getLogWriter().info(
          "Created partitioned region " + regionName
              + " with region descript name " + regionDescriptName);
    }
  }

  /**
   * Hydra task to stop/start numVmsToStop at a time.
   */
  public static void HydraTask_stopStartVms() {
    long minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec
        * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    do {
      (testInstance).stopStartVms();
      long timeToStop = ParRegBB.getBB().getSharedCounters().read(
          ParRegBB.TimeToStop);
      if (timeToStop > 0) { // ops have ended
        throw new StopSchedulingTaskOnClientOrder("Ops have completed");
      }
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }

  protected void stopStartVms() {
    int numRegions = CacheHelper.getCache().rootRegions().size();
    PRObserver.initialize();
    int numVMsToStop = TestConfig.tab().intAt(ParRegPrms.numVMsToStop);
    Log.getLogWriter().info(
        "In stopStartVms, choosing " + numVMsToStop + " vm(s) to stop...");
    List vmList = getDataStoreVms();
    List targetVms = new ArrayList();
    List stopModes = new ArrayList();
    for (int i = 1; i <= numVMsToStop; i++) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size() - 1);
      targetVms.add(vmList.get(randInt));
      vmList.remove(randInt);
      stopModes.add(TestConfig.tab().stringAt(ParRegPrms.stopModes));
    }
    StopStartVMs.stopStartVMs(targetVms, stopModes);
    PRObserver.waitForRebalRecov(targetVms, 1, numRegions-1, null, null, false);
    Log.getLogWriter().info("Done in stopStartVms()");
  }
}
