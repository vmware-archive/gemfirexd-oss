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
package parReg.fixedPartitioning;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.FixedPartitionBlackboard;
import hydra.Log;
import hydra.PartitionPrms;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.ParRegBB;
import parReg.ParRegTest;
import parReg.ParRegUtil;
import parReg.colocation.Month;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import util.TxHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;

public class FPRTxParRegTest extends ParRegTest {

  public final String VM_PARTITIONS = "vm_partitions_";

  public final static String DATASTORE_SEQUENCE_ID = "dataStore_sequence_id_";

  protected static Long dataStoreSequenceId;

  protected static Cache theCache;

  // ------------------- STARTTASKS -----------------------------------------//
  public static void StartTask_initialize() {
    
    // Update the BB with the partitions
    List<FixedPartitionAttributes> primaryPartitions = TestConfig.tab().vecAt(
        hydra.FixedPartitionPrms.partitionNames);

    int redundantCopies = TestConfig.tab().intAt(
        PartitionPrms.redundantCopies);
    List<FixedPartitionAttributes> secondaryPartitions = new ArrayList<FixedPartitionAttributes>();
    for (int i = 0; i < redundantCopies; i++) {
      secondaryPartitions.addAll(primaryPartitions);
    }

    hydra.Log.getLogWriter().info("RedundantCopies is " + redundantCopies);
    hydra.Log.getLogWriter().info("PrimaryPartitions are " + primaryPartitions);
    hydra.Log.getLogWriter().info(
        "Secondary partitions are " + secondaryPartitions);

    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "PrimaryPartitions", primaryPartitions);
    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "SecondaryPartitions", secondaryPartitions);
  }
  // ------------------ INITTASKS -------------------------------------------//
  
  /** Creates and initializes the singleton instance of ParRegTest in this VM.
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FPRTxParRegTest();
      FPRExpirationTest.setDataStoreSequenceId();
      ((FPRTxParRegTest)testInstance).initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        testInstance.isDataStore = false;
        ParRegUtil.registerInterest(testInstance.aRegion);
      }
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing with a PR data store.
   */
  public synchronized static void HydraTask_HA_initializeDataStore() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FPRTxParRegTest();
      FPRExpirationTest.setDataStoreSequenceId();
      ((FPRTxParRegTest)testInstance).initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      testInstance.isDataStore = true;
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing with a PR accessor.
   */
  public synchronized static void HydraTask_HA_initializeAccessor() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FPRTxParRegTest();
      ((FPRTxParRegTest)testInstance).initializeRegion("accessorRegion");
      testInstance.initializeInstance();
      testInstance.isDataStore = false;
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        ParRegUtil.registerInterest(testInstance.aRegion);
      }
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }
  
  /**
   * Creates and initializes the singleton instance of ParRegTest in this VM for
   * HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeAccessor() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      testInstance = new FPRTxParRegTest();
      testInstance.HA_reinitializeRegion();
      testInstance.initializeInstance();
      testInstance.isDataStore = false;
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }

  /**
   * Creates and initializes the singleton instance of ParRegTest in this VM for
   * HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeDataStore() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      testInstance = new FPRTxParRegTest();
      testInstance.HA_reinitializeRegion();
      testInstance.initializeInstance();
      testInstance.isDataStore = true;
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }
      
  /**
   * Create a region with the given region description name.
   * 
   * @param regDescriptName
   *                The name of a region description.
   */
  public void initializeRegion(String regDescriptName) {
    String key = VmIDStr + RemoteTestModule.getMyVmid();
    if (theCache == null || CacheHelper.getCache() == null) {
      theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
    }
    String regionName = RegionHelper.getRegionDescription(regDescriptName)
        .getRegionName();
    
    aRegion = theCache.getRegion(regionName);

    if (aRegion != null) {
      hydra.Log.getLogWriter().info(
          "Region " + aRegion.getName() + " already exists");
      return;
    }
    aRegion = RegionHelper.createRegion(regionName, regDescriptName);
    hydra.Log.getLogWriter().info(
        "Created region " + aRegion.getName() + " with attributes "
            + aRegion.getAttributes());

    String cacheXmlFile = key + ".xml";
    File aFile = new File(cacheXmlFile);
    if (!aFile.exists()) {
      createXmlFile(aFile);
    }
    ParRegBB.getBB().getSharedMap().put(key, regDescriptName);
  }
  
  /**
   * Method to create the xml from the cache.
   */
  public void createXmlFile(File aFile) {
    if (!aFile.exists()) {
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(aFile), true);
        CacheXmlGenerator.generate(theCache, pw);
        pw.close();

      }
      catch (IOException ex) {
        throw new TestException("IOException during cache.xml generation to "
            + aFile + " : ", ex);
      }
    }
  }

  /**
   * Re-Initialize a VM which has restarted by creating the appropriate region.
   */
  public String HA_reinitializeRegion(String cachePrmsName) {
    String key = VmIDStr + RemoteTestModule.getMyVmid();
    String regDescriptName = (String)(ParRegBB.getBB().getSharedMap().get(key));
    reInitializeRegion(regDescriptName);
    Log.getLogWriter().info(
        "After recreating " + aRegion.getFullPath() + ", size is "
            + aRegion.size());

    // re-initialize the TransactionManager (for the newly created cache)
    if (getInitialImage.InitImagePrms.useTransactions()) {
      TxHelper.setTransactionManager();
    }

    // init the lockService
    if (lockOperations) {
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      try {
        distLockService = DistributedLockService.create(LOCK_SERVICE_NAME,
            DistributedSystemHelper.getDistributedSystem());
        Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
      }
      catch (IllegalArgumentException e) {
        // this can happen if we are reinitializing because we closed the cache
        String exceptStr = e.toString();
        if (exceptStr.indexOf("Service named " + LOCK_SERVICE_NAME
            + " already created") >= 0) {
          Log.getLogWriter().info(
              "Caught " + e
                  + "; continuing test because lock service already exists");
        }
        else {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }
    }
    return regDescriptName;
  }

  public void reInitializeRegion(String regDescriptName) {
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    String key = VmIDStr + RemoteTestModule.getMyVmid();
    String cacheXmlFile = key + ".xml";
    File aFile = new File(cacheXmlFile);

    if ((ds != null) && ds.isConnected()) {
      initializeRegion(regDescriptName);
    }
    else {
      createCacheFromXml(cacheXmlFile);
    }
  }

  /**
   * Create cache from the xml file
   */
  public void createCacheFromXml(String cacheXmlFile) {
    hydra.Log.getLogWriter().info(
        "Creating cache using the xml file " + cacheXmlFile);
    theCache = CacheHelper.createCacheFromXml(cacheXmlFile);
    aRegion = theCache.getRegion(Region.SEPARATOR + "partitionedRegion");
  }

  /**
   * Task to verify the fixed partitioning
   */
  public static void HydraTask_verifyFixedPartitioning() {
    ((FPRTxParRegTest)testInstance).verifyPrimaryFixedPartitioning();
    ((FPRTxParRegTest)testInstance).verifySecondaryFixedPartitioning();

  }

  /**
   * Method to verify the fixed partitioning of the primary partitions
   * 
   * @param pr
   */
  protected void verifyPrimaryFixedPartitioning() {
    PartitionedRegion pr = (PartitionedRegion)aRegion;
    List<Integer> primaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    hydra.Log.getLogWriter().info(
        "Primary buckets for the partitioned region " + aRegion.getName()
            + " is " + primaryBucketList);
    Map<String, Set<Integer>> partition2Buckets = new HashMap<String, Set<Integer>>();

    // Return if there are no buckets on this member
    if (primaryBucketList == null || primaryBucketList.size() == 0) {
      hydra.Log.getLogWriter().info(
          "There are no primary buckets on this member");
      return;
    }

    List<FixedPartitionAttributesImpl> primaryPartitionsForPr = pr
        .getPrimaryFixedPartitionAttributes_TestsOnly();
    List<String> primaryPartitions = getPartitionNames(primaryPartitionsForPr);

    /*
     * The following get the entries from each primary bucket on the member. It
     * then verifies that the PartitionName for the entries matches with the
     * Primary partition name of the member for the region.
     */
    for (Integer bucketId : primaryBucketList) {
      Set keySet = pr.getBucketKeys(bucketId);
      Month routingObject = null;
      for (Object key : keySet) {
        routingObject = (Month)ParRegBB.getBB().getSharedMap().get(key);
        String partitionName = routingObject.getQuarter();
        if (!primaryPartitions.contains(partitionName)) {
          throw new TestException(" For the region " + pr.getName()
              + " for the key ," + key + " in the primary bucket " + bucketId
              + " the getPartition returned " + partitionName
              + " but the primary partitions on the node is "
              + primaryPartitions);
        }
        else {
          hydra.Log.getLogWriter().info(
              " For the region " + pr.getName() + " for the key ," + key
                  + " in the primary bucket " + bucketId
                  + " the getPartition returned " + partitionName
                  + " and the primary partitions on the node is "
                  + primaryPartitions);
        }

        /*
         * The following creates a set of bucket ids for each partition name
         * This will be later used for validating that the partition num buckets
         * is valid and each primary partition has the required partition num
         * buckets
         */
        if (partition2Buckets.get(partitionName) == null) {
          Set<Integer> bucketIdSet = new HashSet<Integer>();
          bucketIdSet.add(bucketId);
          partition2Buckets.put(partitionName, bucketIdSet);
        }
        else {
          Set<Integer> bucketIdSet = (Set<Integer>)partition2Buckets
              .get(partitionName);
          if (!bucketIdSet.contains(bucketId)) {
            bucketIdSet.add(bucketId);
          }
        }
      }
    }

    // The following verification is not required and may fail.
    // verifyPartitionNumBuckets(pr, primaryPartitionsForPr, partition2Buckets);
  }

  /**
   * Method to verify the fixed partitioning of the secondary partitions
   * 
   * @param pr
   */
  protected void verifySecondaryFixedPartitioning() {
    PartitionedRegion pr = (PartitionedRegion)aRegion;
    if (pr.getLocalBucketsListTestOnly() == null
        || pr.getLocalBucketsListTestOnly().size() == 0) {
      hydra.Log.getLogWriter().info("There are no buckets on this member");
      return;
    }

    hydra.Log.getLogWriter().info(
        "All local buckets for the partitioned region " + aRegion.getName()
            + " is " + pr.getLocalBucketsListTestOnly());

    Set<Integer> bucketSet = new HashSet(pr.getLocalBucketsListTestOnly());
    bucketSet.removeAll(pr.getLocalPrimaryBucketsListTestOnly());

    hydra.Log.getLogWriter().info(
        "Secondary buckets for the partitioned region " + pr.getName()
            + " on this member are " + bucketSet);

    Map<String, Set<Integer>> partition2Buckets = new HashMap<String, Set<Integer>>();

    List<FixedPartitionAttributesImpl> secondaryPartitionsForPr = pr
        .getSecondaryFixedPartitionAttributes_TestsOnly();
    List<String> secondaryPartitions = getPartitionNames(secondaryPartitionsForPr);

    /*
     * The following get the entries from each secondary bucket on the member.
     * It then verifies that the PartitionName for the entries matches with the
     * secondary partition name of the member for the region.
     */
    for (Integer bucketId : bucketSet) {
      Set keySet = pr.getBucketKeys(bucketId);
      Month routingObject = null;
      for (Object key : keySet) {
        routingObject = (Month)ParRegBB.getBB().getSharedMap().get(key);
        String partitionName = routingObject.getQuarter();
        if (!secondaryPartitions.contains(partitionName)) {
          throw new TestException(" For the region " + pr.getName()
              + " for the key ," + key + " in the secondary bucket " + bucketId
              + " the getPartition returned " + partitionName
              + " but the secondary partitions on the node is "
              + secondaryPartitions);
        }
        else {
          hydra.Log.getLogWriter().info(
              " For the region " + pr.getName() + " for the key ," + key
                  + " in the secondary bucket " + bucketId
                  + " the getPartition returned " + partitionName
                  + " and the secondary partitions on the node is "
                  + secondaryPartitions);
        }
        if (partition2Buckets.get(partitionName) == null) {
          Set<Integer> bucketIdSet = new HashSet<Integer>();
          bucketIdSet.add(bucketId);
          partition2Buckets.put(partitionName, bucketIdSet);
        }
        else {
          Set<Integer> bucketIdSet = (Set<Integer>)partition2Buckets
              .get(partitionName);
          if (!bucketIdSet.contains(bucketId)) {
            bucketIdSet.add(bucketId);
          }
        }
      }
    }
     // The following verification is not required and may fail.
    // verifyPartitionNumBuckets(pr, secondaryPartitionsForPr, partition2Buckets);
  }

  /**
   * Validation method that verifies whether the partition num buckets for
   * partitions are satisfied.
   * 
   * @param pr
   *                Partitioned Region
   * @param fpas
   *                List of FixedPartitionAttributes
   * @param partition2Buckets
   *                Map of partition to buckets
   */
  protected void verifyPartitionNumBuckets(PartitionedRegion pr,
      List<FixedPartitionAttributesImpl> fpas,
      Map<String, Set<Integer>> partition2Buckets) {
    for (FixedPartitionAttributes fpa : fpas) {
      String partitionName = fpa.getPartitionName();
      Set<Integer> bucketIdsOnMemberForPR = new HashSet<Integer>();

      // Verify that for each partition there are right number of buckets.
      if (partition2Buckets.get(partitionName) != null) {
        Set<Integer> bucketIdSetForPartition = (Set<Integer>)partition2Buckets
            .get(partitionName);
        int expectedNumBucketsForPartition = fpa.getNumBuckets();
        if (bucketIdSetForPartition.size() != expectedNumBucketsForPartition) {
          throw new TestException("For the partitioned region " + pr.getName()
              + " for partition name " + partitionName
              + " expected number of buckets is "
              + expectedNumBucketsForPartition + " (" + fpa + ") but found "
              + bucketIdSetForPartition.size()
              + " bucket ids for this partition is " + bucketIdSetForPartition);
        }
        else {
          hydra.Log.getLogWriter().info(
              "For the partitioned region " + pr.getName()
                  + " for partition name " + partitionName
                  + " found the expected number of buckets "
                  + expectedNumBucketsForPartition + " bucket ids are "
                  + bucketIdSetForPartition);
        }

        // Verify that no two partitions share the same bucket
        if (!Collections.disjoint(bucketIdSetForPartition,
            bucketIdsOnMemberForPR)) {
          throw new TestException(
              "For the partitioned region "
                  + pr.getName()
                  + " for partition name "
                  + partitionName
                  + " bucket is shared with other partitions. Partition2Bucket map is "
                  + partition2Buckets);
        }
      }

      hydra.Log.getLogWriter().info(
          "Partition to bucket map for the partitioned region " + pr.getName()
              + " is " + partition2Buckets);
    }
  }

  /**
   * Utility method that give list of parititon names from the list of
   * FixedPartitionAttributes
   * 
   * @param fpas
   *                list of FixedPartitionAttributes
   * @return list of partition names (String)
   */
  protected List<String> getPartitionNames(
      List<FixedPartitionAttributesImpl> fpas) {
    List<String> partitionNames = new ArrayList<String>();
    for (FixedPartitionAttributesImpl fpa : fpas) {
      String partitionName = fpa.getPartitionName();
      partitionNames.add(partitionName);
    }
    return partitionNames;
  }
}
