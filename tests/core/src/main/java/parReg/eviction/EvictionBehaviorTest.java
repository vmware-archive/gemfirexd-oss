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
package parReg.eviction;

import hydra.CacheHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

public class EvictionBehaviorTest {

  protected static EvictionBehaviorTest testInstance;

  protected static Cache theCache;

  protected static PartitionedRegion aRegion;

  // public static final int MAX_KEYS_TO_BE_POPULATED = 1356;
  public static final int MAX_KEYS_TO_BE_POPULATED = (int)TestConfig.tab()
      .longAt(EvictionPrms.maxEntries, 1356 * 2);;

  public static final int ENTRY_SIZE_IN_BYTES = 1280 * 1024;

  public static final int START_OF_LAST_BUCKETS_ID = 101;

  protected static int totalEvictedEntries = 0;

  protected static int evictionThresholdKeys = (int)(2048 * 1024 * 1024 / ENTRY_SIZE_IN_BYTES * 0.75);

  /**
   * Task to initialize the test and create the PR
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new EvictionBehaviorTest();
      testInstance.initialize("clientRegion");
    }
  }

  /**
   * Task that sequentially and uniformly populate each buckets. First bucket is
   * populated fully before the next bucket is populated in the order of bucket
   * id. So first bucket id 0 is fully populated before bucket id 1 and so on.
   */
  public synchronized static void HydraTask_populateSequentiallyAndEvenly() {
    testInstance.populateSequentiallyAndEvenly();
  }

  /**
   * Task that sequentially but incrementally populate each buckets. First
   * bucket is populated fully before the next bucket is populated in the order
   * of bucket id. So first bucket id 0 is fully populated before bucket id 1
   * and so on. Bucket id 1 will have more entries than bucket id 0 and so on.
   * Hence the bucket with smaller bucket id will have fewer entries than bucket
   * with higher bucket id.
   */
  public synchronized static void HydraTask_populateSequentiallyAndIncrementally() {
    testInstance.populateSequentiallyAndIncrementally();
  }

  public synchronized static void HydraTask_logRegionSize() {
    testInstance.logRegionSize();
  }

  public static void HydraTask_printBucketAndDiskEntries() {
    testInstance.printBucketAndDiskEntries();
  }

  /**
   * This is validation task for the test where the buckets are evenly
   * populated. Irrespective of the order in which buckets are populated, heap
   * eviction has to ensure that all evenly populated buckets should evict
   * evenly as well (more or less) Tolerance is given because it will to be
   * thoroughly done because of the bucket sorter thread interval
   */
  public static void HydraTask_verifyUniformBucketEviction() {
    testInstance.verifyUniformBucketEviction();
  }

  /**
   * This is validation task for the test where the buckets are evenly
   * populated. Irrespective of the order in which buckets are populated, heap
   * eviction has to ensure that fatter buckets should evict proportionally more
   * than the leaner buckets (more or less) Tolerance is given because it will
   * to be thoroughly done because of the bucket sorter thread interval
   */
  public static void HydraTask_verifyIncrementalEviction() {
    testInstance.verifyIncrementalEviction();
  }

  protected void logRegionSize() {
    // if (aRegion.size() != MAX_KEYS_TO_BE_POPULATED){
    // throw new TestException("Expected the region size to be
    // "+MAX_KEYS_TO_BE_POPULATED+" but found "+aRegion.size());
    // }else {
    hydra.Log.getLogWriter().info("Region has expected size " + aRegion.size());
    // }
  }

  /**
   * Task to initialize the cache and the region
   */
  protected void initialize(String regionDescriptName) {
    theCache = CacheHelper.createCache("cache1");
    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    Log.getLogWriter().info("Creating region " + regionName);
    RegionAttributes attributes = RegionHelper
        .getRegionAttributes(regionDescriptName);
    aRegion = (PartitionedRegion)theCache.createRegion(regionName, attributes);
    Log.getLogWriter().info("Completed creating region " + aRegion.getName());
  }

  protected void populateSequentiallyAndEvenly() {
    int totalNumBuckets = aRegion.getPartitionAttributes().getTotalNumBuckets();
    int numEntriesPerBucket = MAX_KEYS_TO_BE_POPULATED / totalNumBuckets;

    for (int bucketId = 0; bucketId < totalNumBuckets; bucketId++) {
      populateSpecificBucketSpecificSize(bucketId, numEntriesPerBucket);
    }
  }

  protected void populateSequentiallyAndIncrementally() {
    int totalNumBuckets = aRegion.getPartitionAttributes().getTotalNumBuckets();
    // int numEntriesPerBucket = MAX_KEYS_TO_BE_POPULATED/totalNumBuckets;

    for (int bucketId = 0; bucketId < totalNumBuckets; bucketId++) {
      populateSpecificBucketSpecificSize(bucketId, bucketId + 10);
    }
  }

  protected void populateSpecificBucketSpecificSize(int bucketId,
      int maxEntriesPerBucket) {
    try {
      hydra.Log.getLogWriter().info(
          "Populating the bucketId " + bucketId + " of PR " + aRegion.getName()
              + " with " + maxEntriesPerBucket + "entries");
      int initialNumOfEntriesBeforePopulatingBucket = aRegion.getBucketKeys(
          bucketId).size();

      int totalNumBuckets = aRegion.getPartitionAttributes()
          .getTotalNumBuckets();
      int entryKey = bucketId; // First entry will be 0 for bucket id 0, 1 for
                                // bucket id 1 and 112 for bucketid 112.

      // Start populating
      for (int numEntriesPerBucket = 0; numEntriesPerBucket < maxEntriesPerBucket; numEntriesPerBucket++) {
        if (aRegion.size() > evictionThresholdKeys
            && TestConfig.tab().booleanAt(
                EvictionPrms.pauseAfterEvictionThreshold, false)) {
          hydra.MasterController.sleepForMs(60000);
        }
        aRegion.put(entryKey, new byte[ENTRY_SIZE_IN_BYTES]);
        // hydra.Log.getLogWriter().info(" Put for key " + entryKey + " took " +
        // (end - start) + " ms");
        // New key will be bucket id + total Num of buckets; 113 for bucketid 0,
        // 114 for bucketid1 and 115 for bucketid2
        entryKey = entryKey + totalNumBuckets;
      }

      int initialNumOfEntriesAfterPopulatingBucket = aRegion.getBucketKeys(
          bucketId).size();
      if (initialNumOfEntriesAfterPopulatingBucket != maxEntriesPerBucket
          && aRegion.getAttributes().getEvictionAttributes().getAction() == EvictionAction.OVERFLOW_TO_DISK) {
        throw new TestException("Test issue: bucket supposed to be "
            + maxEntriesPerBucket + " after population");
      }

      hydra.Log.getLogWriter().info(
          "For bucketId " + bucketId + " numEntriesbefore populating "
              + initialNumOfEntriesBeforePopulatingBucket + " and after is "
              + initialNumOfEntriesAfterPopulatingBucket);

    }
    catch (Exception e) {
      throw new TestException("Caught exception during population ", e);
    }

  }

  protected void printBucketAndDiskEntries() {
    hydra.MasterController.sleepForMs(20000);

    int totalNumBuckets = aRegion.getPartitionAttributes().getTotalNumBuckets();
    int numEntriesPerBucket = MAX_KEYS_TO_BE_POPULATED / totalNumBuckets;

    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Iterator iterator = bucketList.iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion bucket = (BucketRegion)entry.getValue();
      int bucketId = ((BucketRegion)bucket).getId();
      long numEvictionsForBucket = ((BucketRegion)bucket).getEvictions();
      totalEvictedEntries += numEvictionsForBucket;
      // long entriesLeftInVm = numEntriesPerBucket - numEvictionsForBucket > 0
      // ? numEntriesPerBucket - numEvictionsForBucket : 0;
      if (aRegion.getAttributes().getEvictionAttributes().getAction() == EvictionAction.OVERFLOW_TO_DISK) {
        hydra.Log.getLogWriter().info(
            " For the bucket region with id " + bucketId
                + " entries left in vm is "
                + ((BucketRegion)bucket).getNumEntriesInVM()
                + " and entries evicted "
                + ((BucketRegion)bucket).getNumOverflowOnDisk());
      }
      else {
        hydra.Log.getLogWriter().info(
            " For the bucket region with id " + bucketId
                + " entries left in vm is " + bucket.entryCount()
                + " and entries evicted "
                + ((BucketRegion)bucket).getEvictions());
      }
    }
    hydra.Log.getLogWriter().info(
        "The tenuredHeasUsage is : "
            + ((InternalResourceManager) theCache.getResourceManager()).getHeapMonitor().getBytesUsed()
            / (1024 * 1024));
    hydra.Log.getLogWriter().info(
        "Total evicted entries = " + totalEvictedEntries);
  }

  protected void verifyUniformBucketEviction() {
    int regionSize = aRegion.size();
    int totalNumBuckets = aRegion.getPartitionAttributes().getTotalNumBuckets();
    float averageEvictionPercentage = ((float)totalEvictedEntries * 100)
        / regionSize;
    int numDeviatedBuckets = 0; // Number of buckets that deviated from expected
                                // eviction
    final int PERCENT_TOLERANCE = 60;

    // hydra.Log.getLogWriter().info("Total evicted entries = "+
    // totalEvictedEntries+ " region size is "+aRegion.size()+" average eviction
    // percentage "+averageEvictionPercentage);

    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Iterator iterator = bucketList.iterator();
    StringBuffer errorString = new StringBuffer();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion bucket = (BucketRegion)entry.getValue();
      int bucketId = ((BucketRegion)bucket).getId();
      long numEvictionsForBucket = ((BucketRegion)bucket).getEvictions();
      long bucketSize;
      try {
        bucketSize = ((BucketRegion)bucket).getNumEntriesInVM()
            + ((BucketRegion)bucket).getNumOverflowOnDisk();

      }
      catch (Exception e) {
        throw new TestException("Caught ", e);
      }
      float bucketEvictionPercentage = ((float)numEvictionsForBucket * 100)
          / bucketSize;

      if (bucketEvictionPercentage > (averageEvictionPercentage + PERCENT_TOLERANCE)
          || bucketEvictionPercentage < (averageEvictionPercentage - PERCENT_TOLERANCE)) {
        errorString.append(" For the bucket region with id " + bucketId
            + " numEvicted is " + numEvictionsForBucket
            + " but average eviction is " + totalEvictedEntries
            / totalNumBuckets + " (Test considers " + PERCENT_TOLERANCE
            + " % tolerance.) \n");
        numDeviatedBuckets++;
        if (numDeviatedBuckets > totalNumBuckets * 0.4) {
          // 40% Tolerance for number of deviation as this can happen because of
          // bucket sorting time interval
          throw new TestException(errorString);
        }
      }
      else {
        String s = " For the bucket region with id " + bucketId
            + " numEvicted is " + numEvictionsForBucket
            + " but average eviction is " + totalEvictedEntries
            / totalNumBuckets;
        hydra.Log.getLogWriter().info(s);
      }
    }
  }

  protected void verifyIncrementalEviction() {
    int totalNumBuckets = aRegion.getPartitionAttributes().getTotalNumBuckets();
    int numDeviatedBuckets = 0; // Number of buckets that deviated from expected
                                // eviction

    // We cannot check percentage eviction because lower size buckets may not
    // evict and this can cause test to fail
    // Will verify that the fatter buckets will evict more.

    Set<Map.Entry<Integer, BucketRegion>> bucketList = aRegion.getDataStore()
        .getAllLocalBuckets();
    Comparator<Map.Entry<Integer, BucketRegion>> comparator = new Comparator<Map.Entry<Integer, BucketRegion>>() {

      public int compare(Entry<Integer, BucketRegion> o1,
          Entry<Integer, BucketRegion> o2) {
        if (o1.getKey() < o2.getKey()) {
          return -1;
        }
        else if (o1.getKey() > o2.getKey()) {
          return 1;
        }
        else {
          return 0;
        }
      }
    };

    TreeSet<Map.Entry<Integer, BucketRegion>> sortedBucketSet = new TreeSet<Entry<Integer, BucketRegion>>(
        comparator);
    sortedBucketSet.addAll(bucketList);

    Iterator iterator = sortedBucketSet.iterator();
    StringBuffer errorString = new StringBuffer();

    int previousBucketId = -1;
    long previousBucketEviction = 0;
    long previousBucketSize = 0;

    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion bucket = (BucketRegion)entry.getValue();
      int bucketId = ((BucketRegion)bucket).getId();
      long numEvictionsForBucket = ((BucketRegion)bucket).getEvictions();
      long bucketSize;
      try {
        bucketSize = ((BucketRegion)bucket).getNumEntriesInVM()
            + ((BucketRegion)bucket).getNumOverflowOnDisk();

      }
      catch (Exception e) {
        throw new TestException("Caught ", e);
      }
      float bucketEvictionPercentage = ((float)numEvictionsForBucket * 100)
          / bucketSize;
      
      if (bucketEvictionPercentage == 100) {
        throw new TestException("For the bucket region with id " + bucketId
            + " bucket evicted all entries");
      }

      if ((numEvictionsForBucket < previousBucketEviction)
          && (bucketSize > previousBucketSize)) {
        errorString.append("For the bucket region with id " + bucketId
            + " with size " + bucketSize + " numEvicted is "
            + numEvictionsForBucket + " but leaner bucket with id "
            + previousBucketId + " with bucket size " + previousBucketSize
            + " evicted more entries " + previousBucketEviction + "\n");
        numDeviatedBuckets++;
        if (numDeviatedBuckets > totalNumBuckets * 0.1) {
          //10% Tolerance for number of deviation as this can happen because of bucket sorting time interval
          throw new TestException(errorString);
        }
        hydra.Log.getLogWriter().info(
            "For the bucket region with id " + bucketId + " with size "
                + bucketSize + " numEvicted is " + numEvictionsForBucket
                + " but leaner bucket with id " + previousBucketId
                + " with bucket size " + previousBucketSize
                + " evicted more entries " + previousBucketEviction);
      }
      else {
        String s = "For the bucket region with id " + bucketId + " with size "
            + bucketSize + " numEvicted is " + numEvictionsForBucket
            + " and leaner bucket with id " + previousBucketId
            + " with bucket size " + previousBucketSize
            + " evicted lesser entries " + previousBucketEviction;
        hydra.Log.getLogWriter().info(s);
      }
      previousBucketId = bucketId;
      previousBucketEviction = numEvictionsForBucket;
      previousBucketSize = bucketSize;
    }
  }
}
