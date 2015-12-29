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
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

public class MultiPRHeapEvictionBehaviorTest {

  protected static MultiPRHeapEvictionBehaviorTest testInstance;

  protected static Cache theCache;

  // public static final int MAX_KEYS_TO_BE_POPULATED = 1356;
  public static final int MAX_KEYS_TO_BE_POPULATED = (int)TestConfig.tab()
      .longAt(EvictionPrms.maxEntries, 1356 * 2);

  public static final int ENTRY_SIZE_IN_BYTES = 1280 * 1024;

  public static final int START_OF_LAST_BUCKETS_ID = 101;

  protected static int totalEvictedEntries = 0;

  protected static int evictionThresholdKeys = 475; // =2048*1024/1280*0.6/2
                                                    // little less than that to
                                                    // reach heap percentage

  protected static int extraKeysToBePopulatedInRegionBLastBucket = 100;

  protected static PartitionedRegion regionA;

  protected static PartitionedRegion regionB;

  /**
   * Initialize the test and the regions
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new MultiPRHeapEvictionBehaviorTest();
      testInstance.initialize("regionA");
      testInstance.initialize("regionB");
    }
  }

  protected void initialize(String regionDescriptName) {
    theCache = CacheHelper.createCache("cache1");

    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    RegionAttributes attributes = RegionHelper
        .getRegionAttributes(regionDescriptName);
    if (regionDescriptName.equals("regionA")) {
      regionA = (PartitionedRegion)theCache
          .createRegion(regionName, attributes);
    }
    else if (regionDescriptName.equals("regionB")) {
      regionB = (PartitionedRegion)theCache
          .createRegion(regionName, attributes);
    }
  }

  /**
   * Task to populate both the regions incrementally till it reaches the
   * threshold
   */
  public synchronized static void HydraTask_putUnEvenlyTillThreshold() {
    testInstance.populateSequentiallyAndIncrementally();
    hydra.Log.getLogWriter().info(
        "After putting till threshold the region sizes are " + regionA.size()
            + " and " + regionB.size());
    //Verify no eviction happened
    testInstance.verifyNoEviction(regionA);
    testInstance.verifyNoEviction(regionB);
  }

  /**
   * This task is done once the regions are populated till the eviction
   * threshold is reached. In this task only one region is populated to trigger
   * the eviction of both PRs.
   */
  public synchronized static void HydraTask_putExtraKeysWithSleep() {
    hydra.MasterController.sleepForMs(20000);
    testInstance.populateSpecificBucketofRegionWithSleep(regionB, regionB
        .getPartitionAttributes().getTotalNumBuckets() - 1,
        extraKeysToBePopulatedInRegionBLastBucket);
  }

  public synchronized static void HydraTask_logRegionSize() {
    testInstance.logRegionSize(regionA);
    testInstance.logRegionSize(regionB);
  }

  public static void HydraTask_printBucketAndDiskEntries() {
    testInstance.printBucketAndDiskEntries(regionA);
    testInstance.printBucketAndDiskEntries(regionB);
  }

  /**
   * Task to verify that even though the eviction is trigerred by one PR,
   * both the PRs evict and they do in proportion to the bucket sizes
   */
  public static void HydraTask_verifyProportionateEviction() {
    testInstance.verifyProportionateEviction(regionA);
    testInstance.verifyProportionateEviction(regionB);
  }

  protected void logRegionSize(PartitionedRegion aRegion) {
    hydra.Log.getLogWriter().info(
        "Region " + aRegion.getName() + " has expected size " + aRegion.size());
  }

  protected void populateSequentiallyAndIncrementally() {
    // Get the lowest totalNumBuckets between the PRs.
    int totalNumBuckets = regionA.getPartitionAttributes().getTotalNumBuckets() <= regionB
        .getPartitionAttributes().getTotalNumBuckets() ? regionA
        .getPartitionAttributes().getTotalNumBuckets() : regionB
        .getPartitionAttributes().getTotalNumBuckets();

    // Populate buckets incrementally till threshold
    for (int bucketId = 0; bucketId < totalNumBuckets; bucketId++) {
      populateSpecificBucketSpecificSize(bucketId, bucketId * 10 + 1,
          totalNumBuckets);
      if (EvictionBB.getBB().getSharedCounters().read(
          EvictionBB.ExecutionNumber) > evictionThresholdKeys
          && TestConfig.tab().booleanAt(
              EvictionPrms.pauseAfterEvictionThreshold, false)) {
        return;
      }
    }
  }

  protected void populateSpecificBucketSpecificSize(int bucketId,
      int maxEntriesPerBucket, int totalNumBuckets) {

    hydra.Log.getLogWriter().info(
        "Populating the bucketId " + bucketId + " with " + maxEntriesPerBucket
            + "entries");

    int entryKey = bucketId; // First entry will be 0 for bucket id 0, 1 for
                              // bucket id 1 and 112 for bucketid 112.

    for (int numEntriesPerBucket = 0; numEntriesPerBucket < maxEntriesPerBucket; numEntriesPerBucket++) {
      if (EvictionBB.getBB().getSharedCounters().read(
          EvictionBB.ExecutionNumber) > evictionThresholdKeys
          && TestConfig.tab().booleanAt(
              EvictionPrms.pauseAfterEvictionThreshold, false)) {
        return;
      }
      regionA.put(entryKey, new byte[ENTRY_SIZE_IN_BYTES]);
      regionB.put(entryKey, new byte[ENTRY_SIZE_IN_BYTES]);
      EvictionBB.getBB().getSharedCounters().incrementAndRead(
          EvictionBB.ExecutionNumber);
      entryKey = entryKey + totalNumBuckets;
    }
  }

  protected void populateSpecificBucketofRegionWithSleep(
      PartitionedRegion aRegion, int bucketId, int numEntries) {

    hydra.Log.getLogWriter().info(
        "Populating the bucketId " + bucketId + " with " + numEntries
            + "entries");

    int entryKey = bucketId; // First entry will be 0 for bucket id 0, 1 for
                              // bucket id 1 and 112 for bucketid 112.

    for (int numEntriesPerBucket = 0; numEntriesPerBucket < numEntries; numEntriesPerBucket++) {
      hydra.MasterController.sleepForMs(500);
      regionB.put(entryKey, new byte[ENTRY_SIZE_IN_BYTES]);
      entryKey = entryKey
          + regionB.getPartitionAttributes().getTotalNumBuckets();
    }

  }

  protected void printBucketAndDiskEntries(PartitionedRegion aRegion) {
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
            "For the region " + aRegion.getName()
                + " For the bucket region with id " + bucketId
                + " entries left in vm is "
                + ((BucketRegion)bucket).getNumEntriesInVM()
                + " and entries evicted "
                + ((BucketRegion)bucket).getEvictions());
      }
      else if (aRegion.getAttributes().getEvictionAttributes().getAction() == EvictionAction.LOCAL_DESTROY) {
        hydra.Log.getLogWriter().info(
            "For the region " + aRegion.getName()
                + "  For the bucket region with id " + bucketId
                + " entries evicted : " + ((BucketRegion)bucket).getEvictions()
                + " and bucket size is "
                + ((BucketRegion)bucket).getSizeForEviction());
      }
    }
    HeapMemoryMonitor hmm = ((InternalResourceManager) theCache.getResourceManager()).getHeapMonitor();
    hydra.Log.getLogWriter().info(
        "The tenuredHeasUsage is : "
            + hmm.getBytesUsed() / (1024 * 1024));
    hydra.Log.getLogWriter().info(
        "Total evicted entries = " + totalEvictedEntries);
  }

  /**
   * Verify that the eviction of buckets in a manner that fatter buckets evict more.
   * Also verifies that both PRs do evict
   * @param aRegion
   */
  protected void verifyProportionateEviction(PartitionedRegion aRegion) {

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
    int numDeviatedBuckets = 0; //Number of buckets that deviated from expected eviction

    int previousBucketId = -1;
    long previousBucketEviction = 0;
    long previousBucketSize = 0;

    long totalEvictionsForRegion = 0;

    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion bucket = (BucketRegion)entry.getValue();
      int bucketId = ((BucketRegion)bucket).getId();
      long numEvictionsForBucket = ((BucketRegion)bucket).getEvictions();
      totalEvictionsForRegion += numEvictionsForBucket;

      long bucketSize;
      try {
        bucketSize = ((BucketRegion)bucket).getNumEntriesInVM()
            + ((BucketRegion)bucket).getNumOverflowOnDisk();

      }
      catch (Exception e) {
        throw new TestException("Caught ", e);
      }

      if (numEvictionsForBucket == bucketSize) {
        throw new TestException("For the region " + aRegion.getName()
            + " bucket region with id " + bucketId
            + " bucket evicted all entries");
      }

      if ((numEvictionsForBucket < previousBucketEviction)
          && (bucketSize > previousBucketSize)) {
        numDeviatedBuckets++;
        errorString.append("For the region " + aRegion.getName()
            + " bucket region with id " + bucketId + " with size " + bucketSize
            + " numEvicted is " + numEvictionsForBucket
            + " but leaner bucket with id " + previousBucketId
            + " with bucket size " + previousBucketSize
            + " evicted more entries " + previousBucketEviction + "\n");

        if (numDeviatedBuckets > 1) { // Give tolerance for 1 bucket to deviate
          throw new TestException(errorString);
        }
        
        hydra.Log.getLogWriter().info(
            "For the region " + aRegion.getName() + " bucket region with id "
                + bucketId + " with size " + bucketSize + " numEvicted is "
                + numEvictionsForBucket + " but leaner bucket with id "
                + previousBucketId + " with bucket size " + previousBucketSize
                + " evicted more entries " + previousBucketEviction);
      }
      else {
        String s = "For the region " + aRegion.getName()
            + " bucket region with id " + bucketId + " with size " + bucketSize
            + " numEvicted is " + numEvictionsForBucket
            + " and leaner bucket with id " + previousBucketId
            + " with bucket size " + previousBucketSize
            + " evicted lesser entries " + previousBucketEviction;
        hydra.Log.getLogWriter().info(s);
      }
      previousBucketId = bucketId;
      previousBucketEviction = numEvictionsForBucket;
      previousBucketSize = bucketSize;
    }

    if (totalEvictionsForRegion <= 0) {
      throw new TestException("Region " + aRegion.getName() + " did not evict.");
    }
  }
  
  /**
   * Task to verify that no eviction happened in the region.
   * This is used to verify that the regions did not triger eviction,
   * when they were populated till eviction threshold.
   * @param aRegion
   */
  protected void verifyNoEviction(PartitionedRegion aRegion) {
    Set<Map.Entry<Integer, BucketRegion>> bucketList = aRegion.getDataStore()
        .getAllLocalBuckets();
    Iterator iterator = bucketList.iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion bucket = (BucketRegion)entry.getValue();
      int bucketId = ((BucketRegion)bucket).getId();
      long numEvictionsForBucket = ((BucketRegion)bucket).getEvictions();
      totalEvictedEntries += numEvictionsForBucket;
    }
    if (totalEvictedEntries != 0) {
      throw new TestException(
          "Test issue - Test expects no eviction to be happened when the regions are populated till eviction threshold but region "
              + aRegion.getName()
              + " evicted "
              + totalEvictedEntries
              + " entries");
    }else {
      hydra.Log.getLogWriter().info("As expected region "+aRegion.getName()+" did not evict");
    }
  }
}
