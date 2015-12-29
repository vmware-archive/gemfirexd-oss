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

package parReg.partitionMan;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.ParRegUtil;
import pdx.PdxTest;
import rebalance.RebalanceUtil;
import recovDelay.PrState;
import util.BaseValueHolder;
import util.NameFactory;
import util.PRObserver;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionManager;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * @author lynn
 *
 */
public class PartitionManTest {

  /** instance of this test class */
  public static PartitionManTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;

  // blackboard keys
  private static final String allRegionsSnapshotKey = "allRegionsSnapshot";

  //=================================================
  // initialization methods

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() throws Throwable {
    if (testInstance == null) {
      testInstance = new PartitionManTest();
      testInstance.initializeInstance();
      CacheHelper.createCache("cache1");
      createRegions();
      Log.getLogWriter().info(TestHelper.regionHierarchyToString());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        //registerInterest();
      }
    }
    testInstance.initializeInstancePerThread();
  }


  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() throws Throwable {
    if (testInstance == null) {
      testInstance = new PartitionManTest();
      testInstance.initializeInstance();
      CacheHelper.createCache("cache1");
      createRegions();
      Log.getLogWriter().info(TestHelper.regionHierarchyToString());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    testInstance.initializeInstancePerThread();
  }

  private static void createRegions() {
    boolean createAccessor = ParRegPrms.getCreateAccessor();
    List<String> regionConfigNames = TestConfig.tab().vecAt(RegionPrms.names);
    for (String configName: regionConfigNames) {
      if (createAccessor) {
        if (configName.startsWith("accessor")) {
          RegionHelper.createRegion(configName);
        }
      } else {
        if (configName.startsWith("dataStore")) {
          RegionHelper.createRegion(configName);
        }
      }
    }
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    PRObserver.installObserverHook();
  }

  /** Initialize an instance of this test class, called for each thread
   * 
   */
  private void initializeInstancePerThread() {
  }

  //=================================================
  // Hydra tasks methods

  public static void HydraTask_load() {
    final int NUM_TO_LOAD_DEFAULT = 500;
    int numToLoad = TestConfig.tab().intAt(ParRegPrms.upperThreshold, NUM_TO_LOAD_DEFAULT);
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region<?, ?>> allRegions = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }
    long lastLogTime = System.currentTimeMillis();
    final long logIntervalMs = 10000;
    while (true) {
      Object key = NameFactory.getNextPositiveObjectName();
      long counterValue = NameFactory.getCounterForName(key);
      if (counterValue <= numToLoad) {
        Object value = new ValueHolder((String)key, rv);
        for (Region aRegion: allRegions) {
          aRegion.put(key, value);
          if (System.currentTimeMillis() - lastLogTime >= logIntervalMs) {
            Log.getLogWriter().info("Loading with key index " + counterValue + ", targeting key index " + numToLoad);
            lastLogTime = System.currentTimeMillis();
          }
        }
      } else {
        break;
      }
    }
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  /** Iterate through all buckets in the pr and call PartitionManager.createPrimaryBucket(...) on each.
   *  Validate the PR after the call.
   */
  public static void HydraTask_createPrimBuckets() {
    testInstance.testCreatePrimaryBucket();
  }

  /** Write a snapshot of all regions to the blackboard
   * 
   */
  public static void HydraTask_writeSnapshot() {
    testInstance.writeSnapshot();
  }

  /** Verify that all regions defined in this member have their primary buckets colocated
   */
  public static void HydraTask_verifyPrimaryColocation() {
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region<?, ?>> allRegions = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }

    Region goldenRegion = CacheHelper.getCache().getRegion("/pr1");
    Set goldenPrimBucketSet = new HashSet(((PartitionedRegion)goldenRegion).getLocalPrimaryBucketsListTestOnly());
    Log.getLogWriter().info(PrState.getPrPicture(goldenRegion));
    for (Region aRegion: allRegions) {
      Set primaryBucketSet = new HashSet(((PartitionedRegion)aRegion).getLocalPrimaryBucketsListTestOnly());
      if (goldenPrimBucketSet == null) {
        goldenPrimBucketSet = primaryBucketSet;
        goldenRegion = aRegion;
      } else {
        if (!primaryBucketSet.equals(goldenPrimBucketSet)) {
          Set missingBuckets = new HashSet(goldenPrimBucketSet);
          missingBuckets.removeAll(primaryBucketSet);
          Set extraBuckets = new HashSet(primaryBucketSet);
          extraBuckets.removeAll(goldenPrimBucketSet);
          StringBuffer aStr = new StringBuffer();
          if (missingBuckets.size() != 0) {
            aStr.append("The bucketIDs " + missingBuckets + " are primary in " + goldenRegion.getFullPath() + 
                " but are not primary in " + aRegion.getFullPath() + "\n");
          }
          if (extraBuckets.size() != 0) {
            aStr.append("The bucketIDs " + extraBuckets + " are primary in " + aRegion.getFullPath() +
                " but are not primary in " + goldenRegion.getFullPath() + "\n");
          }
          aStr.append("Primary bucket ids for " + goldenRegion.getFullPath() + ": " + goldenPrimBucketSet +
              ", primary bucket ids for " + aRegion.getFullPath() + ": " + primaryBucketSet);
          Log.getLogWriter().info(PrState.getPrPicture(goldenRegion) + "\n\n" + PrState.getPrPicture(aRegion));
          throw new TestException("Bug 44181 detected " + aStr.toString());
        } else {
          Log.getLogWriter().info(goldenRegion.getFullPath() + " and " + aRegion.getFullPath() + " have colocated primary bucket ids: " +
              primaryBucketSet);
        }
      }
    }
  }

  /** Hydra task to rebalance only the colocatedWithRegion
   */
  public static void HydraTask_rebalance() {
    long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.rebalance); 
    if (counter == 1) {
      ResourceManager resMan = CacheHelper.getCache().getResourceManager();
      RebalanceFactory factory = resMan.createRebalanceFactory();
      Set regSet = new HashSet();
      regSet.add("/pr1");
      factory.includeRegions(regSet);
      regSet = new HashSet();
      List<String> regionNames = TestConfig.tab().vecAt(RegionPrms.regionName);
      for (String regionName: regionNames) {
        if (!regionName.equals("/pr1")) {
          regSet.add(CacheHelper.getCache().getRegion("/" + regionName));
        }
      }
      try {
        Log.getLogWriter().info("Calling rebalance simulate");
        RebalanceOperation simulateOp = factory.simulate();
        RebalanceResults simulateResults = simulateOp.getResults(); 
        Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(simulateResults, "Simulate"));
        Log.getLogWriter().info("Starting rebalancing");
        RebalanceOperation rebalanceOp = factory.start();
        RebalanceResults rebalanceResults = rebalanceOp.getResults();
        Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));
        RebalanceUtil.isBalanceImproved(rebalanceResults); // this throws an exception if not improved
        RebalanceUtil.checkSimulateAgainstRebalance(simulateResults, rebalanceResults);
        RebalanceUtil.primariesBalanced();
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  /** Verify regions that are PRs 
   *
   */
  public static void HydraTask_verifyPRs() {
    Set<Region<?, ?>> allRegions = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(allRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }
    StringBuffer aStr = new StringBuffer();
    for (Region aRegion: allRegions) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
        Log.getLogWriter().info("Verifying PR " + aRegion.getFullPath() + " with " + redundantCopies + " redundantCopies");
        // verify PR metadata 
        try {
          ParRegUtil.verifyPRMetaData(aRegion);
        } catch (Exception e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        } catch (TestException e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        }

        // verify primaries
        try {
          ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
        } catch (Exception e) {
          aStr.append(e.toString() + "\n");
        }

        // verify PR data
        try {
          ParRegUtil.verifyBucketCopies(aRegion, redundantCopies);
        } catch (Exception e) {
          aStr.append(e.toString() + "\n");
        } catch (TestException e) {
          aStr.append(e.toString() + "\n");
        }

        if (aStr.length() > 0) {
          // shutdownHook will cause all members to dump partitioned region info
          throw new TestException(aStr.toString());
        }
        Log.getLogWriter().info("Done verifying PR internal consistency for " + aRegion.getFullPath());
      }
    }
  }

  /** Verify from the blackboard snapshot
   * 
   */
  public static void HydraTask_verifyFromSnapshot() {
    try {
      testInstance.verifyFromSnapshot();
    } catch (TestException e) {
      throw new TestException("Bug 44173 detected: " + TestHelper.getStackTrace(e));
    }
  }
  
  /** Disconnect from the DS
   * 
   */
  public static void HydraTask_disconnect() {
    DistributedSystemHelper.disconnect();
    testInstance = null;
  }

  //=================================================
  // other methods

  /** Iterate through all buckets in the pr and call PartitionManager.createPrimaryBucket(...) on each.
   *  Validate the PR after the call.
   */ 
  private void testCreatePrimaryBucket() throws TestException {
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    boolean destroyExistingRemote = ParRegPrms.getDestroyExistingRemote();
    boolean destroyExistingLocal = ParRegPrms.getDestroyExistingLocal();

    for (Region<?, ?> aRegion: rootRegions) {
      int numBuckets = aRegion.getAttributes().getPartitionAttributes().getTotalNumBuckets();
      int numRCs = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
      for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
        Log.getLogWriter().info("BucketId is " + bucketId);
        String before_prPicture = PrState.getPrPicture(aRegion);
        Log.getLogWriter().info(PrState.getPrPicture(aRegion, bucketId));

        // find the expected result; if expectedResult is null, then we expect an IllegalStateException
        Boolean expectedResult = null;
        Boolean[] tmp = ParRegUtil.getBucketStatus(aRegion,  bucketId);
        if (tmp == null) { // bucketId does not exist anywhere yet
          expectedResult = true;
        } else {
          boolean bucketIsLocal = tmp[0];
          boolean bucketIsPrimary = tmp[1];
          if (bucketIsLocal && bucketIsPrimary) {
            expectedResult = destroyExistingLocal;
          } else {
            if (destroyExistingRemote) {
              expectedResult = true;
            } else {
              expectedResult = null; // expect IllegalStateException
            }
          }
        }
        Log.getLogWriter().info("Expected result of PartitionManager.createPrimaryBucket(...) is " + 
            ((expectedResult == null) ? (IllegalStateException.class.getName()) : (expectedResult.toString())));

        // do the createPrimaryBucketCall and validate its result
        Log.getLogWriter().info("Calling createPrimaryBucket for " + aRegion.getFullPath() + " with bucket ID " + bucketId + 
            ", destroyExistingRemote " + destroyExistingRemote + " destroyExistingLocal " + destroyExistingLocal);
        boolean result = false;
        try {
          long startTime = System.currentTimeMillis();
          result = PartitionManager.createPrimaryBucket(aRegion, bucketId, destroyExistingRemote, destroyExistingLocal);
          long duration = System.currentTimeMillis() - startTime;
          Log.getLogWriter().info("Done calling createPrimaryBucket for " + aRegion.getFullPath() + " with bucket ID " + bucketId + ", result is " + result +
              ", duration " + duration + "ms");
          if (expectedResult == null) {
            throw new TestException("Expected call to PartitionManager.createPrimaryBucket(...) to throw " + IllegalStateException.class.getName() +
                ", but it returned " + result);
          }
        } catch (IllegalStateException e) {
          Log.getLogWriter().info("PartitionManager.createPrimaryBucket(...) resulted in " + e);
          if (expectedResult != null) {
            throw new TestException("Expected call to PartitionManager.createPrimaryBucket(...) to return " + expectedResult + " but it threw " + e);
          }
        }
        Log.getLogWriter().info(PrState.getPrPicture(aRegion, bucketId));

        // validate if the bucket is now local and primary
        
        if ((expectedResult == null) || (!expectedResult)) { // result was IllegalStateException or false; no buckets should have moved
          String after_prPicture = PrState.getPrPicture(aRegion);
          if (!before_prPicture.equals(after_prPicture)) {
            Log.getLogWriter().info("Before call to createPrimaryBucket: " + before_prPicture + "\nAfter call to createPrimaryBucket: " + after_prPicture);
            throw new TestException("After call to createPrimaryBucket buckets were moved");
          } else {
            Log.getLogWriter().info("Verified that no buckets moved after calling createPrimaryBucket");
          }
        } else { // the bucket should have moved
          List primaryBucketList = ((PartitionedRegion)aRegion).getLocalPrimaryBucketsListTestOnly();
          Log.getLogWriter().info("Primary buckets for this jvm: " + primaryBucketList);
          if (!primaryBucketList.contains(bucketId)) {
            Log.getLogWriter().info("Before call to createPrimaryBucket: " + before_prPicture);
            Log.getLogWriter().info("After call to createPrimaryBucket: " + PrState.getPrPicture(aRegion));
            throw new TestException("Bug 44181 detected: After calling createPrimaryBucket for bucketId " + bucketId + 
                ", this member does not host that bucket as a primary. Primary buckets for this member: " + primaryBucketList);
          }
        }

        // validate primaries on entire PR 
        try {
          ParRegUtil.verifyPrimaries(aRegion, numRCs);
        } catch (TestException e) { // log the before and after pictures before throwing the exception
          Log.getLogWriter().info("Before call to createPrimaryBucket: " + before_prPicture);
          Log.getLogWriter().info("After call to createPrimaryBucket: " + PrState.getPrPicture(aRegion));
          throw new TestException("Bug 44174 detected: " + TestHelper.getStackTrace(e));
        }

        // validate all data is present
        try {
          verifyFromSnapshot();
        } catch (TestException e) { // log the before and after pictures before throwing the exception
          Log.getLogWriter().info("Before call to createPrimaryBucket: " + before_prPicture);
          Log.getLogWriter().info("After call to createPrimaryBucket: " + PrState.getPrPicture(aRegion));
          throw new TestException("Bug 44173 detected: " + TestHelper.getStackTrace(e));
        }

        // verify bucket copies
        try {
          ParRegUtil.verifyBucketCopies(aRegion, numRCs);
        } catch (TestException e) { // log the before and after pictures before throwing the exception
          Log.getLogWriter().info("Before call to createPrimaryBucket: " + before_prPicture);
          Log.getLogWriter().info("After call to createPrimaryBucket: " + PrState.getPrPicture(aRegion));
          throw e;
        }
      }
    }
  }

  /** Write a snapshot of all regions to the blackbaord
   * 
   */
  protected void writeSnapshot() {
    Set<Region<?, ?>> allRegions = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(allRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }
    Log.getLogWriter().info("Preparing to write snapshot for " + allRegions.size() + " regions");
    Map allRegionsSnapshot = new HashMap();
    for (Region aRegion: allRegions) {
      Map regionSnapshot = new HashMap();
      for (Object key: aRegion.keySet()) {
        Object value = null;
        if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
          value = aRegion.get(key);
        }
        if (value instanceof BaseValueHolder) {
          regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
        } else {
          regionSnapshot.put(key, value);
        }
      }
      allRegionsSnapshot.put(aRegion.getFullPath(), regionSnapshot);
      Log.getLogWriter().info("Region snapshot for " + aRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    }
    ParRegBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    Log.getLogWriter().info("Put snapshot for " + allRegions.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm. 
   * 
   */
  private void verifyFromSnapshot() {
    Map<String, Map> allRegionsSnapshot = (Map)(ParRegBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
    if (allRegionsSnapshot == null) { // recoverFromGii test relies on this being null sometimes
      Log.getLogWriter().info("Not verifying from snapshot because the snapshot in the blackboard is null");
      return;
    }
    Set snapshotRegionNames = allRegionsSnapshot.keySet();
    Set<String> definedRegionNames = new HashSet();
    for (Region aRegion: CacheHelper.getCache().rootRegions()) {
      definedRegionNames.add(aRegion.getFullPath());
      Set<Region> subRegSet = aRegion.subregions(true);
      for (Region subReg: subRegSet) {
        definedRegionNames.add(subReg.getFullPath());
      }
    }
    Set missingRegionsInCache = new HashSet(snapshotRegionNames);
    missingRegionsInCache.removeAll(definedRegionNames);
    Set extraRegionsInCache = new HashSet(definedRegionNames);
    extraRegionsInCache.removeAll(snapshotRegionNames);
    if (missingRegionsInCache.size() != 0) {
      throw new TestException("Expected to find regions " + missingRegionsInCache + " defined in cache");
    }
    if (extraRegionsInCache.size() != 0) {
      throw new TestException("Found unexpected regions defined in cache: " + extraRegionsInCache);
    }

    for (String regionName: allRegionsSnapshot.keySet()) {
      Map regionSnapshot = allRegionsSnapshot.get(regionName);
      Region aRegion = CacheHelper.getCache().getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      }
      verifyFromSnapshot(aRegion, regionSnapshot);
    }
  }

  /** Verify that the given region is consistent with the given snapshot.
   * 
   * @param aRegion The region to verify
   * @param snapshot The expected contents of aRegion.
   */
  public void verifyFromSnapshot(Region aRegion, Map regionSnapshot) {
    // init
    StringBuffer errStr = new StringBuffer();
    int snapshotSize = regionSnapshot.size();
    int regionSize = aRegion.size();
    long startVerifyTime = System.currentTimeMillis();
    Log.getLogWriter().info("Verifying " + aRegion.getFullPath() + "  of size " + aRegion.size() + 
        " against snapshot containing " + regionSnapshot.size() + " entries...");

    // verify
    if (snapshotSize != regionSize) {
      errStr.append("Expected region " + aRegion.getFullPath() + " to be size " + snapshotSize + 
          ", but it is " + regionSize + "\n");
    }
    for (Object key: regionSnapshot.keySet()) {
      // validate using ParRegUtil calls even if the region is not PR; these calls do the desired job
      // containsKey
      try {
        ParRegUtil.verifyContainsKey(aRegion, key, true);
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }

      // containsValueForKey
      boolean containsValueForKey = aRegion.containsValueForKey(key);
      Object expectedValue = regionSnapshot.get(key);
      try {
        ParRegUtil.verifyContainsValueForKey(aRegion, key, (expectedValue != null));
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }

      // check the value
      if (containsValueForKey) {
        try {
          Object actualValue = aRegion.get(key);

          if (actualValue instanceof BaseValueHolder) {
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else if (actualValue instanceof PdxInstance) {
            actualValue = PdxTest.toValueHolder(actualValue);
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else {
            if ((actualValue instanceof byte[]) &&
                (expectedValue instanceof byte[])) {
              byte[] actual = (byte[])actualValue;
              byte[] expected = (byte[])expectedValue;
              if (actual.length != expected.length) {
                throw new TestException("Expected value for key " + key + " to be " +
                    TestHelper.toString(expectedValue) + ", but it is " +
                    TestHelper.toString(actualValue));
              }
            } else {
              throw new TestException("Expected value for key " + key + " to be " +
                  TestHelper.toString(expectedValue) + ", but it is " +
                  TestHelper.toString(actualValue));
            }
          }
        } catch (TestException e) {
          errStr.append(e.getMessage() + "\n");
        }
      } 
      // else value in region is null; the above check for containsValueForKey
      // checked that  the snapshot is also null
    }

    // check for extra keys in the region that were not in the snapshot
    Set<?> aRegionKeySet = new HashSet(aRegion.keySet()); 
    Set snapshotKeySet = regionSnapshot.keySet();
    aRegionKeySet.removeAll(snapshotKeySet);
    if (aRegionKeySet.size() != 0) {
      errStr.append("Found the following unexpected keys in " + aRegion.getFullPath() + 
          ": " + aRegionKeySet + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
    Log.getLogWriter().info("Done verifying " + aRegion.getFullPath() + " from snapshot containing " + snapshotSize + " entries, " +
        "verification took " + (System.currentTimeMillis() - startVerifyTime) + "ms");
  }
}
