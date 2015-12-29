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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.colocation.Month;
import parReg.execute.PartitionObjectHolder;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

public class ColocationAndEvictionTest extends ParRegCreateDestroy {

  private ArrayList errorMsgs = new ArrayList();

  private ArrayList errorException = new ArrayList();

  protected static final int ENTRIES_TO_PUT = 50;

  protected static final int HEAVY_OBJECT_SIZE_VAL = 500;

  // following are for the PR execute
  private static ArrayList bucketsInTheDataStore;

  private static String bucketReferenceRegion;

  private static ArrayList primaryBucketsInTheDataStore;

  private static String primaryBucketReferenceRegion;

  /**
   * Creates and initializes the singleton instance of ParRegCreateDestroy in
   * this VM.
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize();
      testInstance = new ColocationAndEvictionTest();
      testInstance.initialize();
    }
    Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
    Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
  }

  /**
   * Creates and initializes the singleton instance of ParRegCreateDestroy in a
   * bridge server.
   */
  public synchronized static void HydraTask_initializeBridgeServer() {
    if (testInstance == null) {
      testInstance = new ColocationAndEvictionTest();
      ((ColocationAndEvictionTest)testInstance).initialize();
      BridgeHelper.startBridgeServer("bridge");
    }
    isBridgeClient = false;
    Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
    Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
  }

  /**
   * Populates partitioned regions.
   */
  public static void HydraTask_populateRegions() {
    ((ColocationAndEvictionTest)testInstance).doPopulate();
  }

  /**
   * Randomly create and destroy partitioned regions. Task to create partitioned
   * regions
   */
  public static void HydraTask_createRegionsAndPopulate() {
    long minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec
        * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    try {
      do {
        ((ColocationAndEvictionTest)testInstance).doCreateRegionsOnly();
        ((ColocationAndEvictionTest)testInstance).doPopulate();
      } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
    }
    finally {
      if (isBridgeConfiguration) {
        for (Iterator ri = theCache.rootRegions().iterator(); ri.hasNext();) {
          Region r = (Region)ri.next();
          ClientHelper.release(r);
        }
      }
    }
  }

  /**
   * Hydra task to create server regions
   */
  public static void HydraTask_createBridgeServerRegions() {
    Region aRegion = null;
    String regionDescriptName = null;
    for (int j = 0; j < bridgeRegionDescriptNames.size(); j++) {
      regionDescriptName = (String)(bridgeRegionDescriptNames.get(j));
      System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      try {
        aRegion = RegionHelper.createRegion(regionName, RegionHelper
            .getRegionAttributes(regionDescriptName));
        Log.getLogWriter().info(
            "Created partitioned region " + regionName
                + " with region descript name " + regionDescriptName);
      }
      catch (RegionExistsException e) {
        // region already exists; ok
        Log.getLogWriter().info(
            "Using existing partitioned region " + regionName);
        aRegion = e.getRegion();
        if (aRegion == null) {
          throw new TestException(
              "RegionExistsException.getRegion returned null");
        }
      }
    }
  }

  /**
   * Task to create and destroy partitioned regions
   */
  public static void HydraTask_dumpBuckets() {
    ((ColocationAndEvictionTest)testInstance).dumpAllTheBuckets();
  }

  public static void HydraTask_verifyColocatedRegions() {
    ((ColocationAndEvictionTest)testInstance)
        .HydraTask_verifyCustomPartitioning();
    ((ColocationAndEvictionTest)testInstance).HydraTask_verifyCoLocation();
  }

  public static void HydraTask_verifyColocatedRegionsWhenOneNodeDown() {
    hydra.MasterController.sleepForMs(90000);
    ((ColocationAndEvictionTest)testInstance)
        .HydraTask_verifyCustomPartitioningWhenOneNodeDown();
    ((ColocationAndEvictionTest)testInstance).HydraTask_verifyCoLocation();
  }

  /**
   * Task to verify custom partitioning.
   */
  public synchronized void HydraTask_verifyCustomPartitioning() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyCustomPartitioning(pr);
    }
  }

  protected void HydraTask_verifyCoLocation() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyPrimaryBucketCoLocation(pr);
      verifyBucketCoLocation(pr);

      if (errorMsgs.size() != 0) {
        for (int index = 0; index < errorMsgs.size(); index++)
          Log.getLogWriter().error((String)errorMsgs.get(index));
      }
      if (errorException.size() != 0) {
        for (int index = 0; index < errorException.size(); index++)
          throw (TestException)errorException.get(index);
      }
    }
  }

  /**
   * Task to verify custom partitioning.
   */
  public synchronized void HydraTask_verifyCustomPartitioningWhenOneNodeDown() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyCustomPartitioningWhenOneNodeDown(pr);
    }
  }

  public static void HydraTask_verifyPRWhenOneNodeDown() {
    ((ColocationAndEvictionTest)testInstance).verifyPRWhenOneNodeDown();
  }

  public static void HydraTask_killVMs() {
    ((ColocationAndEvictionTest)testInstance).killVms();
  }

  public void dumpAllTheBuckets() {
    try {
      for (int i = 0; i < regionDescriptNames.size(); i++) {
        String regionDescriptName = (String)(regionDescriptNames.get(i));
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region aRegion = theCache.getRegion(regionName);
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        pr.dumpAllBuckets(false, Log.getLogWriter().convertToLogWriterI18n());
      }

    }
    catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    catch (ClassCastException e) {
      Log
          .getLogWriter()
          .info(
              "Not dumping data stores on region "
                  + " because it is not a PartitionedRegion (probably because it's a region "
                  + " from a bridge client");
    }
  }

  /**
   * Task to verify PR bucket overflow to disk.
   */
  public static void HydraTask_verifyOverflowToDisk() {
    ((ColocationAndEvictionTest)testInstance).verifyOverflowToDisk();
  }

  public static void HydraTask_verifyMemLRU() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyMemLRU(pr);
    }
  }

  public static void HydraTask_verifyEvictionFairness() {
    verifyEvictionBucketFairness();
  }

  /**
   * Task to verify PR bucket local destroy eviction.
   */
  public static void HydraTask_verifyEvictionLocalDestroy() {
    ((ColocationAndEvictionTest)testInstance).verifyEvictionLocalDestroy();
  }

  public static void HydraTask_verifyPR() {
    ((ColocationAndEvictionTest)testInstance).verifyPR();
  }

  /**
   * Randomly create partitioned regions.
   */
  private void doPopulate() {
    if (isBridgeConfiguration && !isBridgeClient) { // this is a bridge server
      Log.getLogWriter().info("Inside bridge server");
      doPopulateRegions(bridgeRegionDescriptNames);
    }
    else {
      if (isBridgeConfiguration) {
        doPopulateRegions(regionDescriptNames);
      }
      else {
        long numOfAccessors = ParRegBB.getBB().getSharedCounters()
            .incrementAndRead(ParRegBB.numOfAccessors);
        if (numOfAccessors > TestConfig.tab().longAt(
            ParRegPrms.numberOfAccessors, 0)) {

          Iterator iterator = regionDescriptNames.iterator();
          ArrayList nonAccessorRegionDescriptNames = new ArrayList();
          while (iterator.hasNext()) {
            String regionDescription = (String)iterator.next();
            if (!regionDescription.startsWith("aRegion"))
              nonAccessorRegionDescriptNames.add(regionDescription);
          }
          doPopulateRegions(nonAccessorRegionDescriptNames);
        }
        else {
          Iterator iterator = regionDescriptNames.iterator();
          ArrayList accessorRegionDescriptNames = new ArrayList();
          while (iterator.hasNext()) {
            String regionDescription = (String)iterator.next();
            if (regionDescription.startsWith("aRegion"))
              accessorRegionDescriptNames.add(regionDescription);
          }
          doPopulateRegions(accessorRegionDescriptNames);
        }
      }
    }
  }

  public void doCreateAndPopulateRegions(List regionDescriptNames) {

    String regionDescriptName;
    Region aRegion;
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      regionDescriptName = (String)(regionDescriptNames.get(j));
      System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      RegionAttributes attributes = RegionHelper
          .getRegionAttributes(regionDescriptName);
      String poolName = attributes.getPoolName();
      if (poolName != null) {
        PoolHelper.createPool(poolName);
      }
      try {
        aRegion = theCache.createRegion(regionName, RegionHelper
            .getRegionAttributes(regionDescriptName));
        Log.getLogWriter().info(
            "Created partitioned region " + regionName
                + " with region descript name " + regionDescriptName);
      }
      catch (RegionExistsException e) {
        // region already exists; ok
        Log.getLogWriter().info(
            "Using existing partitioned region " + regionName);
        aRegion = e.getRegion();
        if (aRegion == null) {
          throw new TestException(
              "RegionExistsException.getRegion returned null");
        }
      }
      // add entries to the partitioned region
      populateRegion(aRegion);
    }
  }

  public void doPopulateRegions(List regionDescriptNames) {

    String regionDescriptName;
    Region aRegion;
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      regionDescriptName = (String)(regionDescriptNames.get(j));
      System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      RegionAttributes attributes = RegionHelper
          .getRegionAttributes(regionDescriptName);
      String poolName = attributes.getPoolName();
      if (poolName != null) {
        PoolHelper.createPool(poolName);
      }
      try {
        aRegion = theCache.getRegion(regionName);
        Log.getLogWriter()
            .info(
                "Got partitioned region " + regionName
                    + " from cache with region descript name "
                    + regionDescriptName);
      }
      catch (RegionExistsException e) {
        // region already exists; ok
        Log.getLogWriter().info(
            "Using existing partitioned region " + regionName);
        aRegion = e.getRegion();
        if (aRegion == null) {
          throw new TestException(
              "RegionExistsException.getRegion returned null");
        }
      }
      // add entries to the partitioned region
      populateRegion(aRegion);
    }
  }

  public void populateRegion(Region aRegion) {
    Log.getLogWriter().info(
        "Putting " + ENTRIES_TO_PUT + " entries into " + aRegion.getFullPath());
    for (int i = 1; i <= ENTRIES_TO_PUT; i++) {
      try {
        Month callBackArg = Month.months[TestConfig.tab().getRandGen().nextInt(
            11)];
        String keyName = aRegion.getName() + " " + System.currentTimeMillis();
        PartitionObjectHolder key = new PartitionObjectHolder(keyName,
            callBackArg);

        if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isEvictionTest, false)) {

          byte[] newVal = new byte[HEAVY_OBJECT_SIZE_VAL
              * HEAVY_OBJECT_SIZE_VAL];
          Integer keyObject = new Integer((int)ParRegBB.getBB()
              .getSharedCounters()
              .incrementAndRead(ParRegBB.numOfPutOperations));
          // Log.getLogWriter().info("Populating with heavy objects");
          aRegion.put(keyObject, newVal); // for eviction tests
        }
        else {
          aRegion.put(key, new Integer(i), callBackArg);
        }
        hydra.MasterController.sleepForMs(10);
      }
      catch (RegionDestroyedException e) {
        Log.getLogWriter().info(
            "Caught expected exception " + e + " while putting into "
                + aRegion.getFullPath() + "; continuing test");
        break;
      }
      catch (PartitionedRegionStorageException e) {
        String errStr = e.toString();
        if ((errStr.indexOf(PRHARedundancyProvider.INSUFFICIENT_STORES_MSG
            .toLocalizedString()) >= 0)
            || (errStr.indexOf(PRHARedundancyProvider.TIMEOUT_MSG
                .toLocalizedString()) >= 0)) {
          Log.getLogWriter().info(
              "Caught expected exception " + e + " while putting into "
                  + aRegion.getFullPath() + "; continuing test");
          break;
        }
        else {
          throw e;
        }
      }
      catch (CacheWriterException e) {
        if (isBridgeClient) {
          String errStr = e.toString();
          if ((errStr
              .indexOf("Either the specified region, key or value was invalid. ") >= 0)
              || (errStr.indexOf("The BridgeWriter has been closed") >= 0)
              || (errStr.indexOf(PRHARedundancyProvider.TIMEOUT_MSG
                  .toLocalizedString()) >= 0)
              || (errStr.indexOf("was not found during put request") >= 0) || // Region
              // named
              // /region8
              // was
              // not
              // found
              // during
              // put
              // request
              (errStr.indexOf("RegionDestroyedException") >= 0)) {
            Log.getLogWriter().info(
                "Caught expected exception " + e + " while putting into "
                    + aRegion.getFullPath() + "; continuing test");
            break;
          }
          else if (errStr.indexOf("Failed to put entry") >= 0) {
            throw new TestException("Bug 37120 detected: "
                + TestHelper.getStackTrace(e));
          }
          else {
            throw e;
          }
        }
        else {
          throw e;
        }
      }
    }
  }

  protected void verifyCustomPartitioning(PartitionedRegion aRegion) {

    PartitionedRegion pr = (PartitionedRegion)aRegion;
    int totalBuckets = pr.getTotalNumberOfBuckets();
    RegionAttributes attr = aRegion.getAttributes();
    PartitionAttributes prAttr = attr.getPartitionAttributes();
    int redundantCopies = prAttr.getRedundantCopies();
    int expectedNumCopies = redundantCopies + 1;

    int verifyBucketCopiesBucketId = 0;

    while (true) {

      if (verifyBucketCopiesBucketId >= totalBuckets) {
        break; // we have verified all buckets
      }

      Log.getLogWriter().info(
          "Verifying data for bucket id " + verifyBucketCopiesBucketId
              + " out of " + totalBuckets + " buckets");
      List<BucketDump> listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      }
      catch (ForceReattemptException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this
      // particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
        Log.getLogWriter().info(
            "Bucket " + verifyBucketCopiesBucketId + " is empty");
        verifyBucketCopiesBucketId++;
        continue;
      }

      if (size != expectedNumCopies) {
        errorMsgs
            .add(("For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, but have " + listOfMaps
                .size()));
        errorException.add(new TestException("For bucketId "
            + verifyBucketCopiesBucketId + ", expected " + expectedNumCopies
            + " bucket copies, but have " + listOfMaps.size()));
      }
      else {
        Log.getLogWriter().info(
            "For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, and have "
                + listOfMaps.size());
      }

      Log.getLogWriter().info(
          "Validating co-location for all the redundant copies of the bucket with Id : "
              + verifyBucketCopiesBucketId);
      // Check that all copies of the buckets have the same data
      for (int i = 0; i < listOfMaps.size(); i++) {
        BucketDump dump = listOfMaps.get(i);
        Map map = dump.getValues();
        verifyCustomPartition(map, verifyBucketCopiesBucketId);
        verifyUniqueBucketForCustomPartioning(verifyBucketCopiesBucketId);
      }

      verifyBucketCopiesBucketId++;
    }

  }

  protected void verifyCustomPartitioningWhenOneNodeDown(
      PartitionedRegion aRegion) {

    PartitionedRegion pr = (PartitionedRegion)aRegion;
    int totalBuckets = pr.getTotalNumberOfBuckets();
    RegionAttributes attr = aRegion.getAttributes();
    PartitionAttributes prAttr = attr.getPartitionAttributes();
    int redundantCopies = prAttr.getRedundantCopies();
    int expectedNumCopies = redundantCopies;

    int verifyBucketCopiesBucketId = 0;

    while (true) {

      if (verifyBucketCopiesBucketId >= totalBuckets) {
        break; // we have verified all buckets
      }

      Log.getLogWriter().info(
          "Verifying data for bucket id " + verifyBucketCopiesBucketId
              + " out of " + totalBuckets + " buckets");
      List<BucketDump> listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      }
      catch (ForceReattemptException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this
      // particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
        Log.getLogWriter().info(
            "Bucket " + verifyBucketCopiesBucketId + " is empty");
        verifyBucketCopiesBucketId++;
        continue;
      }

      if (size != expectedNumCopies) {
        errorMsgs
            .add(("For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, but have " + listOfMaps
                .size()));
        errorException.add(new TestException("For bucketId "
            + verifyBucketCopiesBucketId + ", expected " + expectedNumCopies
            + " bucket copies, but have " + listOfMaps.size()));
      }
      else {
        Log.getLogWriter().info(
            "For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, and have "
                + listOfMaps.size());
      }

      Log.getLogWriter().info(
          "Validating co-location for all the redundant copies of the bucket with Id : "
              + verifyBucketCopiesBucketId);
      // Check that all copies of the buckets have the same data
      for (int i = 0; i < listOfMaps.size(); i++) {
        BucketDump dump = listOfMaps.get(i);
        Map map = dump.getValues();
        verifyCustomPartition(map, verifyBucketCopiesBucketId);
        verifyUniqueBucketForCustomPartioning(verifyBucketCopiesBucketId);
      }
      verifyBucketCopiesBucketId++;
    }
  }

  protected void verifyCustomPartition(Map map, int bucketid) {

    Iterator iterator = map.entrySet().iterator();
    Map.Entry entry = null;
    PartitionObjectHolder key = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = (PartitionObjectHolder)entry.getKey();

      if (ParRegBB.getBB().getSharedMap().get(
          "RoutingObjectForBucketid:" + bucketid) == null) {
        Log.getLogWriter().info(
            "RoutingObject for the bucket id to be set in the BB");
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectForBucketid:" + bucketid,
            key.getRoutingHint().toString());
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectKeyBucketid:" + bucketid, key);
        Log.getLogWriter().info(
            "BB value set to " + key.getRoutingHint().toString());
      }
      else {
        Log.getLogWriter().info("Checking the value for the routing object ");
        String blackBoardRoutingObject = (String)ParRegBB.getBB()
            .getSharedMap().get("RoutingObjectForBucketid:" + bucketid);
        String keyRoutingObject = key.getRoutingHint().toString();
        if (!keyRoutingObject.equalsIgnoreCase(blackBoardRoutingObject)) {
          throw new TestException(
              "Expected same routing objects for the entries in this bucket id "
                  + bucketid + "but got different values "
                  + blackBoardRoutingObject + " and " + keyRoutingObject);
        }
        else {
          Log.getLogWriter().info(
              "Got the expected values "
                  + blackBoardRoutingObject
                  + " and "
                  + keyRoutingObject
                  + " for the keys "
                  + ParRegBB.getBB().getSharedMap().get(
                      "RoutingObjectKeyBucketid:" + bucketid) + " and " + key);
        }
      }
    }
  }

  /**
   * Task to verify that there is only a single bucket id for a routing Object
   * 
   */
  protected void verifyUniqueBucketForCustomPartioning(int bucketId) {

    if (bucketId == 0) {
      Log
          .getLogWriter()
          .info(
              "This is the first bucket, so no validation required as there is no bucket to be compared");
      return;
    }
    else {
      for (int i = 0; i < bucketId; i++) {
        if (!(ParRegBB.getBB().getSharedMap().get(
            "RoutingObjectForBucketid:" + i) == null)) {
          String referenceValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + i);
          String currentValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + bucketId);
          Log.getLogWriter().info("currentValue: " + currentValue);
          Log.getLogWriter().info("referenceValue: " + referenceValue);

          if (currentValue.equalsIgnoreCase(referenceValue)) {
            throw new TestException("Two buckets with the id " + i + " and "
                + bucketId + " have the same routing Object " + referenceValue);
          }
          else {
            Log.getLogWriter().info(
                "As expected the bucket with ids " + i + " and " + bucketId
                    + " have the different routing Object " + currentValue
                    + " and " + referenceValue);
          }

        }

      }
    }

  }

  protected void killVms() {
    // to prevent a race where a vm is killed by this method, restarted, then runs 
    // an init task to wait for recovery, but this task gets assigned again before
    // recovery completes; we must wait for recovery to complete before we kill
    // the next vm
    Object value = ParRegBB.getBB().getSharedMap().get("ReadyToKill");
    while ((value != null) && !((Boolean)value)) {
       Log.getLogWriter().info("Waiting to be ready to kill...");
       hydra.MasterController.sleepForMs(2000);
       value = ParRegBB.getBB().getSharedMap().get("ReadyToKill");
    }
    ParRegBB.getBB().getSharedMap().put("ReadyToKill", new Boolean(false));
    try {
      hydra.MasterController.sleepForMs(5000);
      if (regionDescriptNames.size() > 0) {
        String regionDescriptName = (String)(regionDescriptNames.get(0));
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = theCache.getRegion(regionName);
        if (region instanceof PartitionedRegion) {
          RegionAttributes attr = region.getAttributes();
          PartitionAttributes prAttr = attr.getPartitionAttributes();
          if (prAttr.getLocalMaxMemory() == 0) {
            Log
                .getLogWriter()
                .info(
                    "This is an accessor which is getting killed and hence to be restarted as an accessor");
            ParRegBB.getBB().getSharedMap().put(
                "Vm Id: " + RemoteTestModule.getMyVmid(), "Accessor");
          }
        }
      }

      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }

  public static void HydraTask_killVMForever() {
    ((ColocationAndEvictionTest)testInstance).killVmForEver();
  }

  protected void killVmForEver() {
    try {
      hydra.MasterController.sleepForMs(5000);
      if (regionDescriptNames.size() > 0) {
        String regionDescriptName = (String)(regionDescriptNames.get(0));
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = theCache.getRegion(regionName);
        if (region instanceof PartitionedRegion) {
          RegionAttributes attr = region.getAttributes();
          PartitionAttributes prAttr = attr.getPartitionAttributes();
          if (prAttr.getLocalMaxMemory() == 0) {
            Log
                .getLogWriter()
                .info(
                    "This is an accessor which is getting killed and hence to be restarted as an accessor");
            ParRegBB.getBB().getSharedMap().put(
                "Vm Id: " + RemoteTestModule.getMyVmid(), "Accessor");
          }
        }
      }

      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.NEVER);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }

  /**
   * Task to verify PR bucket overflow to disk.
   */
  public synchronized void verifyOverflowToDisk() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyOverflowToDisk(pr);
    }
  }

  protected void verifyOverflowToDisk(PartitionedRegion aRegion) {
    if (aRegion.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
          "This is an accessor and hence eviction need not be verified");
      return;
    }

    long numOverflowToDisk = aRegion.getDiskRegionStats()
        .getNumOverflowOnDisk();

    long entriesInVm = aRegion.getDiskRegionStats().getNumEntriesInVM();

    long totalEntriesInBuckets = 0;
    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Log.getLogWriter().info("Number of buckets= " + bucketList.size());
    Iterator iterator = bucketList.iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion localBucket = (BucketRegion)entry.getValue();
      if (localBucket != null) {

        totalEntriesInBuckets = totalEntriesInBuckets
            + localBucket.entryCount();

      }
    }
    if (bucketList.size() > 0) {
      if (numOverflowToDisk == 0) {
        throw new TestException("For the region " + aRegion.getName()
            + " no eviction happened ");

      }
      else {
        Log.getLogWriter().info(
            "For the region " + aRegion.getName()
                + " entries overflown to disk is " + numOverflowToDisk);
      }
    }
    if (totalEntriesInBuckets != (numOverflowToDisk + entriesInVm)) {
      throw new TestException(
          "Total Entries in bucket (actual value "
              + totalEntriesInBuckets
              + " ) is not the same as the sum of entries in disk and entries in vm ( "
              + numOverflowToDisk + " and " + entriesInVm + ")");
    }

    if (entriesInVm == 0) {
      EvictionAlgorithm evicAlgorithm = aRegion.getAttributes()
          .getEvictionAttributes().getAlgorithm();

      if ((evicAlgorithm == EvictionAlgorithm.LRU_HEAP)) {
        // As per Bug 39715 this can happen
        hydra.Log
            .getLogWriter()
            .warning(
                " After eviction(overflow to disk) the entries in vm is zero but entries in disk is "
                    + numOverflowToDisk);
      }
      else {
        throw new TestException(
            " After eviction(overflow to disk) the entries in vm is zero but entries in disk is "
                + numOverflowToDisk);
      }
    }
    Log
        .getLogWriter()
        .info(
            "Total Entries in bucket (actual value "
                + totalEntriesInBuckets
                + " ) is the same as the sum of entries in disk and entries in vm ( "
                + numOverflowToDisk + " and " + entriesInVm + ")");
  }

  /**
   * Task to verify PR bucket local destroy eviction.
   */
  public synchronized void verifyEvictionLocalDestroy() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyEvictionLocalDestroy(pr);
    }
  }

  protected void verifyEvictionLocalDestroy(PartitionedRegion aRegion) {
    if (aRegion.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
          "This is an accessor and hence eviction need not be verified");
      return;
    }

    double numEvicted = 0;

    EvictionAlgorithm evicAlgorithm = aRegion.getAttributes()
        .getEvictionAttributes().getAlgorithm();

    if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY) {
      Log.getLogWriter().info("Eviction algorithm is LRU ENTRY");
      numEvicted = getNumLRUEvictions();
      Log.getLogWriter().info("Evicted numbers :" + numEvicted);
      if (numEvicted == 0) {
        throw new TestException("No eviction happened in this test");
      }
    }
    else if (evicAlgorithm == EvictionAlgorithm.LRU_MEMORY) {
      Log.getLogWriter().info("Eviction algorithm is MEMORY LRU");
      numEvicted = getNumMemLRUEvictions();
      Log.getLogWriter().info("Evicted numbers :" + numEvicted);
      if (numEvicted == 0) {
        throw new TestException("No eviction happened in this test");
      }
    }
    else if (evicAlgorithm == EvictionAlgorithm.LRU_HEAP) {
      Log.getLogWriter().info("Eviction algorithm is HEAP LRU");
      numEvicted = getNumHeapLRUEvictions();
      Log.getLogWriter().info("Evicted numbers :" + numEvicted);
      if (numEvicted == 0) {
        throw new TestException("No eviction happened in this test");
      }
    }

    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Iterator iterator = bucketList.iterator();
    long count = 0;
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion localBucket = (BucketRegion)entry.getValue();
      if (localBucket != null) {
        Log.getLogWriter().info(
            "The entries in the bucket " + localBucket.getName()
                + " after eviction " + localBucket.entryCount());
        if (evicAlgorithm == EvictionAlgorithm.LRU_HEAP
            && localBucket.entryCount() == 0) {
          // May happen especially with lower heap (Bug 39715)
          hydra.Log
              .getLogWriter()
              .warning(
                  "Buckets are empty after evictions with local destroy eviction action");
        }
        if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY
            || evicAlgorithm == EvictionAlgorithm.LRU_MEMORY)
          count = count + localBucket.getCounter();
      }

    }
    if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY
        && count != aRegion.getAttributes().getEvictionAttributes()
            .getMaximum())
      throw new TestException("After Eviction total entries in region "
          + aRegion.getName() + " expected= "
          + aRegion.getAttributes().getEvictionAttributes().getMaximum()
          + " but found= " + count);
    else if (evicAlgorithm == EvictionAlgorithm.LRU_MEMORY) {
        int configuredByteLimit = aRegion.getAttributes().getEvictionAttributes().getMaximum() * 1024 * 1024;
        final int ALLOWANCE = HEAVY_OBJECT_SIZE_VAL * HEAVY_OBJECT_SIZE_VAL; // allow 1 extra object
        int upperLimitAllowed = configuredByteLimit + ALLOWANCE;
        int lowerLimitAllowed = aRegion.getAttributes()
            .getEvictionAttributes().getMaximum() * 1024 * 102;
        Log.getLogWriter().info("memlru, configuredByteLimit: " + configuredByteLimit);
        Log.getLogWriter().info("upperLimitAllowed (bytes): " + upperLimitAllowed);
        Log.getLogWriter().info("lowerLimitAllowed (bytes): " + lowerLimitAllowed);
        Log.getLogWriter().info("actual number of bytes: " + count);
        if ((count > upperLimitAllowed) || (count < lowerLimitAllowed)) {
          throw new TestException("After Eviction, configured memLRU bytes = " + configuredByteLimit +
              " total number of bytes in region " + count);
        }
    }
  }

  /**
   * Return the number of LRU evictions that have occurred.
   * 
   * @return The number of LRU evictions, obtained from stats.
   * 
   */
  public static double getNumLRUEvictions() {
    String spec = "* " // search all archives
        + "LRUStatistics "
        + "* " // match all instances
        + "lruEvictions " + StatSpecTokens.FILTER_TYPE
        + "="
        + StatSpecTokens.FILTER_NONE + " " + StatSpecTokens.COMBINE_TYPE
        + "="
        + StatSpecTokens.COMBINE_ACROSS_ARCHIVES
        + " "
        + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
      Log.getLogWriter().info(
          "Getting stats for spec " + spec + " returned null");
      return 0.0;
    }
    double totalEvictions = 0;
    for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
      // Log.getLogWriter().info(stat.toString());
    }
    return totalEvictions;
  }

  /**
   * Return the number of MemLRU evictions that have occurred.
   * 
   * @return The number of MemLRU evictions, obtained from stats.
   * 
   */
  public static double getNumMemLRUEvictions() {
    String spec = "* " // search all archives
        + "LRUStatistics "
        + "* " // match all instances
        + "lruEvictions " + StatSpecTokens.FILTER_TYPE
        + "="
        + StatSpecTokens.FILTER_NONE + " " + StatSpecTokens.COMBINE_TYPE
        + "="
        + StatSpecTokens.COMBINE_ACROSS_ARCHIVES
        + " "
        + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
      Log.getLogWriter().info(
          "Getting stats for spec " + spec + " returned null");
      return 0.0;
    }
    double totalEvictions = 0;
    for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
    }
    return totalEvictions;
  }

  /**
   * Return the number of HeapLRU evictions that have occurred.
   * 
   * @return The number of HeapLRU evictions, obtained from stats.
   * 
   */
  public static double getNumHeapLRUEvictions() {
    String spec = "* " // search all archives
        + "HeapLRUStatistics "
        + "* " // match all instances
        + "lruEvictions " + StatSpecTokens.FILTER_TYPE
        + "="
        + StatSpecTokens.FILTER_NONE + " " + StatSpecTokens.COMBINE_TYPE
        + "="
        + StatSpecTokens.COMBINE_ACROSS_ARCHIVES
        + " "
        + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
      Log.getLogWriter().info(
          "Getting stats for spec " + spec + " returned null");
      return 0.0;
    }
    double totalEvictions = 0;
    for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
    }
    return totalEvictions;
  }

  public static void verifyEvictionBucketFairness() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyEvictionBucketFairness(pr);
    }
  }

  protected static void verifyEvictionBucketFairness(PartitionedRegion aRegion) {
    if (aRegion.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
          "This is an accessor and hence eviction need not be verified");
      return;
    }

    long numOverflowToDisk = aRegion.getDiskRegionStats()
        .getNumOverflowOnDisk();
    Set bucketSet = aRegion.getDataStore().getAllLocalBuckets();
    int averageEvictionPerBucket = (int)numOverflowToDisk / (bucketSet.size());

    Log.getLogWriter().info(
        "averageEvictionPerBucket = " + averageEvictionPerBucket);
    Iterator iterator = bucketSet.iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion localBucket = (BucketRegion)entry.getValue();
      if (localBucket != null) {
        long entriesInVmForBucket = localBucket.getNumEntriesInVM();
        long entriesInDiskForBucket = localBucket.getNumOverflowOnDisk();

        // Checking with 25% tolerance on both sides
        if (entriesInDiskForBucket > averageEvictionPerBucket * 1.40
            || entriesInDiskForBucket < averageEvictionPerBucket * 0.75) {
          throw new TestException("For the region " + aRegion.getName()
              + " average expected eviction per bucket is "
              + averageEvictionPerBucket + " but for bucket id "
              + localBucket.getId() + " num evicted is "
              + entriesInDiskForBucket);
        }
        else {
          Log.getLogWriter().info(
              "For the region " + aRegion.getName() + " for bucket id "
                  + localBucket.getId() + " num evicted is "
                  + entriesInDiskForBucket
                  + " which is within the expected limit");
        }

      }
    }

  }

  protected void verifyPrimaryBucketCoLocation(PartitionedRegion aRegion) {

    String regionName = aRegion.getName();
    ArrayList primaryBucketList = (ArrayList)aRegion
        .getLocalPrimaryBucketsListTestOnly();
    Log.getLogWriter().info(
        "The primary buckets in this data Store for the Partioned Region "
            + regionName);

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
      Log
          .getLogWriter()
          .info(
              "Reference primary buckets in the Data Store for this vm already set");
      Log.getLogWriter().info(" Verifying for the region " + regionName);

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
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + currentPrimaryBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + currentPrimaryBucket + " colocated"));
        }
      }

      Log.getLogWriter().info("Looking for missed buckets");

      iterator = primaryBucketsInTheDataStore.iterator();

      while (iterator.hasNext()) {
        Integer referenceRegionBucket = (Integer)iterator.next();
        if (primaryBucketList.contains(referenceRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the primary bucket "
                  + referenceRegionBucket + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its primary bucket " + referenceRegionBucket
              + " colocated"));
          errorMsgs
              .add(("Region " + regionName
                  + " does not have its primary bucket "
                  + referenceRegionBucket + " colocated"));
        }
      }
    }

  }

  protected void verifyBucketCoLocation(PartitionedRegion aRegion) {
    String regionName = aRegion.getName();
    ArrayList bucketList = (ArrayList)aRegion.getLocalBucketsListTestOnly();
    Log.getLogWriter()
        .info(
            "The buckets in this data Store for the Partioned Region "
                + regionName);

    if (bucketList == null) {
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
        "Buckets of " + aRegion.getName() + " " + bucketList.toString());
    if (bucketsInTheDataStore == null) {
      Log
          .getLogWriter()
          .info(
              " Setting the reference buckets in the Data Store for this vm with the Partitioned Region "
                  + aRegion.getName());
      bucketsInTheDataStore = bucketList;
      bucketReferenceRegion = regionName;
    }
    else {
      Log
          .getLogWriter()
          .info(
              "Reference primary buckets in the Data Store for this vm already set");
      Log.getLogWriter().info(" Verifying for the region " + regionName);

      Iterator iterator = bucketList.iterator();

      while (iterator.hasNext()) {
        Integer currentRegionBucket = (Integer)iterator.next();
        if (bucketsInTheDataStore.contains(currentRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the bucket " + currentRegionBucket
                  + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + currentRegionBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + currentRegionBucket + " colocated"));
        }
      }

      Log.getLogWriter().info("Looking for missed buckets");

      iterator = bucketsInTheDataStore.iterator();

      while (iterator.hasNext()) {
        Integer referenceRegionBucket = (Integer)iterator.next();
        if (bucketList.contains(referenceRegionBucket)) {
          Log.getLogWriter().info(
              "Both the Regions " + bucketReferenceRegion + " and "
                  + regionName + " have the bucket " + referenceRegionBucket
                  + " in this node");
        }
        else {
          errorException.add(new TestException("Region " + regionName
              + " does not have its bucket " + referenceRegionBucket
              + " colocated"));
          errorMsgs.add(("Region " + regionName + " does not have its bucket "
              + referenceRegionBucket + " colocated"));
        }
      }
    }
  }

  public void verifyPRWhenOneNodeDown() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      RegionAttributes attr = aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      int redundantCopies = prAttr.getRedundantCopies();
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyPR(pr, redundantCopies - 1);
    }
  }

  public void verifyPR() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      RegionAttributes attr = aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      int redundantCopies = prAttr.getRedundantCopies();
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      verifyPR(pr, redundantCopies);
    }

  }

  public void verifyPR(PartitionedRegion aRegion, int redundantCopies) {

    // verifying the PR metadata
    try {
      ParRegUtil.verifyPRMetaData(aRegion);
    }
    catch (Exception e) {
      throw new TestException(e.getMessage());
    }
    catch (TestException e) {
      throw new TestException(e.getMessage());
    }

    // verifying the primaries
    try {
      ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
    }
    catch (Exception e) {
      throw new TestException(e.getMessage());
    }
    catch (TestException e) {
      throw new TestException(e.getMessage());
    }

    // verify PR data
    // try {
    // ParRegUtil.verifyBucketCopies(aRegion, redundantCopies);
    // }catch (Exception e) {
    // throw new TestException(e.getMessage());
    // }catch (TestException e) {
    // throw new TestException(e.getMessage());
    // }
  }

  protected static void verifyMemLRU(PartitionedRegion aRegion) {
    long entriesInVm = aRegion.getDiskRegionStats().getNumEntriesInVM();
    long approxBytesPerEntry = HEAVY_OBJECT_SIZE_VAL * HEAVY_OBJECT_SIZE_VAL;
    double approxBytesInVm = entriesInVm * approxBytesPerEntry;

    long maxPermissibleBytes = aRegion.getAttributes().getEvictionAttributes()
        .getMaximum() * 1024 * 1024;

    if (approxBytesInVm > maxPermissibleBytes) {
      throw new TestException("Permissible Bytes for this region (memLRU) is "
          + maxPermissibleBytes + " but approx bytes in VM is "
          + approxBytesInVm);
    }
    else {
      Log.getLogWriter().info(
          "Permissible Bytes for this region (memLRU) is "
              + maxPermissibleBytes + " but approx bytes in VM is "
              + approxBytesInVm);
    }
  }

  public static void HydraTask_sleepForSomeTime() {
    hydra.MasterController.sleepForMs(180000);
  }

  public static void HydraTask_makeReadyToKill() {
    ParRegBB.getBB().getSharedMap().put("ReadyToKill", new Boolean(true));
  }

  public static void HydraTask_disconnect() {
     DistributedSystemHelper.disconnect();
     testInstance = null;
     primaryBucketsInTheDataStore = null;
  }

  public static void HydraTask_logRegionSizes() {
     Set<Region<?,?>> regSet = CacheHelper.getCache().rootRegions();
     for (Region aRegion: regSet) {
       Log.getLogWriter().info("Verifying region " + aRegion.getFullPath() + " of size " + aRegion.size());
     }
  }
}
