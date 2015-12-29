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

import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.FixedPartitionBlackboard;
import hydra.Log;
import hydra.PartitionPrms;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import parReg.ParRegBB;
import parReg.colocation.Month;
import parReg.execute.FunctionServiceTest;
import util.NameFactory;
import util.PRObserver;
import util.TestException;

import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class FPREvictionTest extends FixedPartitioningTest {

  protected static final int HEAVY_OBJECT_SIZE_VAL = 500;

  // ------------------- STARTTASKS -----------------------------------------//
  public static void StartTask_initialize() {
    List<FixedPartitionAttributes> fixedPartitions = TestConfig.tab().vecAt(
        hydra.FixedPartitionPrms.partitionNames);
    int redundantCopies = TestConfig.tab().intAt(PartitionPrms.redundantCopies);
    List<FixedPartitionAttributes> secondaryFixedPartitions = new ArrayList<FixedPartitionAttributes>();
    for (int i = 0; i < redundantCopies; i++) {
      secondaryFixedPartitions.addAll(fixedPartitions);
    }
    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "PrimaryPartitions", fixedPartitions);
    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "SecondaryPartitions", secondaryFixedPartitions);
  }
  
  // ------------------ INITTASKS -------------------------------------------//
  /**
   * Hydra task for initializing a data store in peer to peer configuration
   */
  public synchronized static void HydraTask_p2p_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FPREvictionTest();
      isBridgeClient = false;
      setDataStoreSequenceId();
      ((FixedPartitioningTest)testInstance).initialize("dataStore");
      ParRegBB.getBB().getSharedMap().put(
          DataStoreVmStr + RemoteTestModule.getMyVmid(),
          new Integer(RemoteTestModule.getMyVmid()));
    }
  }

  /**
   * Hydra task for initializing an accessor in peer to peer configuration
   */
  public synchronized static void HydraTask_p2p_accessorInitialize() {
    if (testInstance == null) {
      testInstance = new FPREvictionTest();
      isBridgeClient = false;
      ((FixedPartitioningTest)testInstance).initialize("accessor");
    }
  }

  public static void HydraTask_populateRegions() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();

    for (Region aRegion : regionSet) {
      ((FPREvictionTest)testInstance).populateRegion(aRegion);
    }
  }

  /**
   * Initialize this test instance and create region(s)
   */
  public void initialize(String regionDescription) {
    Log.getLogWriter().info("ENTRIES_TO_PUT is " + ENTRIES_TO_PUT);

    String key = VmIDStr + RemoteTestModule.getMyVmid();
    String cacheXmlFile = key + ".xml";
    File aFile = new File(cacheXmlFile);

    if (!aFile.exists()) {
      theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      Vector regionNames = (Vector)TestConfig.tab().vecAt(RegionPrms.names,
          null);
      for (Iterator iter = regionNames.iterator(); iter.hasNext();) {
        String rName = (String)iter.next();
        String regionName = RegionHelper.getRegionDescription(rName)
            .getRegionName();
        if (rName.equalsIgnoreCase("rootRegion")
            && regionDescription.equalsIgnoreCase("dataStore")) {
          rootRegion = RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
          rootRegion.createSubregion("subRegion", RegionHelper
              .getRegionAttributes("subRegion"));
        }
        else if (rName.equalsIgnoreCase("aRootRegion")
            && regionDescription.equalsIgnoreCase("accessor")) {
          rootRegion = RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
          rootRegion.createSubregion("subRegion", RegionHelper
              .getRegionAttributes("aSubRegion"));
        }
        else if (rName.startsWith(regionDescription)) {
          RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
        }
      }
      ((FunctionServiceTest)testInstance).registerFunctions();
      createXmlFile(aFile);
    }
    else {
      createCacheFromXml(cacheXmlFile);
    }
  }

  protected void populateRegion(Region aRegion) {
    Log.getLogWriter().info(
        "Putting " + ENTRIES_TO_PUT + " entries into " + aRegion.getFullPath());

    for (int i = 1; i <= ENTRIES_TO_PUT; i++) {
      Month callBackArg = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      Object keyName = NameFactory.getNextPositiveObjectName();
      FixedKeyResolver key = new FixedKeyResolver(keyName, callBackArg);

      byte[] newVal = new byte[HEAVY_OBJECT_SIZE_VAL * HEAVY_OBJECT_SIZE_VAL];
      // Log.getLogWriter().info("Populating with heavy objects");
      aRegion.put(key, newVal); // for eviction tests
      hydra.MasterController.sleepForMs(10);
    }
  }

  /**
   * Task to verify PR bucket local destroy eviction.
   */
  public static void HydraTask_verifyEvictionLocalDestroy() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();

    for (Region aRegion : regionSet) {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      ((FPREvictionTest)testInstance).verifyEvictionLocalDestroy(pr);
    }
  }

  protected void verifyEvictionLocalDestroy(PartitionedRegion aRegion) {
    if (aRegion.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
          "This is an accessor and hence eviction need not be verified");
      return;
    }

    double numEvicted = 0;

    // TODO: Aneesh. Tune the test to do the following validations
    // EvictionAlgorithm evicAlgorithm = aRegion.getAttributes()
    // .getEvictionAttributes().getAlgorithm();
    //
    // if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY) {
    // Log.getLogWriter().info("Eviction algorithm is LRU ENTRY");
    // numEvicted = ParRegCreateDestroy.getNumLRUEvictions();
    // Log.getLogWriter().info("Evicted numbers :" + numEvicted);
    // if (numEvicted == 0) {
    // throw new TestException("No eviction happened in this test");
    // }
    // }
    // else if (evicAlgorithm == EvictionAlgorithm.LRU_MEMORY) {
    // Log.getLogWriter().info("Eviction algorithm is MEMORY LRU");
    // numEvicted = ParRegCreateDestroy.getNumMemLRUEvictions();
    // Log.getLogWriter().info("Evicted numbers :" + numEvicted);
    // if (numEvicted == 0) {
    // throw new TestException("No eviction happened in this test");
    // }
    // }
    // else if (evicAlgorithm == EvictionAlgorithm.LRU_HEAP) {
    // Log.getLogWriter().info("Eviction algorithm is HEAP LRU");
    // numEvicted = ParRegCreateDestroy.getNumHeapLRUEvictions();
    // Log.getLogWriter().info("Evicted numbers :" + numEvicted);
    // if (numEvicted == 0) {
    // throw new TestException("No eviction happened in this test");
    // }
    // }

    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Iterator iterator = bucketList.iterator();
    long count = 0;
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion localBucket = (BucketRegion)entry.getValue();
      if (localBucket != null) {
        Log.getLogWriter().info(
            "For the bucket " + localBucket.getName() + " evicted entries are "
                + localBucket.getEvictions());
        count += localBucket.getEvictions();
      }

      if (bucketList.size() > 0 && count == 0) {
        throw new TestException("No eviction happened in the test");
      }
    }
    // if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY
    // && count != aRegion.getAttributes().getEvictionAttributes()
    // .getMaximum())
    // throw new TestException("After Eviction total entries in region "
    // + aRegion.getName() + " expected= "
    // + aRegion.getAttributes().getEvictionAttributes().getMaximum()
    // + " but found= " + count);
    // else if (evicAlgorithm == EvictionAlgorithm.LRU_MEMORY
    // && (count > aRegion.getAttributes().getEvictionAttributes()
    // .getMaximum()*1024*1024 || count <
    // aRegion.getAttributes().getEvictionAttributes()
    // .getMaximum()*1024*102 ))
    // throw new TestException("After Eviction total entries in region "
    // + aRegion.getName() + " expected= "
    // + aRegion.getAttributes().getEvictionAttributes().getMaximum()
    //          + " but found= " + count);
  }

}
