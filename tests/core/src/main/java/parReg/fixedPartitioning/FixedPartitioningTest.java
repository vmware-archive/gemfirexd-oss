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

import getInitialImage.InitImageBB;
import getInitialImage.InitImagePrms;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ConfigPrms;
import hydra.FixedPartitionBlackboard;
import hydra.Log;
import hydra.PartitionPrms;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.ParRegUtil;
import parReg.colocation.Month;
import parReg.colocation.ParRegColocation;
import parReg.execute.FunctionServiceTest;
import parReg.execute.KeysOperationsFunction;
import parReg.execute.LocalDataSetFunctionPrimaryExecute;
import parReg.execute.LocalDataSetFunctionPrimaryExecuteFalse;
import parReg.execute.MemberResultsCollector;
import parReg.execute.PartitionObjectHolder;
import parReg.execute.PrimaryExecutionFunction;
import parReg.execute.RegionOperationsFunction;
import util.KeyIntervals;
import util.NameBB;
import util.NameFactory;
import util.PRObserver;
import util.RandomValues;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;

import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;

public class FixedPartitioningTest extends FunctionServiceTest {

  protected static Long dataStoreSequenceId;

  public final String PRIMARY_PARTITIONS = "primary_partitions_";

  public final String SECONDARY_PARTITIONS = "secondary_partitions_";

  public final String PRIMARY_BUCKETS = "primary_buckets_";

  public final String ALL_BUCKETS = "all_buckets_";

  public final static String DATASTORE_SEQUENCE_ID = "dataStore_sequence_id_";

  public static Region rootRegion;

  // ------------------- STARTTASKS -----------------------------------------//
  /**
   * Initialize the known keys for this test
   */
  public static void StartTask_initialize() {
    // Initialize the known keys
    ParRegColocation.StartTask_initialize();

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

  /**
   * Hydra task for initializing a data store in peer to peer configuration
   */
  public synchronized static void HydraTask_p2p_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FixedPartitioningTest();
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
      testInstance = new FixedPartitioningTest();
      isBridgeClient = false;
      ((FixedPartitioningTest)testInstance).initialize("accessor");
    }
  }

  /**
   * Hydra task for initializing a data store in HA tests
   */
  public synchronized static void HydraTask_HA_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new FixedPartitioningTest();
      isBridgeClient = false;
      setDataStoreSequenceId();
      String key = VmIDStr + RemoteTestModule.getMyVmid();
      String cacheXmlFile = key + ".xml";
      File aFile = new File(cacheXmlFile);
      if (!aFile.exists()) {
        theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
        BridgeHelper.startBridgeServer("bridge");  
      }
      ((FixedPartitioningTest)testInstance).initialize("dataStore");
      ParRegBB.getBB().getSharedMap().put(
          DataStoreVmStr + RemoteTestModule.getMyVmid(),
          new Integer(RemoteTestModule.getMyVmid()));
      isBridgeConfiguration = true;
    }
  }

  /**
   * Hydra task for initializing an accessor in HA tests
   */
  public synchronized static void HydraTask_HA_accessorInitialize() {
    if (testInstance == null) {
      testInstance = new FixedPartitioningTest();
      ((FixedPartitioningTest)testInstance).initialize("accessor");
      Pool pool = PoolHelper.createPool("edgeDescript");
      isBridgeConfiguration = true;
    }
  }
  
  public static void HydraTask_loadRegions() {
    ((FixedPartitioningTest)testInstance).loadRegions();
  }

  /**
   * Task to update BB with the primary and secondary partitions/buckets for the
   * region on the member
   */
  public static void HydraTask_updateBBWithPartitionInfo() {
    ((FixedPartitioningTest)testInstance).updateBBWithPartitionInfo();
  }

  /**
   * Hydra task to populate the regions using function execution
   */
  public static void HydraTask_loadRegionsWithFuncExec() {
    ((FixedPartitioningTest)testInstance).loadRegionsWithFuncExec();
  }

  /**
   * Task for clients to register interest in all regions
   */
  public synchronized static void HydraTask_registerInterest() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      ParRegUtil.registerInterest(aRegion);
      Log.getLogWriter().info(
          "registered interest for the region " + aRegion.getName());
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
      ((FixedPartitioningTest)testInstance).stopStartVms();
      long timeToStop = ParRegBB.getBB().getSharedCounters().read(
          ParRegBB.TimeToStop);
      if (timeToStop > 0) { // ops have ended
        throw new StopSchedulingTaskOnClientOrder("Ops have completed");
      }
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }

  /**
   * Hydra task to stop/start numVmsToStop at a time.
   */
  public static void HydraTask_recycleClientAndValidateBehavior() {
    ((FixedPartitioningTest)testInstance).recycleClientAndValidateBehavior();
  }

  public static void HydraTask_verifyPRMetaData() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      String regionName = aRegion.getName();
      if (aRegion instanceof PartitionedRegion) {
        verifyPRMetaData(aRegion);
        Log.getLogWriter().info(
            "verified Meta Data for the region " + regionName);
      }
      else {
        Log
            .getLogWriter()
            .info(
                "The region "
                    + regionName
                    + " is not partitioned region and hence need not verify metadata");
      }
    }
  }

  /**
   * Hydra task to verify the size of the region after the load.
   */
  public static void HydraTask_verifyRegionSize() {
    ((FixedPartitioningTest)testInstance).Task_verifyRegionSize();
  }

  /**
   * Hydra task to verify the size of the region after the load.
   */
  public void Task_verifyRegionSize() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      verifyRegionSize(aRegion);
      Log.getLogWriter().info(
          "verified Region size for the region " + aRegion.getName());
    }
  }

  /**
   * Initialize this test instance and create region(s)
   */
  public void initialize(String regionDescriptName) {
    numNewKeys = TestConfig.tab().intAt(InitImagePrms.numNewKeys, -1);
    keyIntervals = (KeyIntervals)(InitImageBB.getBB().getSharedMap()
        .get(InitImageBB.KEY_INTERVALS));
    Log.getLogWriter().info(
        "initInstance, keyIntervals read from blackboard = "
            + keyIntervals.toString());
    int numDestroyed = keyIntervals.getNumKeys(KeyIntervals.DESTROY);
    int numKeyIntervals = keyIntervals.getNumKeys();
    totalNumKeys = numKeyIntervals + numNewKeys - numDestroyed;
    sc = InitImageBB.getBB().getSharedCounters();
    randomValues = new RandomValues();
    Log.getLogWriter().info("numKeyIntervals is " + numKeyIntervals);
    Log.getLogWriter().info("numNewKeys is " + numNewKeys);
    Log.getLogWriter().info("numDestroyed is " + numDestroyed);
    Log.getLogWriter().info("totalNumKeys is " + totalNumKeys);
    destroyThreshold = (int)(regionDescriptNames.size() * 0.8);
    Log.getLogWriter().info("destroyThreshold is " + destroyThreshold);
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
        String regionName;    
        if (rName.startsWith(regionDescriptName)) {
          regionName = RegionHelper.getRegionDescription(rName).getRegionName();
          RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
        }
        else if (rName.equalsIgnoreCase("rootRegion")
            && regionDescriptName.equalsIgnoreCase("dataStore")) {
          regionName = RegionHelper.getRegionDescription(rName).getRegionName();
          rootRegion = (Region)RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
          rootRegion.createSubregion("subRegion", RegionHelper
              .getRegionAttributes("subRegion"));
        }
        else if (rName.equalsIgnoreCase("aRootRegion")
            && regionDescriptName.equalsIgnoreCase("accessor")) {
          regionName = RegionHelper.getRegionDescription(rName).getRegionName();
          rootRegion = RegionHelper.createRegion(regionName, RegionHelper
              .getRegionAttributes(rName));
          rootRegion.createSubregion("subRegion", RegionHelper
              .getRegionAttributes("aSubRegion"));   
        }
      }
      ((FunctionServiceTest)testInstance).registerFunctions();
      createXmlFile(aFile);
    }
    else {
      createCacheFromXml(cacheXmlFile);
    }
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
   * Create cache from the xml file
   */
  public void createCacheFromXml(String cacheXmlFile) {
    hydra.Log.getLogWriter().info(
        "Creating cache using the xml file " + cacheXmlFile);
    theCache = CacheHelper.createCacheFromXml(cacheXmlFile);
  }

  /**
   * Task that registers functions
   */
  public void registerFunctions() {
    FunctionService.registerFunction(new RegionOperationsFunction());
    FunctionService.registerFunction(new KeysOperationsFunction());
    FunctionService.registerFunction(new LocalDataSetFunctionPrimaryExecute());
    FunctionService
        .registerFunction(new LocalDataSetFunctionPrimaryExecuteFalse());
    FunctionService.registerFunction(new PrimaryExecutionFunction());
  }

  protected void loadRegions() {
    final long LOG_INTERVAL_MILLIS = 10000;
    int numKeysToCreate = keyIntervals.getNumKeys();
    long lastLogTime = System.currentTimeMillis();
    long minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec, -1);
    long minTaskGranularityMS = -1;
    if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec
          * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    do {
      long shouldAddCount = sc.incrementAndRead(InitImageBB.SHOULD_ADD_COUNT);
      if (shouldAddCount > numKeysToCreate) {
        String aStr = "In loadRegion, for Region shouldAddCount is "
            + shouldAddCount + ", numOriginalKeysCreated is "
            + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
            + ", numKeysToCreate is " + numKeysToCreate;
        Log.getLogWriter().info(aStr);
        NameBB.getBB().printSharedCounters();
        throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getNextPositiveObjectName();
      try {

        Object value = getValueToAdd(key);
        Month callBackArg = Month.months[((int)(shouldAddCount % 12))];
        Log.getLogWriter().info(
            "Callback arg is " + callBackArg.toString()
                + " which has hashCode " + callBackArg.hashCode());
        InitImageBB.getBB().getSharedMap().put(key, callBackArg);
        //Set<Region<?, ?>> regionSet = theCache.rootRegions();
        Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
            .getTestRegions();
        for (Region aRegion : regionSet) {
          Log.getLogWriter().info("Region is :" + aRegion);
          Log.getLogWriter().info(
              "Doing put in region " + aRegion.getName() + "for key value "
                  + key.toString() + " " + value.toString());
          aRegion.put(key, value);
          Log.getLogWriter().info(
              "The region size is for " + aRegion.getName() + " is "
                  + aRegion.size());
        }
        // Log.getLogWriter().info("Loading with put, key " + key + ", value " +
        // TestHelper.toString(value));
        sc.increment(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheWriterException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        // Log.getLogWriter().info("Added " +
        // sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED) + " out of " +
        // numKeysToCreate +
        // " entries into " + TestHelper.regionToString(aRegion, false));
        Log.getLogWriter().info(
            "Added " + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
                + " out of " + numKeysToCreate);
        lastLogTime = System.currentTimeMillis();
      }
    } while ((minTaskGranularitySec == -1)
        || (System.currentTimeMillis() - startTime < minTaskGranularityMS));
  }

  /**
   * Method to load regions using function execution
   */
  protected void loadRegionsWithFuncExec() {
    final long LOG_INTERVAL_MILLIS = 10000;
    int numKeysToCreate = keyIntervals.getNumKeys();
    long lastLogTime = System.currentTimeMillis();
    long minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec, -1);
    long minTaskGranularityMS = -1;
    if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec
          * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    do {
      long shouldAddCount = sc.incrementAndRead(InitImageBB.SHOULD_ADD_COUNT);
      if (shouldAddCount > numKeysToCreate) {
        String aStr = "In loadRegion, for Region shouldAddCount is "
            + shouldAddCount + ", numOriginalKeysCreated is "
            + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
            + ", numKeysToCreate is " + numKeysToCreate;
        Log.getLogWriter().info(aStr);
        NameBB.getBB().printSharedCounters();
        throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getNextPositiveObjectName();
      try {

        Object value = getValueToAdd(key);
        if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
            false)) {
          Month callBackArg = Month.months[((int)(shouldAddCount % 12))];
          Log.getLogWriter().info(
              "Callback arg is " + callBackArg.toString()
                  + " which has hashCode " + callBackArg.hashCode());
          InitImageBB.getBB().getSharedMap().put(key, callBackArg);
          //Set<Region<?, ?>> regionSet = theCache.rootRegions();
          Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
              .getTestRegions();
          for (Region aRegion : regionSet) {
            Log.getLogWriter().info(
                "Doing put in region " + aRegion.getName() + " for key value "
                    + key.toString() + " " + value.toString());
            // aRegion.put(key, value);
            addNewKeyFunction(aRegion, key);
            Log.getLogWriter().info(
                "The region size is " + aRegion.size() + " region is "
                    + aRegion.getName());
          }
        }
        // Log.getLogWriter().info("Loading with put, key " + key + ", value " +
        // TestHelper.toString(value));
        sc.increment(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
      }
      catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheWriterException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        // Log.getLogWriter().info("Added " +
        // sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED) + " out of " +
        // numKeysToCreate +
        // " entries into " + TestHelper.regionToString(aRegion, false));
        Log.getLogWriter().info(
            "Added " + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
                + " out of " + numKeysToCreate);
        lastLogTime = System.currentTimeMillis();
      }
    } while ((minTaskGranularitySec == -1)
        || (System.currentTimeMillis() - startTime < minTaskGranularityMS));
  }

  /**
   * Task to put the PR key set in the BB so they can be used by the client vms
   */
  public static void HydraTask_putKeySetInBB() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      boolean isKeySetInBB = testInstance.putKeySetInBB(aRegion);
      if (isKeySetInBB) {
        return;
      }
    }
  }
  
  /**
   * HydraTask to do fire and forget function executions
   */
  public static void HydraTask_doFireAndForgetExecutions() {
    FixedPartitioningTest.doOnRegionFireAndForgetExecutions();
    if (isBridgeConfiguration) {
      FunctionServiceTest.doFireAndForgetServerExecutions();
    }
    else {
      FunctionServiceTest.doFireAndForgetMemberExecutions();
    }
  }

  /**
   * Task to do onRegion() fire and forget function executions
   */
  public static void doOnRegionFireAndForgetExecutions() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();

    for (Region<?, ?> aRegion : regionSet) {
      Execution dataSet = FunctionService.onRegion(aRegion);
      final Set<Object> keySet = new HashSet<Object>();

      if (System.currentTimeMillis() % 2 == 0) {
        hydra.Log.getLogWriter().info(
            "Doing onRegion() fire and forget execution using single key");
        Object key = NameFactory.getNextPositiveObjectName();
        Month callBackArg = Month.months[TestConfig.tab().getRandGen().nextInt(
            11)];
        InitImageBB.getBB().getSharedMap().put(key, callBackArg);
        keySet.add(key);
      }
      else {
        hydra.Log.getLogWriter().info(
            "Doing onRegion() fire and forget execution using multiple keys");
        Object key1 = NameFactory.getNextPositiveObjectName();
        Object key2 = NameFactory.getNextPositiveObjectName();
        Object key3 = NameFactory.getNextPositiveObjectName();
        Object key4 = NameFactory.getNextPositiveObjectName();

        Month callBackArg1 = Month.months[TestConfig.tab().getRandGen()
            .nextInt(11)];
        Month callBackArg2 = Month.months[TestConfig.tab().getRandGen()
            .nextInt(11)];
        Month callBackArg3 = Month.months[TestConfig.tab().getRandGen()
            .nextInt(11)];
        Month callBackArg4 = Month.months[TestConfig.tab().getRandGen()
            .nextInt(11)];

        InitImageBB.getBB().getSharedMap().put(key1, callBackArg1);
        InitImageBB.getBB().getSharedMap().put(key2, callBackArg2);
        InitImageBB.getBB().getSharedMap().put(key3, callBackArg3);
        InitImageBB.getBB().getSharedMap().put(key4, callBackArg4);

        keySet.add(key1);
        keySet.add(key2);
        keySet.add(key3);
        keySet.add(key4);
      }

      try {
        dataSet.withFilter(keySet).execute(new FunctionAdapter() {
          public void execute(FunctionContext context) {
            if (context instanceof RegionFunctionContext) {
              RegionFunctionContext prContext = (RegionFunctionContext)context;
              Log
                  .getLogWriter()
                  .info(
                      "Inside FireAndForget PartitionedRegionFunctionContext execute");
              Set keySet = prContext.getFilter();
              Log.getLogWriter().info("got the filer set " + keySet.toString());
              PartitionedRegion pr = (PartitionedRegion)prContext.getDataSet();

              Iterator iterator = keySet.iterator();
              while (iterator.hasNext()) {
                Object key = iterator.next();
                RandomValues randomValues = new RandomValues();
                Object value = new ValueHolder((String)key, randomValues);
                pr.put(key, value);
                Log.getLogWriter().info("Did put using execute..");
              }
            }
          }

          public String getId() {
            return "" + hashCode();
          }

          public boolean hasResult() {
            return false;
          }

          public boolean optimizeForWrite() {
            return true;
          }

          public boolean isHA() {
            return false;
          }
        });

      }
      catch (Exception e) {
        e.printStackTrace();
        throw new TestException("Caught the exception during execute " + e);
      }
    }
  }

  // -----------------------TASKS ---------------------------------------//

  /**
   * Hydra task to execute Random region Functions
   */
  public static void HydraTask_doRandomFuncExec() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      doRandomFunctionExecutions(aRegion);
      Log.getLogWriter().info(
          "Random function execution on the region " + aRegion.getName());
    }

    long timeToStop = ParRegBB.getBB().getSharedCounters().read(
        ParRegBB.TimeToStop);
    if (timeToStop > 0) { // ops have ended
      throw new StopSchedulingTaskOnClientOrder("Ops have completed");
    }
  }

  /**
   * Hydra task to do region operations, then stop scheduling.
   */
  public static void HydraTask_doOps() {
    BitSet availableOps = new BitSet(operations.length);
    availableOps.flip(FIRST_OP, LAST_OP + 1);
    ((FixedPartitioningTest)testInstance).doOps(availableOps);
    Log.getLogWriter().info("Cardinality is " + availableOps.cardinality());
    if (availableOps.cardinality() == 0) {
      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.TimeToStop);
      throw new StopSchedulingTaskOnClientOrder("Finished with ops");
    }
  }
  

  // ------------------OPS ------------------------------------//

  /**
   * Add a new key to REGION_NAME.
   * 
   * @return true if all new keys have been added (specified by
   *         InitImagePrms.numNewKeys)
   */
  protected boolean addNewKey() {

    long numNewKeysCreated = sc
        .incrementAndRead(InitImageBB.NUM_NEW_KEYS_CREATED);

    if (numNewKeysCreated > numNewKeys) {
      Log.getLogWriter().info(
          "All new keys created; returning from addNewKey()");
      return true;
    }

    Object key = NameFactory.getNextPositiveObjectName();

    if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
        false)) {
      Month callBackArg = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      InitImageBB.getBB().getSharedMap().put(key, callBackArg);
    }

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      addNewKey(aRegion, key);
    }

    return (numNewKeysCreated >= numNewKeys);
  }

  /**
   * Add a new key to REGION_NAME. The method uses PR execute function for
   * addNewKey
   * 
   */
  protected boolean addNewKeyFunction() {
    Log.getLogWriter().info("Inside addNewKeyFunction()");

    long numNewKeysCreated = sc
        .incrementAndRead(InitImageBB.NUM_NEW_KEYS_CREATED);

    if (numNewKeysCreated > numNewKeys) {
      Log.getLogWriter().info(
          "All new keys created; returning from addNewKeyFunction()");
      return true;
    }

    Object key = NameFactory.getNextPositiveObjectName();

    if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
        false)) {
      Month callBackArg = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      InitImageBB.getBB().getSharedMap().put(key, callBackArg);
    }

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      addNewKeyFunction(aRegion, key);

    }
    return (numNewKeysCreated >= numNewKeys);
  }

  /**
   * To invalidate a key to REGION_NAME. The method uses PR execute function for
   * invalidate
   * 
   */
  protected boolean invalidateFunction() {
    Log.getLogWriter().info("Inside invalidateFunction()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_INVALIDATE);
    if (!keyIntervals.keyInRange(KeyIntervals.INVALIDATE, nextKey)) {
      Log
          .getLogWriter()
          .info(
              "All existing keys invalidated; returning from invalidate Function execution");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance).getTestRegions();
    for (Region aRegion : regionSet) {
      invalidateFunction(aRegion, key);
    }
    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.INVALIDATE));
  }

  /**
   * Invalidate a key in region REGION_NAME. The keys to invalidate are
   * specified in keyIntervals.
   * 
   * @return true if all keys to be invalidated have been completed.
   */
  protected boolean invalidate() {

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_INVALIDATE);
    if (!keyIntervals.keyInRange(KeyIntervals.INVALIDATE, nextKey)) {
      Log.getLogWriter().info(
          "All existing keys invalidated; returning from invalidate operation");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      invalidate(aRegion, key);
    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.INVALIDATE));

  }

  /**
   * To locally invalidate a key to REGION_NAME. The method uses PR execute
   * function for local invalidate
   * 
   */
  protected boolean localInvalidateFunction() {
    Log.getLogWriter().info("Inside localInvalidateFunction()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_INVALIDATE);
    if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_INVALIDATE, nextKey)) {
      Log.getLogWriter().info(
          "All local invalidates completed; returning from localInvalidate");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      localInvalidateFunction(aRegion, key);
    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE));

  }

  /**
   * Locally invalidate a key in region REGION_NAME. The keys to locally
   * invalidate are specified in keyIntervals.
   * 
   * @return true if all keys to be locally invalidated have been completed.
   */
  protected boolean localInvalidate() {
    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_INVALIDATE);
    if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_INVALIDATE, nextKey)) {
      Log.getLogWriter().info(
          "All local invalidates completed; returning from localInvalidate");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      localInvalidate(aRegion, key);
    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE));

  }

  /**
   * Destroy key to REGION_NAME. The method uses PR execute function for destroy
   * Key
   * 
   */
  protected boolean destroyFunction() {

    Log.getLogWriter().info("Inside destroyFunction()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_DESTROY);
    if (!keyIntervals.keyInRange(KeyIntervals.DESTROY, nextKey)) {
      Log.getLogWriter().info("All destroys completed; returning from destroy");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance).getTestRegions();
    for (Region aRegion : regionSet) {
      destroyFunction(aRegion, key);
    }
    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.DESTROY));
  }

  /**
   * Destroy a key in region REGION_NAME. The keys to destroy are specified in
   * keyIntervals.
   * 
   * @return true if all keys to be destroyed have been completed.
   */
  protected boolean destroy() {

    Log.getLogWriter().info("Inside destroy()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_DESTROY);
    if (!keyIntervals.keyInRange(KeyIntervals.DESTROY, nextKey)) {
      Log.getLogWriter().info("All destroys completed; returning from destroy");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      destroy(aRegion, key);

    }
    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.DESTROY));
  }

  /**
   * Locally destroy key to REGION_NAME. The method uses PR execute function for
   * destroy Key
   * 
   */
  protected boolean localDestroyFunction() {

    Log.getLogWriter().info("Inside localDestroyFunction()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_DESTROY);
    if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_DESTROY, nextKey)) {
      Log.getLogWriter().info(
          "All local destroys completed; returning from localDestroy");
      return true;
    }

    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      localDestroyFunction(aRegion, key);
    }
    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY));
  }

  /**
   * Locally destroy a key in region REGION_NAME. The keys to locally destroy
   * are specified in keyIntervals.
   * 
   * @return true if all keys to be locally destroyed have been completed.
   */
  protected boolean localDestroy() {
    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_DESTROY);
    if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_DESTROY, nextKey)) {
      Log.getLogWriter().info(
          "All local destroys completed; returning from localDestroy");
      return true;
    }

    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      localDestroy(aRegion, key);
    }
    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY));
  }

  /**
   * Update existing key in REGION_NAME. The method uses PR execute function for
   * updating the value of the Key.
   * 
   */
  protected boolean updateExistingKeyFunction() {

    Log.getLogWriter().info("Inside update Function()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY);
    if (!keyIntervals.keyInRange(KeyIntervals.UPDATE_EXISTING_KEY, nextKey)) {
      Log.getLogWriter().info(
          "All existing keys updated; returning from updateExistingKey");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      updateFunction(aRegion, key);

    }

    return (nextKey >= keyIntervals
        .getLastKey(KeyIntervals.UPDATE_EXISTING_KEY));
  }

  /**
   * Update an existing key in region REGION_NAME. The keys to update are
   * specified in keyIntervals.
   * 
   * @return true if all keys to be updated have been completed.
   */
  protected boolean updateExistingKey() {
    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY);
    if (!keyIntervals.keyInRange(KeyIntervals.UPDATE_EXISTING_KEY, nextKey)) {
      Log.getLogWriter().info(
          "All existing keys updated; returning from updateExistingKey");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      updateExistingKey(aRegion, key);
    }

    return (nextKey >= keyIntervals
        .getLastKey(KeyIntervals.UPDATE_EXISTING_KEY));
  }

  /**
   * get the value of key in REGION_NAME. The method uses PR execute function
   * for getting the value of the Key.
   * 
   */
  protected boolean getFunction() {
    Log.getLogWriter().info("Inside getFunction()");

    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_GET);
    if (!keyIntervals.keyInRange(KeyIntervals.GET, nextKey)) {
      Log.getLogWriter().info("All gets completed; returning from get");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      getFunction(aRegion, key);

    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.GET));
  }

  /**
   * get the value of key in REGION_NAME. The method uses PR execute function
   * for getting the value of the Key.
   * 
   */
  protected boolean get() {
    long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_GET);
    if (!keyIntervals.keyInRange(KeyIntervals.GET, nextKey)) {
      Log.getLogWriter().info("All gets completed; returning from get");
      return true;
    }
    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      get(aRegion, key);
    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.GET));
  }

  /**
   * Task is doing a query on the key (executed on the same keys of get ops)
   * 
   * @return
   */
  protected boolean queryFunction() {

    Log.getLogWriter().info("queryFunction");

    long nextKey = sc.incrementAndRead(InitImageBB.LAST_QUERY);

    if (!keyIntervals.keyInRange(KeyIntervals.GET, nextKey)) {
      Log
          .getLogWriter()
          .info(
              "All query completed (done on same keys of gets) ; returning from query");
      return true;
    }

    Object key = NameFactory.getObjectNameForCounter(nextKey);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      queryFunction(aRegion, key);
    }

    return (nextKey >= keyIntervals.getLastKey(KeyIntervals.GET));
  }

  // ----------------- CLOSETASKS ----------------------------------//
  
  public static void HydraTask_verifyFPRCoLocation() {
    ((FixedPartitioningTest)testInstance).verifyCoLocation();
  }

  /**
   * Task to verify colocation
   */
  protected void verifyCoLocation() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        if (pr.getAttributes().getPartitionAttributes()
            .getFixedPartitionAttributes() != null) {
          verifyBucketCoLocation(pr);
          verifyPrimaryBucketCoLocation(pr);
        }
      }
      else {
        Log
            .getLogWriter()
            .info(
                "The region "
                    + aRegion.getName()
                    + " is not partitioned region and hence need not verify custom partitioning");
      }
    }
  }
  
  /**
   * Task to verify the FPRs does not rebalance but normal PR does. Done by the
   * new node that got added and rebalanced
   */
  public static void HydraTask_verifyRebalanceBehavior() {
    ((FixedPartitioningTest)testInstance).verifyRebalanceBehavior();
  }

  /**
   * Method to verify the FPRs does not rebalance but normal PR does. Done by
   * the new node that got added and rebalanced
   */
  protected void verifyRebalanceBehavior() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();

    for (Region aRegion : regionSet) {
      // true if the PR is a FixedPartitionRegion
      boolean isFPR = false;
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        if (pr.isFixedPartitionedRegion()) {
          isFPR = true;
        }
        else {
          // to check if the the region is colocated with the FPR region
          String colocatedWithReg = pr.getPartitionAttributes()
              .getColocatedWith();
          if (colocatedWithReg != null) {
            PartitionedRegion colWithRegion = (PartitionedRegion)theCache
                .getRegion(colocatedWithReg);
            if (colWithRegion.isFixedPartitionedRegion()) {
              isFPR = true;
            }
          }
        }
        if (isFPR) {
          verifyFPRsDontRebalance(pr);
        }
        else {
          verifyNormalPRsRebalance(pr);
        }
      }
    }

  }

  /**
   * Verify that the FPRs did not get rebalanced to the new node
   * 
   * @param pr
   */
  public void verifyFPRsDontRebalance(PartitionedRegion pr) {
    List<Integer> primaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    List<Integer> allBucketList = pr.getLocalBucketsListTestOnly();

    boolean isRebalanced = false;
    if (!(primaryBucketList == null || primaryBucketList.size() == 0)) {
      isRebalanced = true;
    }
    if (!isRebalanced) {
      if (!(allBucketList == null || allBucketList.size() == 0)) {
        isRebalanced = true;
      }
    }

    if (isRebalanced) {
      throw new TestException(
          "For the Fixed Partitioned Region, "
              + pr.getName()
              + " expected not to rebalance but got rebalanced. Rebalanced primary  "
              + "buckets " + primaryBucketList
              + " and total rebalanced buckets " + allBucketList);
    }
    else {
      hydra.Log.getLogWriter().info(
          "The Fixed Partitioned Region " + pr.getName()
              + " did not rebalance - which is as expected.");
    }
  }

  /**
   * Verify that the FPRs did get rebalanced to the new node
   * 
   * @param pr
   */
  public void verifyNormalPRsRebalance(PartitionedRegion pr) {
    List<Integer> primaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    List<Integer> allBucketList = pr.getLocalBucketsListTestOnly();
    boolean isRebalanced = true;

    if (primaryBucketList == null || primaryBucketList.size() == 0) {
      isRebalanced = false;
    }
    if (isRebalanced) {
      if (allBucketList == null || allBucketList.size() == 0) {
        isRebalanced = false;
      }
    }

    if (!isRebalanced) {
      throw new TestException("For the normal Partitioned Region, "
          + pr.getName()
          + " expected buckets to rebalance but did not rebalance. Primary  "
          + "buckets " + primaryBucketList + " and total rebalanced buckets "
          + allBucketList);
    }
    else {
      hydra.Log.getLogWriter().info(
          "The normal Partitioned Region " + pr.getName()
              + " did rebalance - which is as expected.");
    }
  }
  
  /**
   * Hydra task to verify PR overflow to disk
   */
  public static void HydraTask_verifyOverflowToDisk() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        ((FixedPartitioningTest)testInstance).verifyOverflowToDisk(pr);
      }
    }
  }

  /**
   * Hydra task to verify PR overflow to disk
   */
  protected void verifyOverflowToDisk(PartitionedRegion pr) {

    if (pr.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
          "This is an accessor and hence eviction need not be verified");
      return;
    }
    long numOverflowToDisk = pr.getDiskRegionStats().getNumOverflowOnDisk();

    long entriesInVm = pr.getDiskRegionStats().getNumEntriesInVM();
    long totalEntriesInBuckets = 0;
    Set bucketList = pr.getDataStore().getAllLocalBuckets();
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
        throw new TestException("For the region " + pr.getName()
            + " no eviction happened ");
      }
      else {
        Log.getLogWriter().info(
            "For the region " + pr.getName() + " entries overflown to disk is "
                + numOverflowToDisk);
      }
    }
    Log.getLogWriter().info(
        "For the region " + pr.getName() + " entries in disk = "
            + numOverflowToDisk + " entries in vm = " + entriesInVm
            + "entries in bucket = " + totalEntriesInBuckets);

    if (totalEntriesInBuckets != (numOverflowToDisk + entriesInVm)) {
      throw new TestException(
          "Entries in bucket (actual value "
              + totalEntriesInBuckets
              + " ) is not the same as the sum of entries in disk and entries in vm ( "
              + numOverflowToDisk + " and " + entriesInVm + ")");

    }
  }
  
  
  /**
   * Task to verify the fixed partitioning
   */
  public static void HydraTask_verifyFixedPartitioning() {
    ((FixedPartitioningTest)testInstance).verifyFixedPartitioning();
  }

  /**
   * Method to verify the fixed partitioning of primary and secondary partitions
   * Also whether the partition num buckets is taken care of.
   */
  protected void verifyFixedPartitioning() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        if (pr.getAttributes().getPartitionAttributes()
            .getFixedPartitionAttributes() != null) {
          printVmParitionInfo(pr);
          verifyPrimaryFixedPartitioning(pr);
          verifySecondaryFixedPartitioning(pr);
        }
        else {
          Log.getLogWriter().info(
              "There are no FPA attributes for " + pr.getName());
        }
      }
      else {
        Log
            .getLogWriter()
            .info(
                "The region "
                    + aRegion.getName()
                    + " is not partitioned region and hence need not verify custom partitioning");
      }
    }
  }
  
  
  /**
   * Method to verify the fixed partitioning of the primary partitions
   * 
   * @param pr
   */
  protected void verifyPrimaryFixedPartitioning(PartitionedRegion pr) {
    List<Integer> primaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
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
        if (key instanceof FixedKeyResolver) {
          routingObject = (Month)((FixedKeyResolver)key).getRoutingHint();
        }
        else if (key instanceof PartitionObjectHolder) {
          routingObject = (Month)((PartitionObjectHolder)key).getRoutingHint();
        }
        else {
          routingObject = (Month)InitImageBB.getBB().getSharedMap().get(key);
        }
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

    verifyPartitionNumBuckets(pr, primaryPartitionsForPr, partition2Buckets);
  }

  /**
   * Method to verify the fixed partitioning of the secondary partitions
   * 
   * @param pr
   */
  protected void verifySecondaryFixedPartitioning(PartitionedRegion pr) {
    if (pr.getLocalBucketsListTestOnly() == null
        || pr.getLocalBucketsListTestOnly().size() == 0) {
      hydra.Log.getLogWriter().info("There are no buckets on this member");
      return;
    }

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
        if (key instanceof FixedKeyResolver) {
          routingObject = (Month)((FixedKeyResolver)key).getRoutingHint();
        }
        else if (key instanceof PartitionObjectHolder) {
          routingObject = (Month)((PartitionObjectHolder)key).getRoutingHint();
        }
        else {
          routingObject = (Month)InitImageBB.getBB().getSharedMap().get(key);
        }
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

    verifyPartitionNumBuckets(pr, secondaryPartitionsForPr, partition2Buckets);

  }

  /**
   * Utility method that give list of parititon names from the list of
   * FixedPartitionAttributes
   * 
   * @param fpas
   *                list of FixedPartitionAttributes
   * @return list of partition names (String)
   */
  protected List<String> getPartitionNames(List<FixedPartitionAttributesImpl> fpas) {
    List<String> partitionNames = new ArrayList<String>();
    for (FixedPartitionAttributesImpl fpa : fpas) {
      String partitionName = fpa.getPartitionName();
      partitionNames.add(partitionName);
    }
    return partitionNames;
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
   * Update the BB with the partition and bucket info for the partitioned region
   * on the member
   */
  protected void updateBBWithPartitionInfo() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        if (pr.getAttributes().getPartitionAttributes()
            .getFixedPartitionAttributes() == null) {
          return;
        }
        Map vmInfo = (Map)ParRegBB.getBB().getSharedMap().get(
            RemoteTestModule.getMyVmid());
        vmInfo.put(PRIMARY_PARTITIONS, pr
            .getPrimaryFixedPartitionAttributes_TestsOnly());
        vmInfo.put(SECONDARY_PARTITIONS, pr
            .getSecondaryFixedPartitionAttributes_TestsOnly());
        vmInfo.put(PRIMARY_BUCKETS, pr.getLocalPrimaryBucketsListTestOnly());
        vmInfo.put(ALL_BUCKETS, pr.getLocalBucketsListTestOnly());
        ParRegBB.getBB().getSharedMap().put(RemoteTestModule.getMyVmid(),
            vmInfo);
      }
      else {
        Log
            .getLogWriter()
            .info(
                "The region "
                    + aRegion.getName()
                    + " is not partitioned region and hence need not verify custom partitioning");
      }
    }
  }

  /**
   * Stop and start ParRegPrms.numVMsToStop. Do not return until the VMs have
   * restarted and reinitialized
   */
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
    PRObserver.waitForRebalRecov(targetVms, 1, numRegions, null, null, false);
    Log.getLogWriter().info("Done in FixedPartitioning.stopStartVms()");
  }

  /**
   * Task to recycle data store vm one by one and validate the following 1.
   * Whether the primary status of the primary buckets of crashed vm is taken
   * care by buckets on secondary partition. 2. Only one secondary partition
   * will become primary for all the new primary buckets. 3. If a member
   * crashes, the redundantCopies will not be satisfied by members that does not
   * have partition names mentioned for all the buckets on the member (primary
   * and secondary) 4. When the crashed member comes back, it will regain all
   * the buckets and satisfy their redundancy. 5. The crashed member regains the
   * primary status of the buckets of primary partitions
   * 
   */
  public void recycleClientAndValidateBehavior() {
    int numRegions = CacheHelper.getCache().rootRegions().size();
    PRObserver.initialize();
    List targetVms = new ArrayList();
    List stopModes = new ArrayList();
    List vmList = getDataStoreVms();
    int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size() - 1);

    // Crash the member
    ClientVmInfo targetMember = (ClientVmInfo)vmList.get(randInt);
    targetVms.add(targetMember);
    stopModes.add(TestConfig.tab().stringAt(ParRegPrms.stopModes));
    StopStartVMs.stopVMs(targetVms, stopModes);

    // Validate that only single member will now host the primary buckets
    // of the primary partitions (using getPrimaryOwner method)
    InternalDistributedMember memberAfterCrash = getPrimaryOwner(targetMember);

    // validate that the new member is eligible to become new primary
    // for the buckets. Verify that they do have the secondary partitions
    // mentioned
    isMemberEligibleToHostPrimary(memberAfterCrash, targetMember);

    // Verify that the redundant copies is not satisfied
    verifyRedundantCopiesSatisfied(targetMember, false);

    // Bring back the crashed vms and wait for recovery
    StopStartVMs.startVMs(targetVms);
    PRObserver.waitForRebalRecov(targetVms, 1, numRegions, null, null, false);

    // Get the new member for the primaries of the primaries of the crashed vm
    // (and validate that still there is only one member hosting primary
    // for all those buckets)
    InternalDistributedMember memberAfterRecycle = getPrimaryOwner(targetMember);
    int newMemberVmId = -1;
    int memberAfterCrashVmId = -1;
    try {
      newMemberVmId = RemoteTestModule.Master.getVmid(
          memberAfterRecycle.getHost(), memberAfterRecycle.getVmPid());
      memberAfterCrashVmId = RemoteTestModule.Master.getVmid(
          memberAfterCrash.getHost(), memberAfterCrash.getVmPid());
    }
    catch (Exception e) {
      throw new TestException("Caught Exception ", e);
    }

    // Verify that the crashed vm now grabs back the primary status of the
    // primary buckets
    if (newMemberVmId != targetMember.getVmid()) {
      throw new TestException(
          "The member vm_"
              + memberAfterCrashVmId
              + " did not give back the primary ownership of buckets after the primary node vm_"
              + targetMember.getVmid() + " came back");
    }
    else {
      hydra.Log.getLogWriter().info(
          "Old member was vm_" + memberAfterCrashVmId
              + " and new member is vm_" + targetMember.getVmid());
    }

    // Verify that the redundantCopies are satisfied
    verifyRedundantCopiesSatisfied(targetMember, true);
  }

  /**
   * The method will give the primary member for the buckets of primary
   * partitions of the crashed member. This method will also verify that only
   * one member will always host the primary buckets of a partition.
   * 
   * @param crashedOrRecycledVm -
   *                ClientVMInfo of the crashed/recycled Member
   * @return InternalDistributedMember of the primary node for the primary
   *         partitions of crashed/recycled member.
   * 
   * TODO Aneesh: This method is valid if each member have only one primary
   * partition max.
   */
  public InternalDistributedMember getPrimaryOwner(
      ClientVmInfo crashedOrRecycledVm) {
    hydra.MasterController.sleepForMs(30000);
    int targetVmId = crashedOrRecycledVm.getVmid();
    Map vmInfo = (Map)ParRegBB.getBB().getSharedMap().get(targetVmId);
    List<Integer> primaryBucketsOnVm = (List<Integer>)vmInfo
        .get(PRIMARY_BUCKETS);
    InternalDistributedMember newMember = null;

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      HashSet memberSet = new HashSet();

      for (int bucketId : primaryBucketsOnVm) {
        newMember = pr.getBucketPrimary(bucketId);
        hydra.Log.getLogWriter().info("For the bucketId "+bucketId+" the primary member is "+newMember);
        memberSet.add(newMember);
      }

      // Verify that only one member holds primary status of buckets of primary
      // partition
      if (memberSet.size() != 1) {
        throw new TestException("For the recycled vm "
            + crashedOrRecycledVm.getVmid() + " primary buckets "
            + primaryBucketsOnVm + " for the region " + aRegion.getName()
            + " is hosted by " + memberSet.size() + " members - " + memberSet);
      }
    }
    return newMember;
  }

  /**
   * Method to verify whether the redundantCopies are satisfied or not for the
   * buckets of the crashed/recycled member..
   * 
   * @param crashedOrRecycledMember -
   *                ClientVmInfo of the member recycled.
   * @param isExpectedToBeSatisfied -
   *                Is the test expecting redundantCopies to be satisfied or
   *                not.
   */
  public void verifyRedundantCopiesSatisfied(
      ClientVmInfo crashedOrRecycledMember, boolean isExpectedToBeSatisfied) {
    int targetVmId = crashedOrRecycledMember.getVmid();
    Map vmInfo = (Map)ParRegBB.getBB().getSharedMap().get(targetVmId);
    List<Integer> bucketsOnVm = (List<Integer>)vmInfo.get(ALL_BUCKETS);
    hydra.Log.getLogWriter().info("Buckets on crashed vm are " + bucketsOnVm);

    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      int redundantCopies = pr.getPartitionAttributes().getRedundantCopies();

      for (int bucketId : bucketsOnVm) {
        Set bucketOwners = new HashSet();
        try {
          List aList = pr.getBucketOwnersForValidation(bucketId);
          if (aList.size() == 0) { // there are no buckets for this bucket id
            continue;
          }
          for (int i = 0; i < aList.size(); i++) {
            Object[] tmpArr = (Object[])(aList.get(i));
            InternalDistributedMember member = (InternalDistributedMember)(tmpArr[0]);
            int pid = member.getVmPid();
            Integer vmId = null;
            try {
              vmId = new Integer(RemoteTestModule.Master.getVmid(member.getHost(), pid));
            }
            catch (java.rmi.RemoteException e) {
              throw new TestException(TestHelper.getStackTrace(e));
            }
            bucketOwners.add("vm_" + vmId);
          }
        }
        catch (Exception e) {
          throw new TestException("Caught Exception ", e);
        }

        // If member crashed, one copies of the bucket to be lost. If recycled
        // redundantCopies have to be satisfied
        int expectedNumBucketCopies = isExpectedToBeSatisfied ? redundantCopies + 1
            : redundantCopies;

        if (bucketOwners.size() != expectedNumBucketCopies) {
          if (isExpectedToBeSatisfied) {
            throw new TestException(
                "For Fixed Partitioned Regions, after the crash of the vm_"
                    + crashedOrRecycledMember.getVmid()
                    + " the buckets hosted by the member "
                    + bucketsOnVm
                    + " should have their redundantCopies satisfied \n"
                    + " For the bucket id "
                    + bucketId
                    + " for the region, expected number of copies "
                    + expectedNumBucketCopies
                    + " but found the following number of members hosting the buckets"
                    + bucketOwners.size() + "-" + bucketOwners);
          }
          else {
            throw new TestException(
                "For Fixed Partitioned Regions, after the crash of the vm_"
                    + crashedOrRecycledMember.getVmid()
                    + " the buckets hosted by the member "
                    + bucketsOnVm
                    + " should have their redundantCopies unsatisfied \n"
                    + " For the bucket id "
                    + bucketId
                    + " for the region, expected number of copies "
                    + expectedNumBucketCopies
                    + " but found the following number of members hosting the buckets"
                    + bucketOwners.size() + "-" + bucketOwners);
          }
        }
        else {
          hydra.Log.getLogWriter().info(
              "For the partitioned region " + pr.getName()
                  + " for the bucket id " + bucketId
                  + " of the crashed/recycled vm vm_"
                  + crashedOrRecycledMember.getVmid()
                  + " found the expected number of bucket copies "
                  + expectedNumBucketCopies + "-" + bucketOwners);
        }
      }
    }
  }


  /**
   * Method to verify whether the member (new member) is eligible to host
   * primary for the primary buckets of the crashed member. If the member has
   * the secondary partition for the primary partition of the crashed member
   * specified, it is eligible
   * 
   * @param newMember
   * @param crashedMember
   */
  public void isMemberEligibleToHostPrimary(
      InternalDistributedMember newMember, ClientVmInfo crashedMember) {
    int crashedMemberVmId = crashedMember.getVmid();
    Map vmInfoOfCrashedMember = (Map)ParRegBB.getBB().getSharedMap().get(
        crashedMemberVmId);
    List<FixedPartitionAttributesImpl> primaryPartitionsOfCrashedMember = (List<FixedPartitionAttributesImpl>)vmInfoOfCrashedMember
        .get(PRIMARY_PARTITIONS);

    int newMemberVmId = -1;

    try {
      newMemberVmId = RemoteTestModule.Master.getVmid(newMember.getHost(), newMember.getVmPid());
    }
    catch (Exception e) {
      throw new TestException("Caught exception ", e);
    }

    Map vmInfoOfNewMember = (Map)ParRegBB.getBB().getSharedMap().get(
        newMemberVmId);
    List<FixedPartitionAttributesImpl> secondaryPartitionsOfNewMember = (List<FixedPartitionAttributesImpl>)vmInfoOfNewMember
        .get(SECONDARY_PARTITIONS);

    List<String> crashedMemberPrimaryPartitions = getPartitionNames(primaryPartitionsOfCrashedMember);
    List<String> newMemberSecPartitions = getPartitionNames(secondaryPartitionsOfNewMember);

    if (newMemberSecPartitions.containsAll(crashedMemberPrimaryPartitions)) {
      hydra.Log.getLogWriter().info(
          "New vm vm_" + newMemberVmId + " with secondary partitions "
              + newMemberSecPartitions
              + " is eligible to host primary of crashed member vm_"
              + crashedMemberVmId + " with primary partitions "
              + crashedMemberPrimaryPartitions);
    }
    else {
      throw new TestException("New vm vm_" + newMemberVmId
          + " with secondary partitions " + newMemberSecPartitions
          + " is not eligible to host primary of crashed member vm_"
          + crashedMemberVmId + " with primary partitions "
          + crashedMemberPrimaryPartitions);
    }
  }

  /**
   * Utility method that get the datastore sequence id. Used to get the correct
   * region definition for the member during HA scenarios
   */
  public static void setDataStoreSequenceId() {
    if (ParRegBB.getBB().getSharedMap().get(RemoteTestModule.getMyVmid()) == null) {
      Map vmInfo = new HashMap();
      dataStoreSequenceId = ParRegBB.getBB().getSharedCounters()
          .incrementAndRead(ParRegBB.numOfDataStores);
      vmInfo.put(DATASTORE_SEQUENCE_ID, dataStoreSequenceId);
      ParRegBB.getBB().getSharedMap().put(RemoteTestModule.getMyVmid(), vmInfo);
    }
    else {
      Map vmInfo = (HashMap)ParRegBB.getBB().getSharedMap().get(
          RemoteTestModule.getMyVmid());
      dataStoreSequenceId = (Long)vmInfo.get(DATASTORE_SEQUENCE_ID);
    }
  }

  /**
   * Task to print the partition info on the vm
   * 
   * @param pr
   */
  public void printVmParitionInfo(PartitionedRegion pr) {
    Map vmInfo = (Map)ParRegBB.getBB().getSharedMap().get(
        RemoteTestModule.getMyVmid());
    hydra.Log.getLogWriter().info(
        "Primary buckets for the partitioned region " + pr.getName()
            + " on this member are " + pr.getLocalPrimaryBucketsListTestOnly());
    hydra.Log.getLogWriter().info(
        "Total buckets for the partitioned region " + pr.getName()
            + " on this member are " + pr.getLocalBucketsListTestOnly());
    hydra.Log.getLogWriter().info(
        "Primary partition attributes for the region " + pr.getName()
            + " on this member are " + vmInfo.get(PRIMARY_PARTITIONS));
    hydra.Log.getLogWriter().info(
        "Secondary partition attributes for the region " + pr.getName()
            + " on this member are " + vmInfo.get(SECONDARY_PARTITIONS));
  }

  /**
   * Log the local size of the PR data store
   */
  public synchronized static void HydraTask_logLocalSize() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      Log.getLogWriter().info(
          "Number of entries in this data store: "
              + ParRegUtil.getLocalSize(aRegion));
    }
  }

  /**
   * Hydra task to verify primaries
   */
  public static void HydraTask_verifyPrimaries() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        verifyPrimaries(aRegion);
        Log.getLogWriter().info(
            "verified primaries for the region " + aRegion.getName());
      }
      else {
        Log
            .getLogWriter()
            .info(
                "The region "
                    + aRegion.getName()
                    + " is not partitioned region and hence need not verify primaries");
      }
    }
  }

  /**
   * Hydra task to verify bucket copies.
   */
  public static void HydraTask_verifyBucketCopies() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      testInstance.verifyBucketCopies(aRegion);
    }
  }

  /**
   * Hydra task to verify the contents of the region after all ops.
   * 
   */
  public static void HydraTask_verifyRegionContents() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      testInstance.verifyRegionContents(aRegion);
    }
  }
  
  /**
   * Hydra task to verify whether function execution with optimizeForWrite true
   * executes on the right nodes in case of FPR.
   * 
   */
  public static void HydraTask_onRegionFunctionPrimaryBuckets() {
    //Set<Region<?, ?>> regionSet = theCache.rootRegions();
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      if (aRegion instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        ((FixedPartitioningTest)testInstance)
            .executeFunctionPrimaryBucketsAllBuckets(pr);
        ((FixedPartitioningTest)testInstance)
            .executeFunctionPrimaryBucketsMultipleNodes(pr);
        ((FixedPartitioningTest)testInstance)
            .executeFunctionPrimaryBucketsSingleNode(pr);
      }
    }
  }
  
  /**
   * Hydra task for executing functions on all keys
   */
  public static void HydraTask_executeFunctionAllKeys() {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    for (Region aRegion : regionSet) {
      ((FixedPartitioningTest)testInstance).executeFunctionAllKeys(aRegion);
      ((FixedPartitioningTest)testInstance).executeFunctionAllBuckets(aRegion);
      ((FixedPartitioningTest)testInstance)
          .executeFunctionMultipleNodes(aRegion);
    }
  }

  /**
   * Task to verify executeOnPrimaryBuckets in function execution (all buckets)
   */
  public void executeFunctionPrimaryBucketsAllBuckets(PartitionedRegion aRegion) {
    Execution dataSet = FunctionService.onRegion(aRegion);

    hydra.Log.getLogWriter().info(
        "Checking optimizeForWrite for no filter executions");

    PrimaryExecutionFunction primaryExecuteFunction = new PrimaryExecutionFunction();

    HashMap map = new HashMap();

    try {
      map = (HashMap)dataSet.withCollector(new MemberResultsCollector())
          .execute(primaryExecuteFunction.getId()).getResult();
    }
    catch (Exception e) {
      throw new TestException(
          "Got the exception during execute on primary buckets " + e);
    }

    // Verifying the memberID of addResult
    Iterator iterator = map.entrySet().iterator();
    Map.Entry entry = null;
    String key = null;
    String value = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = (String)entry.getKey();
      value = (String)((ArrayList)entry.getValue()).get(0);
      if (!key.equals(value)) {
        throw new TestException("For the memberID " + key
            + " the result received mentions the executed node ID as " + value);
      }
    }

    hydra.Log
        .getLogWriter()
        .info(
            "Successful completion of executeFunctionPrimaryBucketsAllBuckets and the result is "
                + map);
  }

  /**
   * Task to verify executeOnPrimaryBuckets in function execution (multiple
   * nodes)
   */
  public void executeFunctionPrimaryBucketsMultipleNodes(
      PartitionedRegion aRegion) {
    Execution dataSet = FunctionService.onRegion(aRegion);

    hydra.Log.getLogWriter().info(
        "Checking optimizeForWrite for multiple nodes executions");

    PrimaryExecutionFunction primaryExecuteFunction = new PrimaryExecutionFunction();

    HashMap map;
    Set keySet;

    if (aRegion instanceof PartitionedRegion) {
      keySet = ((PartitionedRegion)aRegion).keys();
    }
    else {
      ArrayList keyList = (ArrayList)parReg.ParRegBB.getBB().getSharedMap()
          .get(KEY_LIST);
      keySet = new HashSet();
      keySet.addAll(keyList);
    }

    try {
      map = (HashMap)dataSet.withFilter(keySet).withCollector(
          new MemberResultsCollector()).execute(primaryExecuteFunction.getId())
          .getResult();
    }
    catch (Exception e) {
      throw new TestException(
          "Got the exception during execute on primary buckets " + e);
    }

    // Verifying the memberID of addResult
    Iterator iterator = map.entrySet().iterator();
    Map.Entry entry = null;
    String key = null;
    String value = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = (String)entry.getKey();
      value = (String)((ArrayList)entry.getValue()).get(0);
      if (!key.equals(value)) {
        throw new TestException("For the memberID " + key
            + " the result received mentions the executed node ID as " + value);
      }
    }

    hydra.Log
        .getLogWriter()
        .info(
            "Successful completion of executeFunctionPrimaryBucketsMultipleNodes and the result is "
                + map);
  }

  /**
   * Task to verify executeOnPrimaryBuckets in function execution (Single node)
   */
  public void executeFunctionPrimaryBucketsSingleNode(PartitionedRegion aRegion) {
    Execution dataSet = FunctionService.onRegion(aRegion);

    PrimaryExecutionFunction primaryExecuteFunction = new PrimaryExecutionFunction();

    HashMap map;
    Set keySet;

    if (aRegion instanceof PartitionedRegion) {
      keySet = ((PartitionedRegion)aRegion).keys();
    }
    else {
      ArrayList keyList = (ArrayList)parReg.ParRegBB.getBB().getSharedMap()
          .get(KEY_LIST);
      keySet = new HashSet();
      keySet.addAll(keyList);
    }

    hydra.Log.getLogWriter().info(
        "Checking optimizeForWrite for single node executions");

    for (Object key : keySet) {
      Set singleKeySet = new HashSet();
      singleKeySet.add(key);

      try {
        map = (HashMap)dataSet.withFilter(singleKeySet).withCollector(
            new MemberResultsCollector()).execute(
            primaryExecuteFunction.getId()).getResult();
      }
      catch (Exception e) {
        throw new TestException(
            "Got the exception during execute on primary buckets " + e);
      }

      // Verifying the memberID of addResult
      Iterator iterator = map.entrySet().iterator();
      Map.Entry entry = null;
      String mapKey = null;
      String value = null;

      while (iterator.hasNext()) {
        entry = (Map.Entry)iterator.next();
        mapKey = (String)entry.getKey();
        value = (String)((ArrayList)entry.getValue()).get(0);
        if (!mapKey.equals(value)) {
          throw new TestException("For the memberID " + mapKey
              + " the result received mentions the executed node ID as "
              + value);
        }
      }
      map.clear();
    }
    hydra.Log.getLogWriter().info(
        "Successful completion of executeFunctionPrimaryBucketsSingleNode");
  }
  
  /**
   * Task for doing onRegions() executions
   */
  public void doOnRegionsExecutions() {
    String regionDescriptName = null;
    HashSet regionSet = ((HashSet)((FixedPartitioningTest)testInstance)
    .getTestRegions());
    doOnRegionsExecutions(regionSet);
  }
  
  /**
   * Utility method to get the set of regions used in the test This may include
   * the normal PRs and FPRs (root regions of subregions are not included
   * though)
   * 
   * @return
   */
  public Set<Region<?, ?>> getTestRegions() {
    Set<Region<?, ?>> rootRegions = theCache.rootRegions();
    Set<Region<?, ?>> testRegions = new HashSet<Region<?, ?>>();

    Vector regionNames = (Vector)TestConfig.tab().get(RegionPrms.regionName);
    for (Iterator iter = regionNames.iterator(); iter.hasNext();) {
      Object region = iter.next();
      // String regionName =
      // RegionHelper.getRegionDescription((String)region).getRegionName();
      /* add subRegion in case of colocated tests */
      String regionName = region.toString();
      Log.getLogWriter().info("RegionName is : " + regionName);
      if (regionName.equalsIgnoreCase("subRegion")) {
        Region aRegion = theCache.getRegion(Region.SEPARATOR + "rootRegion"
            + Region.SEPARATOR + "subRegion");
        testRegions.add(aRegion);
      }
      /* we don't need rootRegion to be a part of region set */
      else if (!regionName.equalsIgnoreCase("rootRegion")) {
        Region aRegion = theCache.getRegion(regionName);
        testRegions.add(aRegion);
      }
    }

    return testRegions;
  }

}
