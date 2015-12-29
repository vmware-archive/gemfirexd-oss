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
package resumeTx;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import parReg.ParRegUtil;
import pdx.PdxTestVersionHelper;
import util.BaseValueHolder;
import util.KeyIntervals;
import util.NameFactory;
import util.RandomValues;
import util.SummaryLogListener;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;

public class ResumableKnownKeysTest {

  static ResumableKnownKeysTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration = false;
  private boolean isBridgeClient = false;
  protected KeyIntervals keyIntervals;   // the key intervals used for this test; test test does a different
  // operation on each interval so it can be validated at the end
  protected int numNewKeys;              // the number of new keys to add to the region

  // used to make a task run once per vm
  private volatile static boolean verifyRegionsInProgress = false;
  private volatile static boolean verifyLoadInProgress = false;

  // blackboard key
  protected static final String txListKey = "txListForVmId_";

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new ResumableKnownKeysTest();
      testInstance.initializeInstance();
      Vector configNames = ResumeTxPrms.getRegionConfigNames();
      testInstance.createRegions(configNames);
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        registerInterest();
      }
      testInstance.intializeIntervals();
    }
  }


  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ResumableKnownKeysTest();
      testInstance.initializeInstance();
      Vector configNames = ResumeTxPrms.getRegionConfigNames();
      testInstance.createRegions(configNames);
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
      initializeBlackboard();
      testInstance.intializeIntervals();
    }
  } 

  /** Load all root regions with identical data
   * 
   */
  public static void HydraTask_loadRegions() {
    if (TestConfig.tab().getRandGen().nextBoolean()) {
      testInstance.loadRegionsWithPut();
    } else {
      testInstance.loadRegionsWithPutAll();
    }
  }

  /** Task to log all root region sizes
   * 
   */
  public static void HydraTask_logRegionSizes() {
    Log.getLogWriter().info(regionSizesToString());
  }

  /** Verify that each region has the correct keys. It is expected
   *  that all rootRegions contains the same data.
   *  
   *  This executes once per jvm per test run.
   */
  public synchronized static void HydraTask_verifyLoad() {
    if (!verifyLoadInProgress) {
      verifyLoadInProgress = true;
      Log.getLogWriter().info("Verifying load for all regions...");
      int numKeysInIntervals = testInstance.keyIntervals.getNumKeys();
      long nameCounter = NameFactory.getPositiveNameCounter();
      if (nameCounter != numKeysInIntervals) {
        throw new TestException("Expected NameFactory counter " + nameCounter + " to be the same as keyIntervals " + numKeysInIntervals);
      }
      StringBuffer aStr = new StringBuffer();
      for (Region aRegion: CacheHelper.getCache().rootRegions()) {
        int regionSize = aRegion.size();
        if (regionSize != nameCounter) {
          aStr.append(aRegion.getFullPath() + " is size " + regionSize + " but expected it to be " + nameCounter + "\n");
          for (int keyIndex = 1; keyIndex <= nameCounter; keyIndex++) {
            Object key = NameFactory.getObjectNameForCounter(keyIndex);
            if (!aRegion.containsKey(key)) {
              aStr.append(aRegion.getFullPath() + " does not contain expected key " + key + "\n");
            }
          }
        }
      }
      if (aStr.length() > 0) {
        throw new TestException(aStr.toString());
      }
      Log.getLogWriter().info("Done verifying load for " + CacheHelper.getCache().rootRegions());
    }
  }

  /** Wait for silence for server queues to drain
   * 
   */
  public static void HydraTask_waitForSilence() {
    SummaryLogListener.waitForSilence(30, 5000);
  }

  /** This task to be run from an edge client. It executes a function on the servers
   *  to initialize in preparation of doing operations. When the function runs on the
   *  server it writes a list of open TransactionIDs to the blackboard.
   */
  public static void HydraTask_initFunction() {
    ArrayList aList = new ArrayList();
    aList.add(getClientIdString());
    aList.add("initialize");
    Execution exe = null;
    if (testInstance.isBridgeConfiguration) {
      exe = FunctionService.onServers(PoolManager.find("edgePool")).withArgs(aList);
    } else {
      exe = FunctionService.onMembers(DistributedSystemHelper.getDistributedSystem()).withArgs(aList);
    }
    ResultCollector rc = exe.execute(new KnownKeysFcn());
    Log.getLogWriter().info("result is " + rc.getResult());
  }

  /** This task is to be run from an edge client. It executes a function on the servers
   *  to commit a suspended transaction on the server. This should be called repeatedly
   *  until it throw StopSchedulingTaskOnClientOrder.
   */
  /* TODO: TX: needs to be rewritten for new TX impl
  public static void HydraTask_commitWithFunction() {
    Set<TransactionId> txSet = getCurrentTransactions();
    for (TransactionId txId: txSet) {
      Execution exe = TransactionFunctionService.onTransaction(txId);
      ResultCollector rc = exe.execute(new CommitFunction());
      Log.getLogWriter().info("Result from commit for " + txId + " is " + rc.getResult());
    }
  }
  */

  /** This task is to be run from an edge client. It executes a function on the servers
   *  to do ops. This task is workload based and should be run repeatedly until it
   *  throws StopSchedulingTaskOnClientOrder.
   */
  /* TODO: TX: needs to be rewritten for new TX impl
  public static void HydraTask_doOpsWithFunctions() {
    ArrayList aList = new ArrayList();
    aList.add(getClientIdString());
    aList.add("ops");
    TransactionId txId = getRandomTxId();
    aList.add(txId);
    if (txId == null) { // all server transactions have completed ops and committed
      throw new StopSchedulingTaskOnClientOrder("all ops have completed");
    }
    Log.getLogWriter().info("Calling KnownKeysFxn with onTransaction(" + txId + ")");
    Execution exe = TransactionFunctionService.onTransaction(txId).withArgs(aList);
    ResultCollector rc = exe.execute(new KnownKeysFcn());
    Log.getLogWriter().info("Result is " + rc.getResult());
  }
  */

  /** Verifies the contents of all root regions after doing ops.
   *  This executes once per jvm per test run.
   * 
   */
  public static synchronized void HydraTask_verifyRegions() {
    if (!verifyRegionsInProgress) {
      verifyRegionsInProgress = true;
      testInstance.verifyRegions();
    }
  }

  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
  }

  /** Initialize the blackboard. 
   * 
   */
  private static void initializeBlackboard() {
    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(CacheHelper.getCache().getRegion("region1"));
    int numDataStores = info.getPartitionMemberInfo().size();
    ResumeTxBB.getBB().getSharedMap().put(ResumeTxBB.NUM_DATASTORES, numDataStores);
    Log.getLogWriter().info("Num dataStores is " + numDataStores);
  }

  /** Initialize the key intervals for this test
   * 
   */
  private void intializeIntervals() {
    int numIntervalKeys = ResumeTxPrms.getNumIntervalKeys();
    numNewKeys = ResumeTxPrms.getNumNewKeys();
    keyIntervals = new KeyIntervals(new int[] {KeyIntervals.NONE, KeyIntervals.INVALIDATE,
        KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
        KeyIntervals.GET}, 
        numIntervalKeys);
    Log.getLogWriter().info(keyIntervals.toString());
    Log.getLogWriter().info("numNewKeys is " + numNewKeys);
  }

  /** Register interest in all root regions
   * 
   */
  private static void registerInterest() {
    for (Region aRegion: CacheHelper.getCache().rootRegions()) {
      ParRegUtil.registerInterest(aRegion);
    }
  }

  /** Create regions specified in RegionPrms.names
   *
   */
  private void createRegions(Vector regionConfigNames) {
    CacheHelper.createCache("cache1");
    Vector<String> availableNames = TestConfig.tab().vecAt(RegionPrms.names);
    for (String regionConfigName: availableNames) {
      if (regionConfigNames.contains(regionConfigName)) {
        Log.getLogWriter().info("Creating region " + regionConfigName);
        RegionHelper.createRegion(regionConfigName);
      }
    }
  }

  /** Workload based method to load all root regions with identical data. This is
   *  called repeatedly until a StopSchedulingTaskOnClinetOrder exception is thrown.
   */
  public void loadRegionsWithPutAll() {
    final long LOG_INTERVAL_MILLIS = 10000;
    int numKeysToCreate = keyIntervals.getNumKeys();
    long lastLogTime = System.currentTimeMillis();
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
    long minTaskGranularityMS = -1;
    if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    RandomValues rv = new RandomValues();
    Map putAllMap = new HashMap();
    SharedCounters sc = ResumeTxBB.getBB().getSharedCounters();
    do {
      long loadController = sc.incrementAndRead(ResumeTxBB.loadController);
      if (loadController <= numKeysToCreate) {
        Object key = NameFactory.getNextPositiveObjectName();
        Object value = new ValueHolder((String)key, rv);
        putAllMap.put(key, value);
        if (putAllMap.size() == 100) {
          putToAllRegions(putAllMap);
          putAllMap = new HashMap();
        }
      } else {
        if (putAllMap.size() > 0) {
          putToAllRegions(putAllMap);
        }
        String aStr = "In loadRegion, name counter is " + NameFactory.getPositiveNameCounter() + "; done with load";
        Log.getLogWriter().info(aStr);
        throw new StopSchedulingTaskOnClientOrder(aStr);
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        Log.getLogWriter().info("In load, current region sizes: " + regionSizesToString());
        lastLogTime = System.currentTimeMillis();
      }
    } while ((minTaskGranularitySec == -1) ||
        (System.currentTimeMillis() - startTime < minTaskGranularityMS));
    if (putAllMap.size() > 0) {
      putToAllRegions(putAllMap);
    }
  }

  /** Workload based method to load all root regions with identical data. This is
   *  called repeatedly until a StopSchedulingTaskOnClinetOrder exception is thrown.
   */
  public void loadRegionsWithPut() {
    final long LOG_INTERVAL_MILLIS = 10000;
    int numKeysToCreate = keyIntervals.getNumKeys();
    long lastLogTime = System.currentTimeMillis();
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
    long minTaskGranularityMS = -1;
    if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    RandomValues rv = new RandomValues();
    SharedCounters sc = ResumeTxBB.getBB().getSharedCounters();
    do {
      long loadController = sc.incrementAndRead(ResumeTxBB.loadController);
      if (loadController <= numKeysToCreate) {
        Object key = NameFactory.getNextPositiveObjectName();
        Object value = new ValueHolder((String)key, rv);
        for (Region aRegion: CacheHelper.getCache().rootRegions()) {
          //Log.getLogWriter().info("Putting " + key + ", " + TestHelper.toString(value) + " for region " + aRegion.getFullPath());
          aRegion.put(key, value);
        }
      } else {
        String aStr = "In loadRegion, name counter is " + NameFactory.getPositiveNameCounter() + "; done with load";
        Log.getLogWriter().info(aStr);
        throw new StopSchedulingTaskOnClientOrder(aStr);
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        Log.getLogWriter().info("In load, current region sizes: " + regionSizesToString());
        lastLogTime = System.currentTimeMillis();
      }
    } while ((minTaskGranularitySec == -1) ||
        (System.currentTimeMillis() - startTime < minTaskGranularityMS));
  }
  
  /** Return a String logging the size of each available root region
   * 
   * @return A String of regions and their sizes. 
   */
  private static String regionSizesToString() {
    StringBuffer aStr = new StringBuffer();
    for (Region aRegion: CacheHelper.getCache().rootRegions()) {
      aStr.append(aRegion.getFullPath() + " is size " + aRegion.size() + ";");
    }
    return aStr.toString();
  }

  /** For each available root region, do a putAll with the given putAll argument
   * 
   * @param putAllMap The map to use for the putAll
   */
  private void putToAllRegions(Map putAllMap) {
    for (Region aRegion: CacheHelper.getCache().rootRegions()) {
      //Log.getLogWriter().info("Putting putAll map of size " + putAllMap.size() + " into " + aRegion.getFullPath() +
      //    ", keys of putAll map are: " + keySet);
      aRegion.putAll(putAllMap);
    }
  }

  /** Verify the contents of all regions after ops.
   * 
   */
  private void verifyRegions() {
    // get a set of expected keys
    int numKeys = keyIntervals.getNumKeys();
    Set expectedKeys = new HashSet();
    for (int i = 1; i <= numKeys; i++) {
      expectedKeys.add(NameFactory.getObjectNameForCounter(i));
    }
    for (int i = keyIntervals.getFirstKey(KeyIntervals.DESTROY); 
    i <= keyIntervals.getLastKey(KeyIntervals.DESTROY); i++) {
      expectedKeys.remove(NameFactory.getObjectNameForCounter(i));
    }
    for (int i = keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY); 
    i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY); i++) {
      expectedKeys.remove(NameFactory.getObjectNameForCounter(i));
    }
    for (int i = numKeys+1; i <= numKeys+numNewKeys; i++) {
      expectedKeys.add(NameFactory.getObjectNameForCounter(i));
    }

    // verify each region
    for (Region aRegion: CacheHelper.getCache().rootRegions()) {
      verifyRegion(aRegion, expectedKeys);
    }

  }

  /** Verify the contents of a single region after ops.
   * 
   */
  private void verifyRegion(Region aRegion, Set expectedKeys) {
    Log.getLogWriter().info("Verifying " + aRegion.getFullPath() + " of size " + aRegion.size());
    final int logIntervalMs = 10000;
    StringBuffer errStr = new StringBuffer();
    int regionSize = aRegion.size();
    int expectedSize = expectedKeys.size();
    long start = System.currentTimeMillis();
    Set regionKeys = new HashSet(aRegion.keySet());
    long duration = System.currentTimeMillis() - start;
    Log.getLogWriter().info(duration + " ms to get regionKeys for " + aRegion.getFullPath());
    start = System.currentTimeMillis();
    Set missing = new HashSet(expectedKeys);
    missing.removeAll(regionKeys);
    duration = System.currentTimeMillis() - start;
    Log.getLogWriter().info(duration + " ms to get missing set for " + aRegion.getFullPath());
    start = System.currentTimeMillis();
    Set unexpected = new HashSet(regionKeys);
    unexpected.removeAll(expectedKeys);
    duration = System.currentTimeMillis() - start;
    Log.getLogWriter().info(duration + " ms to get unexpected set for " + aRegion.getFullPath());

    if (regionSize != expectedSize) {
      errStr.append("Expected " + aRegion.getFullPath() + " to have size" + expectedSize + " but it is size " + regionSize + "\n");
    }
    if (missing.size() > 0) {
      errStr.append("The following keys were missing in " + aRegion.getFullPath() + ": " + missing + "\n");
    }
    if (unexpected.size() > 0) {
      errStr.append("The following keys were unexpected in " + aRegion.getFullPath() + ": " + unexpected + "\n");
    }

    // iterate keys of the region
    start = System.currentTimeMillis();
    long lastLogTime = System.currentTimeMillis();
    int lastKeyToCheck = keyIntervals.getNumKeys() + numNewKeys;
    for (int i = 1; i <= lastKeyToCheck; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      try {
        if (((i >= keyIntervals.getFirstKey(KeyIntervals.NONE)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.NONE)))    ||
            ((i >= keyIntervals.getFirstKey(KeyIntervals.GET)) &&
                (i <= keyIntervals.getLastKey(KeyIntervals.GET)))) {
          // this key was untouched after its creation
          checkContainsKey(aRegion, key, true, "key was untouched");
          checkContainsValueForKey(aRegion, key, true, "key was untouched");
          Object value = aRegion.get(key);
          checkValue(aRegion, key, value);
        } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
          checkContainsKey(aRegion, key, true, "key was invalidated");
          checkContainsValueForKey(aRegion, key, false, "key was invalidated");
        } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
          // this key was locally invalidated
          checkContainsKey(aRegion, key, true, "key was locally invalidated");
          checkContainsValueForKey(aRegion, key, true, "key was locally invalidated");
          Object value = aRegion.get(key);
          checkValue(aRegion, key, value);
        } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.DESTROY)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
          // this key was destroyed
          checkContainsKey(aRegion, key, false, "key was destroyed");
          checkContainsValueForKey(aRegion, key, false, "key was destroyed");
        } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
          // this key was locally destroyed
          checkContainsKey(aRegion, key, true, "key was locally destroyed");
          checkContainsValueForKey(aRegion, key, true, "key was locally destroyed");
          Object value = aRegion.get(key);
          checkValue(aRegion, key, value);
        } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)) &&
            (i <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
          // this key was updated
          checkContainsKey(aRegion, key, true, "key was updated");
          checkContainsValueForKey(aRegion, key, true, "key was updated");
          Object value = aRegion.get(key);
          checkUpdatedValue(aRegion, key, value);
        } else if (i > keyIntervals.getNumKeys()) {
          // key was newly added
          checkContainsKey(aRegion, key, true, "key was new");
          checkContainsValueForKey(aRegion, key, true, "key was new");
          Object value = aRegion.get(key);
          checkValue(aRegion, key, value);
        }
      } catch (TestException e) {
        Log.getLogWriter().info(TestHelper.getStackTrace(e));
        errStr.append(e.getMessage() + "\n");
      }

      if (System.currentTimeMillis() - lastLogTime > logIntervalMs) {
        Log.getLogWriter().info("Verified key " + i + " out of " + lastKeyToCheck + " in " + aRegion.getFullPath());
        lastLogTime = System.currentTimeMillis();
      }
    }
    duration = System.currentTimeMillis() - start;
    Log.getLogWriter().info(duration + " ms to iterate " + aRegion.getFullPath());

    if (errStr.length() > 0)
      throw new TestException(errStr.toString());
  }

  /** Check that containsKey() called on the given region has the expected result.
   *  Throw an error if any problems.
   *  
   *  @param aRegion The region to check.
   *  @param key The key to check.
   *  @param expected The expected result of containsKey
   *  @param logStr Used if throwing an error due to an unexpected value.
   */
  protected void checkContainsKey(Region aRegion, Object key, boolean expected, String logStr) {
    boolean containsKey = aRegion.containsKey(key);
    if (containsKey != expected)
      throw new TestException("Expected containsKey(" + key + ") to be " + expected + 
          ", but it was " + containsKey + ": " + logStr);
  }

  /** Check that containsValueForKey() called on the given region has the expected result.
   *  Throw an error if any problems.
   *  
   *  @param aRegion The region to check.
   *  @param key The key to check.
   *  @param expected The expected result of containsValueForKey
   *  @param logStr Used if throwing an error due to an unexpected value.
   */
  protected void checkContainsValueForKey(Region aRegion, Object key, boolean expected, String logStr) {
    boolean containsValue = aRegion.containsValueForKey(key);
    if (containsValue != expected)
      throw new TestException("Expected containsValueForKey(" + key + ") to be " + expected + 
          ", but it was " + containsValue + ": " + logStr);
  }

  /** Check that the value of the given key is expected for this test.
   *  Throw an error if any problems.
   *  
   *  @param aRegion The region to check.
   *  @param key The key to check.
   *  @param value The value for the key.
   */
  protected void checkValue(Region aRegion, Object key, Object value) {
    if (value instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)value;
      long keyCounter = NameFactory.getCounterForName(key);
      if (vh.myValue instanceof Long) {
        Long aLong = (Long)vh.myValue;
        long longValue = aLong.longValue();
        if (keyCounter != longValue)
          throw new TestException("Inconsistent ValueHolder.myValue for key " + key + ":" + TestHelper.toString(vh));      
      } else {
        throw new TestException("Expected ValueHolder.myValue for key " + key + " to be a Long for " + TestHelper.toString(vh));      
      }
    } else {
      throw new TestException("For key " + key + ", expected value " + TestHelper.toString(value) + " to be a ValueHolder");
    }
  }

  /** Check that the value of the given key is expected as an updated value.
   *  Throw an error if any problems.
   *  
   *  @param aRegion The region to check.
   *  @param key The key to check.
   *  @param value The value for the key.
   */
  protected void checkUpdatedValue(Region aRegion, Object key, Object value) {
    Object baseObject = PdxTestVersionHelper.toBaseObject(value);
    if (baseObject instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)baseObject;
      long keyCounter = NameFactory.getCounterForName(key);
      if (vh.myValue instanceof String) {
        String aStr = (String)vh.myValue;
        String expectedStr = "updated_" + keyCounter;
        if (!aStr.equals(expectedStr))
          throw new TestException("Inconsistent ValueHolder.myValue for key " + key + ":" + TestHelper.toString(vh));      
      } else {
        throw new TestException("Expected ValueHolder.myValue for key " + key + 
            " to be a String indicating it was updated, but the value for this key is " + 
            TestHelper.toString(vh));      
      }
    } else {
      throw new TestException("Expected value " + TestHelper.toString(baseObject) + " to be a ValueHolder");
    }
  }

  /** Return a randome TransactionId in use on the servers.
   * 
   * @return A TransactionId.
   */
  private static TransactionId getRandomTxId() throws Exception {
    Set<TransactionId> serverTxIds = getCurrentTransactions();
    if (serverTxIds.size() == 0) { // all ops are completed and committed
      return null;
    }
    List<TransactionId> serverTxIdList = new ArrayList(serverTxIds);
    int randInt = TestConfig.tab().getRandGen().nextInt(0, serverTxIdList.size()-1);
    return serverTxIdList.get(randInt);
  }

  /** Return a Set of current transactions available on the blackboard from all
   *  jvms. 
   * @return A system-wide set of transactions. 
   */
  private static Set<TransactionId> getCurrentTransactions() throws Exception {
    Set<TransactionId> serverTxIds = new HashSet();
    Map aMap = ResumeTxBB.getBB().getSharedMap().getMap();
    for (Object key: aMap.keySet()) {
      if ((key instanceof String) && (((String)key).startsWith(txListKey))) {
        serverTxIds.addAll(RtxUtilVersionHelper
            .convertByteArrayListToTransactionIDList((List) (aMap.get(key))));
      }
    }
    return serverTxIds;
  }

  /** Return a String identifying this client jvm and thread to be used to send to a function
   *  useful for analyzing failed runs.
   *  
   * @return
   */
  private static String getClientIdString() {
    return "vm_" + RemoteTestModule.getMyVmid() + "_thr_" + RemoteTestModule.getCurrentThread().getThreadId();
  }

}
