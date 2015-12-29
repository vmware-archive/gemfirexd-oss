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
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.colocation.Month;
import parReg.execute.ArrayListResultCollector;
import parReg.execute.FunctionServiceTest;
import parReg.execute.PartitionObjectHolder;
import parReg.execute.PrimaryExecutionFunction;
import util.BaseValueHolder;
import util.KeyIntervals;
import util.NameBB;
import util.NameFactory;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;

import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;

public class KeyCallbackResolverTest extends FixedPartitioningTest {

  // ------------------ INITTASKS -------------------------------------------//
  /**
   * Hydra task for initializing a data store in peer to peer configuration
   */
  public synchronized static void HydraTask_p2p_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new KeyCallbackResolverTest();
      isBridgeClient = false;
      setDataStoreSequenceId();
      ((KeyCallbackResolverTest)testInstance).initialize("dataStore");
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
      testInstance = new KeyCallbackResolverTest();
      isBridgeClient = false;
      ((KeyCallbackResolverTest)testInstance).initialize("accessor");
    }
  }

  /**
   * Hydra task for initializing a data store in HA tests
   */
  public synchronized static void HydraTask_HA_dataStoreInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new KeyCallbackResolverTest();
      isBridgeClient = false;
      setDataStoreSequenceId();
      String key = VmIDStr + RemoteTestModule.getMyVmid();
      String cacheXmlFile = key + ".xml";
      File aFile = new File(cacheXmlFile);
      if (!aFile.exists()) {
        theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
        BridgeHelper.startBridgeServer("bridge");  
      }
      ((KeyCallbackResolverTest)testInstance).initialize("dataStore");
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
      testInstance = new KeyCallbackResolverTest();
      ((KeyCallbackResolverTest)testInstance).initialize("accessor");
      Pool pool = PoolHelper.createPool("edgeDescript");
      isBridgeConfiguration = true;
    }
  }
  
  /**
   * Hydra task to do onRegion function executions with and without filter
   */
  public static void HydraTask_executeFunctions() {
    if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("key")) {
      FixedPartitioningTest.HydraTask_executeFunctionAllKeys();
    }
    else if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callBackArg")) {
      Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
          .getTestRegions();
      for (Region aRegion : regionSet) {
        ((KeyCallbackResolverTest)testInstance)
            .executeWithCallBackFilter(aRegion);
        ((FunctionServiceTest)testInstance).executeFunctionAllBuckets(aRegion);
      }
    }
  }

  /**
   * Task to load regions
   */
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
        loadRegion(key, value, callBackArg);
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
        Log.getLogWriter().info(
            "Added " + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
                + " out of " + numKeysToCreate);
        lastLogTime = System.currentTimeMillis();
      }
    } while ((minTaskGranularitySec == -1)
        || (System.currentTimeMillis() - startTime < minTaskGranularityMS));
  }

  /**
   * Task to load region based on whether key or callback implements the
   * resolver
   * @param key
   * @param value
   * @param callBackArg
   */
  public void loadRegion(Object key, Object value, Month callBackArg) {
    Set<Region<?, ?>> regionSet = ((FixedPartitioningTest)testInstance)
        .getTestRegions();
    InitImageBB.getBB().getSharedMap().put(key, callBackArg);

    if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("key")) {
      FixedKeyResolver resolverKey = new FixedKeyResolver(key, callBackArg);
      for (Region aRegion : regionSet) {
        aRegion.put(resolverKey, value);
      }
    }
    else if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callBackArg")) {
      PartitionObjectHolder keyHolder = new PartitionObjectHolder(key,
          callBackArg);
      FixedKeyResolver resolverArg = new FixedKeyResolver(key, callBackArg);
      for (Region aRegion : regionSet) {
        aRegion.put(keyHolder, value, resolverArg);
      }
    }
  }

  /**
   * Return the key wrapped in the wrapper to be used as region key in the test
   * @param keyName
   * @return
   */
  public Object getKeyWrapper(Object keyName) {
    Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(keyName);
    if (callBackArg == null) {
      callBackArg = Month.months[TestConfig.tab().getRandGen().nextInt(11)];
      InitImageBB.getBB().getSharedMap().put(keyName, callBackArg);
    }
    if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("key")) {
      FixedKeyResolver resolverKey = new FixedKeyResolver(keyName, callBackArg);
      return resolverKey;
    }
    else if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callBackArg")) {
      PartitionObjectHolder keyHolder = new PartitionObjectHolder(keyName,
          callBackArg);
      return keyHolder;
    }
    else {
      throw new TestException(
          "Invalid congifuration - test uses either key/callback implementing resolver");
    }
  }

  /**
   * Return the key wrapped in the wrapper to be used as region key in the test
   * @param keyName
   * @return
   */
  public Object getCallBackWrapper(Object keyName) {
    Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(keyName);
    if (callBackArg == null) {
      callBackArg = Month.months[TestConfig.tab().getRandGen().nextInt(11)];
      InitImageBB.getBB().getSharedMap().put(keyName, callBackArg);
    }
    if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("key")) {
      return null;
    }
    else if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callBackArg")) {
      FixedKeyResolver resolverArg = new FixedKeyResolver(keyName, callBackArg);
      return resolverArg;
    }
    else {
      throw new TestException(
          "Invalid congifuration - test uses either key/callback implementing resolver");
    }
  }
  
  /**
   * Method to do onRegion function executions with filter when callbackArg
   * implements FixedPartitionResolver
   * 
   * @param aRegion
   */
  public void executeWithCallBackFilter(Region aRegion) {
    Set filterSet = new HashSet<Object>();

    if (TestConfig.tab().getRandGen().nextBoolean()) { // execute on single
      // node
      String key = NameFactory.OBJECT_NAME_PREFIX
          + TestConfig.tab().getRandGen().nextInt(keyIntervals.getNumKeys());
      Object callBackArg = getCallBackWrapper(key);
      filterSet.add(callBackArg);
    }
    else { // execute on multiple nodes
      int numCallbackArgs = 10; //
      for (int i = 1; i <= numCallbackArgs; i++) {
        String key = NameFactory.OBJECT_NAME_PREFIX
            + TestConfig.tab().getRandGen().nextInt(keyIntervals.getNumKeys());
        Object callBackArg = getCallBackWrapper(key);
        filterSet.add(callBackArg);
      }
    }

    hydra.Log.getLogWriter().info(
        "Executing function on "
            + (filterSet.size() == 1 ? " single node" : "multiple nodes")
            + " with filter size as " + filterSet.size());
    Function function = new PrimaryExecutionFunction();
    ArrayListResultCollector resultCollectorList = new ArrayListResultCollector();

    Execution dataSet = FunctionService.onRegion(aRegion).withCollector(
        resultCollectorList).withFilter(filterSet);

    ArrayList<Object> list;
    try {
      list = (ArrayList<Object>)dataSet.execute(function).getResult();
    }
    catch (Exception e) {
      Log.getLogWriter().info("Caught " + e + ", " + e.getStackTrace());
      throw new TestException("Caught Exception during function execution ", e);
    }

    HashSet quarterSet = new HashSet();
    for (Object callBackArg : filterSet) {
      Month month = (Month)((FixedKeyResolver)callBackArg).getRoutingHint();
      String quarter = month.getQuarter();
      quarterSet.add(quarter);
    }

    int numPrimariesPerPartition = 1;

    if (numPrimariesPerPartition == 1) {
      if (list.size() == quarterSet.size()) {
        hydra.Log.getLogWriter().info(
            "Got the expected result from the onRegion() execution "
                + list.size());
      }
      else {
        throw new TestException(
            " The filter used had the primary quarters "
                + quarterSet
                + " where each node have only one primary partition. Expected result set be of size "
                + quarterSet.size() + " but got " + list.size()
                + " and result is " + list);
      }
    }
  }

  // ---------- TESTOPS -------------------------------------//

  protected void addNewKey(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    Log.getLogWriter().info(
        "In addNewKey " + key + " in the region " + aRegion.getName());
    Object value = new ValueHolder(key.toString(), randomValues);
    aRegion.put(key, value, callbackArg);
  }

  public void invalidate(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    Log.getLogWriter().info(
        "Invalidating " + key + " in the region " + aRegion.getName());
    aRegion.invalidate(key, callbackArg);
  }

  public void destroy(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    Log.getLogWriter().info(
        "Destroying " + key + " in the region " + aRegion.getName());
    aRegion.destroy(key, callbackArg);
  }

  public void updateExistingKey(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    Log.getLogWriter().info(
        "Update existing key " + key + " in the region " + aRegion.getName());
    BaseValueHolder existingValue = (BaseValueHolder)aRegion.get(key, callbackArg);
    BaseValueHolder newValue = new ValueHolder(key.toString(), randomValues);
    if (existingValue == null)
      throw new TestException("Get of key " + key + " returned unexpected "
          + existingValue);
    if (existingValue.myValue instanceof String)
      throw new TestException(
          "Trying to update a key which was already updated: "
              + existingValue.myValue);
    newValue.myValue = "updated_" + key.toString();
    aRegion.put(key, newValue, callbackArg);
  }

  public void get(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    Object existingValue = null;
    existingValue = aRegion.get(key, callbackArg);

    if (existingValue == null)
      throw new TestException("Get of key " + key + " returned unexpected "
          + existingValue);
  }

  public void localInvalidate(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    aRegion.localInvalidate(key, callbackArg);
  }

  public void localDestroy(Region aRegion, Object keyName) {
    Object key = getKeyWrapper(keyName);
    Object callbackArg = getCallBackWrapper(keyName);
    aRegion.localDestroy(key, callbackArg);
  }

  /**
   * Verify the contents of the region, taking into account the keys that were
   * destroyed, invalidted, etc (as specified in keyIntervals) Throw an error of
   * any problems are detected.
   */
  public void verifyRegionContents(Region aRegion) {
    if (TestConfig.tab().stringAt(ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callBackArg")) {
      hydra.Log
          .getLogWriter()
          .info(
              "This test uses callBackArg as Resolver and hence cannot check contaisKey()"
                  + " that does not use callBackArg. Hence returning without validating");
      return;
    }
    InitImageBB.getBB().printSharedCounters();
    NameBB.getBB().printSharedCounters();
    StringBuffer errStr = new StringBuffer();

    // check region size
    long numKeys = aRegion.size();
    if (totalNumKeys != numKeys) {
      String tmpStr = "Expected " + totalNumKeys + " keys, but there are "
          + numKeys;
      Log.getLogWriter().info(tmpStr);
      errStr.append(tmpStr + "\n");
    }
    Log.getLogWriter().info(
        "In verifyRegionContents, region has " + numKeys + " keys");

    // iterate keys
    long lastLogTime = System.currentTimeMillis();
    for (int i = 1; i <= totalNumKeys; i++) {
      Object keyName = NameFactory.getObjectNameForCounter(i);
      Object key = getKeyWrapper(keyName);
      try {
        if (((i >= keyIntervals.getFirstKey(KeyIntervals.NONE)) && (i <= keyIntervals
            .getLastKey(KeyIntervals.NONE)))
            || ((i >= keyIntervals.getFirstKey(KeyIntervals.GET)) && (i <= keyIntervals
                .getLastKey(KeyIntervals.GET)))) {
          // this key was untouched after its creation
          checkContainsKey(aRegion, key, true, "key was untouched");
          checkContainsValueForKey(aRegion, key, true, "key was untouched");
          Object value = aRegion.get(key);
          checkValue(key, value);
        }
        else if ((i >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE))
            && (i <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
          checkContainsKey(aRegion, key, true, "key was invalidated");
          checkContainsValueForKey(aRegion, key, false, "key was invalidated");
        }
        else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE))
            && (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
          // this key was locally invalidated
          checkContainsKey(aRegion, key, true, "key was locally invalidated");
          checkContainsValueForKey(aRegion, key, true,
              "key was locally invalidated");
          Object value = aRegion.get(key);
          checkValue(key, value);
        }
        else if ((i >= keyIntervals.getFirstKey(KeyIntervals.DESTROY))
            && (i <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
          // this key was destroyed
          checkContainsKey(aRegion, key, false, "key was destroyed");
          checkContainsValueForKey(aRegion, key, false, "key was destroyed");
        }
        else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY))
            && (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
          // this key was locally destroyed
          checkContainsKey(aRegion, key, true, "key was locally destroyed");
          checkContainsValueForKey(aRegion, key, true,
              "key was locally destroyed");
          Object value = aRegion.get(key);
          checkValue(key, value);
        }
        else if ((i >= keyIntervals
            .getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY))
            && (i <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
          // this key was updated
          checkContainsKey(aRegion, key, true, "key was updated");
          checkContainsValueForKey(aRegion, key, true, "key was updated");
          Object value = aRegion.get(key);
          checkUpdatedValue(key, value);
        }
        else if (i > keyIntervals.getNumKeys()) {
          // key was newly added
          checkContainsKey(aRegion, key, true, "key was new");
          checkContainsValueForKey(aRegion, key, true, "key was new");
          Object value = aRegion.get(key);
          checkValue(key, value);
        }
      }
      catch (TestException e) {
        Log.getLogWriter().info(TestHelper.getStackTrace(e));
        errStr.append(e.getMessage() + "\n");
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        Log.getLogWriter()
            .info("Verified key " + i + " out of " + totalNumKeys);
        lastLogTime = System.currentTimeMillis();
      }
    }

    if (errStr.length() > 0)
      throw new TestException(errStr.toString());
  }

  /**
   * Check that the value of the given key is expected as an updated value.
   * Throw an error if any problems.
   * 
   * @param key
   *                The key to check.
   * @param value
   *                The value for the key.
   * @param logStr
   *                Used if throwing an error due to an unexpected value.
   */
  protected void checkUpdatedValue(Object key, Object value) {
    if (value instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)value;
      String keyName = key.toString();
      if (vh.myValue instanceof String) {
        String aStr = (String)vh.myValue;
        String expectedStr = "updated_" + keyName;
        if (!aStr.equals(expectedStr))
          throw new TestException("Inconsistent ValueHolder.myValue for key "
              + key + ":" + TestHelper.toString(vh));
      }
      else {
        throw new TestException(
            "Expected ValueHolder.myValue for key "
                + key
                + " to be a String indicating it was updated, but the value for this key is "
                + TestHelper.toString(vh));
      }
    }
    else {
      throw new TestException("Expected value " + TestHelper.toString(value)
          + " to be a ValueHolder");
    }
  }

  /**
   * Check that the value of the given key is expected for this test. Throw an
   * error if any problems.
   * 
   * @param key
   *                The key to check.
   * @param value
   *                The value for the key.
   * @param logStr
   *                Used if throwing an error due to an unexpected value.
   */
  protected void checkValue(Object key, Object value) {
    if (value instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)value;
      long keyCounter;
      if (key instanceof FixedPartitionResolver) {
        Object counter = ((FixedPartitionResolver)key).getName();
        keyCounter = NameFactory.getCounterForName(counter);
      }
      else {
        keyCounter = NameFactory.getCounterForName(key);
      }
      if (vh.myValue instanceof Long) {
        Long aLong = (Long)vh.myValue;
        long longValue = aLong.longValue();
        if (keyCounter != longValue)
          throw new TestException("Inconsistent ValueHolder.myValue for key "
              + key + ":" + TestHelper.toString(vh));
      }
      else {
        throw new TestException("Expected ValueHolder.myValue for key " + key
            + " to be a Long for " + TestHelper.toString(vh));
      }
    }
    else {
      throw new TestException("For key " + key + ", expected value "
          + TestHelper.toString(value) + " to be a ValueHolder");
    }
  }

}
