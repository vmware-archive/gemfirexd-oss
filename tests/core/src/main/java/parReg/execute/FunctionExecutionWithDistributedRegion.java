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
package parReg.execute;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.GatewayPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import parReg.KnownKeysTest;
import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.ParRegUtil;
import util.PRObserver;
import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

public class FunctionExecutionWithDistributedRegion extends ExecutionAndColocationTest {
  
  protected static final String KEY_SET = "Key Set";

  private static FunctionExecutionWithDistributedRegion getTestInstance() {
    return (FunctionExecutionWithDistributedRegion)testInstance;
  }

  public synchronized static void HydraTask_HA_initReplicatedDataStore() {
    if (testInstance == null) {
      // TODO : Aneesh Remove this (currently required for stop start though)
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      testInstance = new FunctionExecutionWithDistributedRegion();
      ((FunctionExecutionWithDistributedRegion)testInstance)
          .initInstance("replicateServerRegion");
      ParRegBB.getBB().getSharedMap().put(
          DataStoreVmStr + RemoteTestModule.getMyVmid(),
          new Integer(RemoteTestModule.getMyVmid()));
      RegionAttributes attr = testInstance.aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      if (prAttr == null) { // this is not a PR datastore, but probably a
                            // replicate; done in parReg/execute tests
        ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(-1));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay",
            new Long(-1));
      }
      else {
        ParRegBB.getBB().getSharedMap().put("recoveryDelay",
            new Long(prAttr.getRecoveryDelay()));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay",
            new Long(prAttr.getStartupRecoveryDelay()));
      }
    }
  }

  public synchronized static void HydraTask_HA_initEmptyDataStore() {
    if (testInstance == null) {
      // TODO : Aneesh Remove this (currently required for stop start though)
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      testInstance = new FunctionExecutionWithDistributedRegion();
      ((FunctionExecutionWithDistributedRegion)testInstance)
          .initInstance("emptyServerRegion");
      RegionAttributes attr = testInstance.aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      if (prAttr == null) { // this is not a PR datastore, but probably a
                            // replicate; done in parReg/execute tests
        ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(-1));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay",
            new Long(-1));
      }
      else {
        ParRegBB.getBB().getSharedMap().put("recoveryDelay",
            new Long(prAttr.getRecoveryDelay()));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay",
            new Long(prAttr.getStartupRecoveryDelay()));
      }
      if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("server");
      }
    }
  }

  public synchronized static void HydraTask_HA_initClients() {
    if (testInstance == null) {
      testInstance = new FunctionExecutionWithDistributedRegion();
      ((FunctionExecutionWithDistributedRegion)testInstance)
          .initInstance("clientRegion");
      if (isBridgeConfiguration) {
        Log.getLogWriter().info("Registering interest on the region");
        ParRegUtil.registerInterest(testInstance.aRegion);
      }
    }
  }

  /**
   * Hydra task for executing functions on all keys for HA (so only on Region)
   */
  public static void HydraTask_executeFunctionAllKeysHA() {
    ((FunctionExecutionWithDistributedRegion)testInstance)
        .executeFunctionWithMapCollector();
    ((FunctionExecutionWithDistributedRegion)testInstance)
        .executeFunctionWithListCollector();
  }
  
  public static void HydraTask_putKeySetInBB() {
    ((FunctionExecutionWithDistributedRegion)testInstance).putKeySetInBB();
  }

  /**
   * Initialize this test instance
   */
  public void initInstance(String regDescriptName) {
    super.initInstance();
    highAvailability = TestConfig.tab().booleanAt(ParRegPrms.highAvailability,
        false);
    Cache myCache = CacheHelper.createCache("cache1");
    ((FunctionExecutionWithDistributedRegion)testInstance).registerFunctions();
    // used by rebalance tests only
    InternalResourceManager.ResourceObserver ro = ParRegPrms
        .getResourceObserver();
    if (ro != null) {
      InternalResourceManager rm = InternalResourceManager
          .getInternalResourceManager(myCache);
      rm.setResourceObserver(ro);
    }

    aRegion = RegionHelper.createRegion(regDescriptName);
    isBridgeConfiguration = (TestConfig.tab().vecAt(BridgePrms.names, null) != null);
    isGatewayConfiguration = (TestConfig.tab()
        .stringAt(GatewayPrms.names, null) != null);
    lockOperations = TestConfig.tab().booleanAt(ParRegPrms.lockOperations,
        false);
    if (lockOperations) {
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      distLockService = DistributedLockService.create(LOCK_SERVICE_NAME,
          DistributedSystemHelper.getDistributedSystem());
      Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
    }
  }

  /**
   * Task to execute functions on all keys
   */
  public void executeFunctionWithMapCollector() {
    Log.getLogWriter().info("executeFunctionWithMapCollector()");

    Function getAllKeysFunction = new KeysOperationsFunction();
    FunctionService.registerFunction(getAllKeysFunction);
    HashMapResultCollector resultCollectorMap = new HashMapResultCollector();
    Execution dataSet;
    Set keySet = null;
    Map myMap = null;

    
    if (aRegion.getAttributes().getDataPolicy() == DataPolicy.EMPTY) {
      keySet = (Set)parReg.ParRegBB.getBB().getSharedMap().get(KEY_SET);
    }
    else {
      keySet = aRegion.keySet();
    }
    dataSet = FunctionService.onRegion(aRegion).withCollector(
        resultCollectorMap);

    try {
      ResultCollector rc = dataSet.withFilter(keySet).execute(
          new KeysOperationsFunction().getId());
      myMap = (HashMap)rc.getResult(200,TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new TestException("Got this exception " + e.getMessage()
          + " Cause " + e.getCause(), e);
    }

    if (!(keySet.size() == myMap.size())) {
      throw new TestException(
          "Expected result collector returned map size to be " + keySet.size()
              + " but got the value " + myMap.size());
    }
    else {
      Log.getLogWriter().info(
          "Got the expected size for the map " + myMap.size());
    }

    Iterator iterator = myMap.entrySet().iterator();
    Map.Entry entry = null;
    Object key = null;
    Object value = null;
    Object referenceValue = null;
    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = entry.getKey();
      value = entry.getValue();

      referenceValue = aRegion.get(key);

      if (!(value == null || referenceValue == null)) {
        if (!value.equals(referenceValue)) {
          throw new TestException("For the key " + key
              + " the values found in map is " + value
              + " and that in region is " + referenceValue);
        }
        else {
          Log.getLogWriter().info(
              "For the key " + key
                  + " the values found the expected values in map and region "
                  + value);
        }
      }
      else {
        if ((value == null && referenceValue == null)) {
          Log.getLogWriter().info("Got expected null for the key " + key);
        }
        else {
          throw new TestException("Expected value for the key " + key
              + " to be " + referenceValue + " but got the value " + value);
        }
      }

    }

  }

  /**
   * Task to execute functions on all keys
   */
  public void executeFunctionWithListCollector() {
    Log.getLogWriter().info("executeFunctionWithListCollector()");

    ArrayListResultCollector resultCollectorList = new ArrayListResultCollector();
    Execution dataSet;
    Set keySet = null;
    ArrayList myList = null;

    if (aRegion.getAttributes().getDataPolicy() == DataPolicy.EMPTY) {
      keySet = (Set)parReg.ParRegBB.getBB().getSharedMap().get(KEY_SET);
    }
    else {
      keySet = aRegion.keySet();
    }
    
    dataSet = FunctionService.onRegion(aRegion).withCollector(
        resultCollectorList).withArgs("MultiNode");

    try {
      myList = (ArrayList)dataSet.withFilter(keySet).execute(
          new KeysOperationsFunction().getId()).getResult(200, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new TestException("Got this exception " + e.getMessage()
          + " Cause " + e.getCause(), e);
    }

    if (!(keySet.size() == myList.size())) {
      throw new TestException(
          "Expected result collector returned map size to be " + keySet.size()
              + " but got the value " + myList.size());
    }
    else {
      Log.getLogWriter().info(
          "Got the expected size for the map " + myList.size());
    }
  }
  
  
  public void putKeySetInBB() {
    HashSet keySet = new HashSet();
    Set keys = aRegion.keySet();
    Iterator iterator = keys.iterator();
    while (iterator.hasNext()) {
      Object key = iterator.next();
      keySet.add(key);
    }
    if (parReg.ParRegBB.getBB().getSharedMap().get(KEY_SET) == null) {
      parReg.ParRegBB.getBB().getSharedMap().put(KEY_SET, keySet);
    }
  }

}
