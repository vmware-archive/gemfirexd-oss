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
package vsphere.vijava;

import getInitialImage.InitImageBB;
import getInitialImage.InitImagePrms;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.Log;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import parReg.ParRegBB;
import parReg.execute.FunctionServiceTest;
import util.KeyIntervals;
import util.PRObserver;
import util.RandomValues;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class VMotionFunctionServiceTest extends FunctionServiceTest {

  private static int maxRegions;

  /**
   * Hydra task for initializing a dataStore VM
   */
  public synchronized static void HydraTask_dataStoreVMInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      isBridgeConfiguration = false;
      testInstance = new VMotionFunctionServiceTest();
      isBridgeClient = false;
      testInstance.initialize("dataStore");
      ((FunctionServiceTest)testInstance).registerFunctions();
      ParRegBB
          .getBB()
          .getSharedMap()
          .put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
              new Integer(RemoteTestModule.getMyVmid()));
    }
  }

  /**
   * Hydra task for initializing an accessor VM
   */
  public synchronized static void HydraTask_accessorVMInitialize() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      isBridgeConfiguration = true;
      testInstance = new VMotionFunctionServiceTest();
      isBridgeClient = false;
      testInstance.initialize();
      ((FunctionServiceTest)testInstance).registerFunctions();
      ParRegBB
          .getBB()
          .getSharedMap()
          .put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
              new Integer(RemoteTestModule.getMyVmid()));
      if (isBridgeConfiguration
          && !TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile,
              false)) {
        BridgeHelper.startBridgeServer("bridge");
        Log.getLogWriter().info("started bridge server on accessor.");
      }
    }
  }

  /**
   * Hydra task for initializing a client VM
   */
  public synchronized static void HydraTask_HA_clientVMInitialize() {
    if (testInstance == null) {
      isBridgeConfiguration = true;
      isBridgeClient = true;
      testInstance = new VMotionFunctionServiceTest();
      testInstance.initialize();
      ((FunctionServiceTest)testInstance).registerFunctions();
    }
  }

  /**
   * Initialize this test instance
   */
  public void initialize() {
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

    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));
      if (regionDescriptName.startsWith("bridge")) { // this is a server region
        // description
        bridgeRegionDescriptNames.add(regionDescriptName);
      }
      else if (!regionDescriptName.startsWith("dataStore")) {
        regionDescriptNames.add(regionDescriptName);
      }
    }
    if (isBridgeConfiguration) {
      if (bridgeRegionDescriptNames.size() != regionDescriptNames.size()) {
        throw new TestException(
            "Error in test configuration; need equal number of region descriptions for bridge servers and bridge clients");
      }
    }
    Log.getLogWriter().info(
        "bridgeRegionDescriptNames is " + bridgeRegionDescriptNames);
    Log.getLogWriter().info("regionDescriptNames is " + regionDescriptNames);
    maxRegions = regionDescriptNames.size();
    destroyThreshold = (int)(regionDescriptNames.size() * 0.8);
    Log.getLogWriter().info("destroyThreshold is " + destroyThreshold);
    Log.getLogWriter().info("ENTRIES_TO_PUT is " + ENTRIES_TO_PUT);
    if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile, false)) {
      createRegionsWithXml();
    }
    else {
      theCache = CacheHelper.createCache("cache1");
      createRegions();
    }
    TestHelper.logRegionHierarchy();
  }

  /**
   * Initialize this test instance
   */
  public void initialize(String regionDescription) {
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

    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));
      if (regionDescriptName.startsWith("dataStore")
          && regionDescription.startsWith("dataStore")) { // this is a data
        // store
        regionDescriptNames.add(regionDescriptName);
      }
    }
    Log.getLogWriter().info("regionDescriptNames is " + regionDescriptNames);
    maxRegions = regionDescriptNames.size();
    // theCache = CacheHelper.createCache("cache1");
    destroyThreshold = (int)(regionDescriptNames.size() * 0.8);
    Log.getLogWriter().info("destroyThreshold is " + destroyThreshold);
    Log.getLogWriter().info("ENTRIES_TO_PUT is " + ENTRIES_TO_PUT);
    if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile, false)) {
      createRegionsWithXml();
    }
    else {
      theCache = CacheHelper.createCache("cache1");
      createRegions();
    }
  }

//  /**
//   * Hydra task to execute region functions, then stop scheduling.
//   */
//  public static void HydraTask_doFunctionExecution() {
//    BitSet availableOps = new BitSet(operations.length);
//    availableOps.flip(FIRST_OP, LAST_OP + 1);
//    ((VMotionFunctionService)testInstance).doFunctionExecution(availableOps);
//    Log.getLogWriter().info("Cardinality is " + availableOps.cardinality());
//    if (availableOps.cardinality() == 0) {
//      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.TimeToStop);
//      throw new StopSchedulingTaskOnClientOrder("Finished with ops");
//    }
//  }

  public synchronized void queryFunction(Region aRegion, Object key) {

    Function queryFunction = new VMotionRegionOperationsFunction();
    // FunctionService.registerFunction(getFunction);
    Execution dataSet;

    if (aRegion instanceof PartitionedRegion) {
      Log.getLogWriter().info("Inside p2p function execution");
      dataSet = FunctionService.onRegion(aRegion);
    }
    else {
      Log.getLogWriter().info("Inside client server function execution");
      dataSet = FunctionService.onRegion(aRegion);
    }

    final Set keySet = new HashSet();
    keySet.add(key);
    ArrayList aList = new ArrayList();
    aList.add("query");
    aList.add(RemoteTestModule.getCurrentThread().getThreadId());
    Log.getLogWriter().info("Going to do query execute");
    ResultCollector drc = dataSet.withFilter(keySet).withArgs(aList)
        .execute(queryFunction.getId());
    drc.getResult();
  }
}
