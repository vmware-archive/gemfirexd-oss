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
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.GsRandom;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class ExceptionHandlingTest {

  private static final int NUM_KEYS = 500;

  protected static String DataStoreVmStr = "DataStoreVM_";

  public static GsRandom random = new GsRandom();

  protected static ExceptionHandlingTest testInstance;

  protected static Cache theCache;

  protected static Region<String, Integer> partitionedRegion;

  protected static Region<String, Integer> replicatedRegion;

  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ExceptionHandlingTest();
    }
    testInstance.initialize();
    ParRegBB.getBB().getSharedMap().put(
        DataStoreVmStr + RemoteTestModule.getMyVmid(),
        new Integer(RemoteTestModule.getMyVmid()));
  }

  public synchronized static void HydraTask_initializeServer() {
    if (testInstance == null) {
      testInstance = new ExceptionHandlingTest();
    }
    testInstance.initializeServer();
    BridgeHelper.startBridgeServer("bridge");
    ParRegBB.getBB().getSharedMap().put(
        DataStoreVmStr + RemoteTestModule.getMyVmid(),
        new Integer(RemoteTestModule.getMyVmid()));

  }

  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new ExceptionHandlingTest();
    }
    testInstance.initializeClient();
  }

  public synchronized static void HydraTask_populateRegion() {
    if (testInstance == null) {
      testInstance = new ExceptionHandlingTest();
    }
    testInstance.populateRegion(partitionedRegion);
    testInstance.populateRegion(replicatedRegion);
  }

  public synchronized static void HydraTask_registerFunction() {
    if (testInstance == null) {
      testInstance = new ExceptionHandlingTest();
    }
    FunctionService.registerFunction(new ExceptionHandlingFunction());
  }

  public synchronized static void HydraTask_doFunctionExecutions() {
    if (TestConfig.tab().booleanAt(ParRegPrms.highAvailability, false)) {
      testInstance.doHctFunctionExecutions();
    }
    else {
      testInstance.doP2PFunctionExecutions();
    }
  }

  public static void HydraTask_stopStartVms() {
    testInstance.stopStartVms();
  }

  protected void initialize() {
    theCache = CacheHelper.createCache("cache1");
    replicatedRegion = RegionHelper.createRegion("replicatedRegion");
    partitionedRegion = RegionHelper.createRegion("partitionedRegion");
  }

  protected void initializeServer() {
    theCache = CacheHelper.createCache("cache1");
    partitionedRegion = RegionHelper.createRegion("bridgePR");
    replicatedRegion = RegionHelper.createRegion("bridgeReplicated");
  }

  protected void initializeClient() {
    theCache = CacheHelper.createCache("cache1");
    partitionedRegion = RegionHelper.createRegion("edgeRegion1");
    replicatedRegion = RegionHelper.createRegion("edgeRegion2");
  }

  protected void populateRegion(Region<String, Integer> region) {
    for (int i = 0; i < NUM_KEYS; i++) {
      String keyName = "Key "
          + ParRegBB.getBB().getSharedCounters().incrementAndRead(
              ParRegBB.numOfPutOperations);
      Integer value = new Integer(i);
      region.put(keyName, value);
    }
    Log.getLogWriter().info(
        "Completed put for " + NUM_KEYS + " keys and region size is "
            + region.size());
  }

  protected void doP2PFunctionExecutions() {
    int n = random.nextInt(1, 40);
    if (n < 10) {
      doParitionedRegionFunctionExecution();
    }
    else if (n < 20) {
      doReplicatedRegionFunctionExecution();
    }
    else if (n < 30) {
      doFireAndForgetFunctionExecution();
    }
    else {
      doOnMembersFunctionExcecution();
    }
  }

  protected void doHctFunctionExecutions() {
    int n = random.nextInt(1, 40);
    if (n < 10) {
      doParitionedRegionFunctionExecution();
    }
    else if (n < 20) {
      doReplicatedRegionFunctionExecution();
    }
    else if (n < 30) {
      doFireAndForgetFunctionExecution();
    }
    else {
      doOnServersFunctionExcecution();
    }
  }

  protected void doParitionedRegionFunctionExecution() {
    Execution dataSet;
    if (random.nextBoolean()) {
      Log.getLogWriter().info("Execution on partitionedRegion with no filter");
      dataSet = FunctionService.onRegion(partitionedRegion).withCollector(
          new ArrayListResultCollector());
    }
    else {
      Log.getLogWriter().info("Execution on partitionedRegion with filter");
      Set keySet = partitionedRegion.keySet();
      dataSet = FunctionService.onRegion(partitionedRegion).withFilter(keySet)
          .withCollector(new ArrayListResultCollector());
    }
    try {
      ResultCollector rc = dataSet.execute(new ExceptionHandlingFunction());
      List list = (ArrayList)rc.getResult();
      Log.getLogWriter().info(
          "Successful completion of execution on partitioned region on nodes "
              + list);
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains("Server unreachable")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerConnectivityException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains(
              "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException) && (e.getCause() instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException(
            "Caught the exception during function execution ", e);
      }
    }
  }

  protected static void doFireAndForgetFunctionExecution() {
    Region region;
    if (random.nextBoolean()) {
      Log.getLogWriter()
          .info("Execution Fire and forget on partitioned region");
      region = partitionedRegion;
    }
    else {
      Log.getLogWriter().info("Execution Fire and forget on replicated region");
      region = replicatedRegion;
    }

    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());

    try {
      ResultCollector rc = dataSet.execute(new FunctionAdapter() {
        public void execute(FunctionContext context) {
          String argument = null;
          ArrayList<String> list = new ArrayList<String>();
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          String localVM = ds.getDistributedMember().getId();
          Log.getLogWriter().info(
              "Inside function execution (some time) node " + localVM);
          for (int i = 0; i < Integer.MAX_VALUE; i++) {
            for (int j = 0; j < 4; j++) {
            }
          }
          list.add(localVM);
          Log.getLogWriter().info(
              "Completed function execution (some time) node " + localVM);
        }

        @Override
        public String getId() {
          return "FireNForget";
        }

        public boolean hasResult() {
          return false;
        }
        
        public boolean isHA() {
          return false;
        }
      });
      Log.getLogWriter().info(
          "Successful completion of Fire and forget execution ");
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains("Server unreachable")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerConnectivityException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains(
              "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException) && (e.getCause() instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException(
            "Caught the exception during function execution ", e);
      }
    }
  }

  protected void doReplicatedRegionFunctionExecution() {
    Log.getLogWriter().info("Execution on replicated Region");
    Execution dataSet = FunctionService.onRegion(replicatedRegion)
        .withCollector(new ArrayListResultCollector());

    try {
      ResultCollector rc = dataSet.execute(new ExceptionHandlingFunction());
      List list = (ArrayList)rc.getResult();
      Log.getLogWriter().info(
          "Successful completion of execution on replicated region on nodes "
              + list);
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains("Server unreachable")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerConnectivityException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains(
              "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException) && (e.getCause() instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException(
            "Caught the exception during function execution ", e);
      }
    }
  }

  protected void doOnServersFunctionExcecution() {
    Log.getLogWriter().info("Execution on onServers");

    Pool pool = PoolHelper.getPool("edgeDescript");
    if (pool == null) {
      Log.getLogWriter().info("Pool is null");
      pool = PoolHelper.createPool("edgeDescript");
    }

    Execution dataSet = FunctionService.onServers(pool).withCollector(
        new ArrayListResultCollector());

    try {
      ResultCollector rc = dataSet.execute(new ExceptionHandlingFunction());
      List list = (ArrayList)rc.getResult();
      Log.getLogWriter().info(
          "Successful completion of execution onMembers on nodes " + list);
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains("Server unreachable")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof ServerConnectivityException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains(
              "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
/*      else if ((e instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }*/
      else if ((e instanceof FunctionException) && (e.getCause() instanceof ServerOperationException)) {
        // && e.getMessage().contains(
        // "ExceptionHandlingFunction is not registered")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException(
            "Caught the exception during function execution ", e);
      }
    }
  }

  protected void doOnMembersFunctionExcecution() {
    Log.getLogWriter().info("Execution on onMembers");

    DistributedSystem ds = theCache.getDistributedSystem();
    Execution dataSet = FunctionService.onMembers(ds).withCollector(
        new ArrayListResultCollector());

    try {
      ResultCollector rc = dataSet.execute(new ExceptionHandlingFunction());
      List list = (ArrayList)rc.getResult();
      Log.getLogWriter().info(
          "Successful completion of execution onMembers on nodes " + list);
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info("Got expected exception " + e);
      }else if ((e instanceof FunctionException)
          && e.getMessage().contains(
          "ExceptionHandlingFunction is not registered")) {
       Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException(
            "Caught the exception during function execution ", e);
      }
    }
  }

  protected void stopStartVms() {
    int numRegions = CacheHelper.getCache().rootRegions().size();
     //PRObserver.initialize();
    int numVMsToStop = TestConfig.tab().intAt(ParRegPrms.numVMsToStop);
    // Remove own id (do not crash self)
    ParRegBB.getBB().getSharedMap().remove(
        DataStoreVmStr + RemoteTestModule.getMyVmid());
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
    //PRObserver.waitForRebalRecov(targetVms, 1, numRegions, null, null,
    // false);
    Log.getLogWriter().info("Done in stopStartVms()");
  }

  protected List getDataStoreVms() {
    List aList = new ArrayList();
    Map aMap = ParRegBB.getBB().getSharedMap().getMap();
    Iterator it = aMap.keySet().iterator();
    while (it.hasNext()) {
      Object key = it.next();
      if (key instanceof String) {
        if (((String)key).startsWith(DataStoreVmStr)) {
          aList.add(new ClientVmInfo((Integer)(aMap.get(key)), null, null));
        }
      }
    }
    return aList;
  }

}
