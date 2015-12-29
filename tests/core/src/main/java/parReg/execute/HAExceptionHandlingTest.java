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
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import parReg.ParRegBB;
import parReg.colocation.KeyResolver;
import parReg.colocation.Month;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class HAExceptionHandlingTest {

  protected static HAExceptionHandlingTest testInstance;

  public GsRandom random = new GsRandom();

  protected static Cache theCache;

  protected static Region aRegion;

  public static final int NUM_KEYS = 200;

  public static final String BUCKETS_ON_NODE = "Buckets on node";

  public static final String ALL_BUCKET_IDS = "All Bucket Ids";
  
  public static final String KEY_SET = "Key Set";
  
  public static final String BUCKET_KEYS = "Bucket Keys";
  
  public static final String CACHE_CLOSED_VM = "Cache Closed Vm";

  /**
   * Hydra task to initialize the data storeo
   */
  public synchronized static void HydraTask_initializeDataStore() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
      testInstance.initialize("dataStore");
    }
  }

  /**
   * Hydra task to initialize the accessor
   */
  public synchronized static void HydraTask_initializeAccessor() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
      testInstance.initialize("accessor");
    }
  }

  /**
   * Hydra task to initialize the server
   */
  public synchronized static void HydraTask_initializeServer() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
      testInstance.initialize("bridge");
      BridgeHelper.startBridgeServer("bridge");
    }
  }

  /**
   * Hydra task to initialize the client
   */
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
      testInstance.initialize("edge");
    }
  }

  /**
   * Hydra task to populate the region
   */
  public synchronized static void HydraTask_populateRegion() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.populateRegion(aRegion);
  }

  /**
   * Hydra task to populate ther region with custom partitioning enabled
   */
  public synchronized static void HydraTask_populateRegionWithCustomPartition() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.populateRegionWithCustomPartition(aRegion);
  }

  /**
   * Hydra task to register function
   */
  public static void HydraTask_registerFunction() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    FunctionService.registerFunction(new HAExceptionHandlingFunction());
  }

  /**
   * Hydra task to update the BB
   */
  public synchronized static void HydraTask_updateBBWithNodes() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.updateBBWithNodes(aRegion);
  }
  
  /**
   * Hydra task to update the BB for adding the keyset to BB
   */
  public synchronized static void HydraTask_updateBB() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.updateBB(aRegion);
  }

  /**
   * Hydra task to cause normal cache close of vm
   */
  public synchronized static void HydraTask_clientNormalShutDown() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.clientNormalShutDown();
  }

  /**
   * Hydra task to execute functions expecting exceptions during execution
   */
  public static void HydraTask_doExecuteExpectingExceptions() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.doExecuteNodeCacheCloseGetResult(aRegion);
    testInstance.doExecuteWithEntryDestroys(aRegion);
    testInstance.doExecuteNodeFailOverGetResult(aRegion);
  }
  
  /**
   * Hydra task to execute functions when the expected exceptions are wrapped in
   * by the customer with the FITE - to force re-execution with HA turned on.
   */
  public static void HydraTask_reExecuteExceptions() {
    testInstance.doReExecuteExceptionAllBuckets(aRegion);
    testInstance.doReExecuteExceptionMultipleBuckets(aRegion);
    testInstance.doReExecuteExceptionSingleBucket(aRegion);
  }

  /**
   * Hydra task to execute functions expecting exceptions during execution
   */
  public static void HydraTask_doRandomFunctionExecutions() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.doRandomFunctionExecutions(aRegion);
  }

  /**
   * Hydra task to either do a cache close or recycle the vm
   */
  public synchronized static void HydraTask_recycleOrCloseCache() {
    if (testInstance == null) {
      testInstance = new HAExceptionHandlingTest();
    }
    testInstance.recycleOrCloseCache(aRegion);
  }

  /**
   * Hydra task to execute functions expecting exceptions during execution
   */
  protected void recycleOrCloseCache(Region aRegion) {
    int n = random.nextInt(1, 100);
    if (n < 51) {
      recycleVM();
    }
    else {
      doCacheClose();
    }
  }

  /**
   * Task to either do a cache close for the server and start it back
   */
  protected void doCacheClose() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      Log.getLogWriter().info("Closing cache");
      theCache.close();
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    hydra.MasterController.sleepForMs(10000);
    initialize("bridge");
    BridgeHelper.startBridgeServer("bridge");
    FunctionService.registerFunction(new HAExceptionHandlingFunction());
    hydra.MasterController.sleepForMs(20000);
  }

  /**
   * Task to either do a cache close for the server and start it back
   */
  protected void recycleVM() {
    try {
      hydra.MasterController.sleepForMs(30000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.NEVER);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }

  /**
   * Task to do function execution on region
   * 
   * @param region
   */
  protected void doRandomFunctionExecutions(Region region) {
    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());

    ResultCollector rc;

    try {
      rc = dataSet.withArgs("ExecuteForSomeTime").execute(
          new HAExceptionHandlingFunction());
      rc.getResult();
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log.getLogWriter().info(
            "Got expected FunctionInvocationTargetException");
      }
      else if ((e instanceof FunctionException)
          && e.getMessage().contains("Server unreachable")) {
        Log.getLogWriter().info("Got expected exception " + e);
      }
      else {
        throw new TestException("Caught exception " + e + e.getMessage()
            + " Cause " + e.getMessage(), e);
      }
    }
  }

  /**
   * Task to do function execution where function execution encounters entries
   * destroyed during executin
   * 
   * @param region
   */
  protected void doExecuteWithEntryDestroys(Region region) {
    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());

    ArrayList list;
    ResultCollector rc;
    Set<String> filterSet;

    try {
      filterSet = ((PartitionedRegion)region).getSomeKeys(new Random(20));
    }
    catch (Exception e1) {
      throw new TestException("Test issue :" , e1);
    }

    try {
      rc = dataSet.withFilter(filterSet).withArgs("waitForKeyDestroys")
          .execute(new HAExceptionHandlingFunction());
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception ", e);
    }

    for (String key : filterSet) {
      region.destroy(key);
    }
    ExecuteExceptionBB.getBB().getSharedCounters().increment(
        ExecuteExceptionBB.keyDestroyCompleted);

    try {
      rc.getResult();
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof EntryNotFoundException)) {
        Log.getLogWriter().info("Got the expected EntryNotFoundException");
      }
      else {
        throw new TestException("Caught the exception ", e);
      }
    }

    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.keyDestroyCompleted);

  }
  
  /**
   * Task to do function exectution when the remote node fails (cache close)
   * 
   * @param region
   */
  protected void doExecuteNodeCacheCloseGetResult(Region region) {
    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());

    ArrayList list;
    ResultCollector rc;

    try {
      rc = dataSet.withArgs("executeForCacheClose").execute(
          new HAExceptionHandlingFunction());
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception ", e);
    }

    // Target the other vm to do a cache close
    Log.getLogWriter().info("Signalling the cache close for another node.");
    ExecuteExceptionBB.getBB().getSharedCounters().increment(
        ExecuteExceptionBB.signalCacheClose);

    try {
      Log.getLogWriter().info("Going to getResult()");
      rc.getResult();
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log
            .getLogWriter()
            .info(
                "Got the expected FunctionInvocationTargetException : going to re-execute");
        dataSet = FunctionService.onRegion(region).withCollector(
            new ArrayListResultCollector());
        try {
          rc = dataSet.withArgs("ExecuteForSomeTime").execute(
              new HAExceptionHandlingFunction());
          rc.getResult();
        }
        catch (Exception e1) {
          throw new TestException("Function execution failed with exception ",
              e1);
        }
      }
      else {
        throw new TestException("Caught the exception ", e);
      }
    }

    ExecuteExceptionBB.getBB().getSharedCounters().increment(
        ExecuteExceptionBB.signalCacheCreate);
    TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
        "ExecuteExceptionBB.cacheCreateCompleted",
        ExecuteExceptionBB.cacheCreateCompleted, 1, true, 60000);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.signalCacheClose);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.cacheCloseCompleted);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.signalCacheCreate);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.cacheCreateCompleted);

  }

  /**
   * Task to do function exectution when the remote node fails (vm crash)
   * 
   * @param region
   */
  protected void doExecuteNodeFailOverGetResult(Region region) {
    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());

    ArrayList list;
    ResultCollector rc;

    try {
      rc = dataSet.withArgs("executeForVMDown").execute(
          new HAExceptionHandlingFunction());
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception " , e);
    }

    // Kill VM
    ClientVmInfo targetNode = getTargetNodeToKill(region);
    Log.getLogWriter().info("Killing the vm " + targetNode);
    try {
      ClientVmMgr.stop("Killing vm", ClientVmMgr.MEAN_KILL,
          ClientVmMgr.ON_DEMAND, targetNode);
      ExecuteExceptionBB.getBB().getSharedCounters().increment(
          ExecuteExceptionBB.vmCrashComplete);
      Log.getLogWriter().info("Going to getResult()");
      rc.getResult();
    }
    catch (Exception e) {
      if ((e instanceof FunctionException)
          && (e.getCause() instanceof FunctionInvocationTargetException)) {
        Log
            .getLogWriter()
            .info(
                "Got the expected FunctionInvocationTargetException: going to re-execute");
        dataSet = FunctionService.onRegion(region).withCollector(
            new ArrayListResultCollector());
        try {
          rc = dataSet.withArgs("ExecuteForSomeTime").execute(
              new HAExceptionHandlingFunction());
          rc.getResult();
        }
        catch (Exception e1) {
          throw new TestException("Function execution failed with exception ",
              e1);
        }
      }
    }

    // Re-start the killed vm
    try {
      ClientVmMgr.start("Bringing back the target node", targetNode);
      ExecuteExceptionBB.getBB().getSharedCounters().increment(
          ExecuteExceptionBB.crashedVMUp);
    }
    catch (ClientVmNotFoundException e) {
      throw new TestException("Caught unexpected exception ", e);
    }
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.vmCrashComplete);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.crashedVMUp);
  }

  /**
   * Task to get the target node for vm crash
   */
  protected ClientVmInfo getTargetNodeToKill(Region region) {
    if (region instanceof PartitionedRegion) {
      List otherVMs = new ArrayList(ClientVmMgr.getOtherClientVmids());
      if (ExecuteExceptionBB.getBB().getSharedMap().get(CACHE_CLOSED_VM) != null) {
        Integer cacheClosedVmId = (Integer)ExecuteExceptionBB.getBB()
            .getSharedMap().get(CACHE_CLOSED_VM);
        otherVMs.remove(cacheClosedVmId);
      }
      return new ClientVmInfo((Integer)otherVMs.get(0));
    }
    else {
      List endpoints = BridgeHelper.getEndpoints();
      Random rand = new Random();
      int index = rand.nextInt(endpoints.size() - 1);
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints
          .get(index);
      ClientVmInfo target = new ClientVmInfo(endpoint);
      return target;
    }
  }

  /**
   * Task to initialize the cache and the region
   */
  protected void initialize(String regionDescriptName) {
    theCache = CacheHelper.createCache("cache1");
    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    Log.getLogWriter().info("Creating region " + regionName);
    RegionAttributes attributes = RegionHelper
        .getRegionAttributes(regionDescriptName);
    String poolName = attributes.getPoolName();
    if (poolName != null) {
      PoolHelper.createPool(poolName);
    }
    aRegion = theCache.createRegion(regionName, attributes);
    Log.getLogWriter().info("Completed creating region " + aRegion.getName());
  }

  /**
   * Task for the client to do crash and come up
   */
  public void clientNormalShutDown() {
    TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
        "ExecuteExceptionBB.signalCacheClose",
        ExecuteExceptionBB.signalCacheClose, 1, true, 60000);
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      int myVmid = RemoteTestModule.getMyVmid();
      ExecuteExceptionBB.getBB().getSharedMap().put(CACHE_CLOSED_VM, myVmid);
      Log.getLogWriter().info("Closing cache");
      theCache.close();
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    ExecuteExceptionBB.getBB().getSharedCounters().increment(
        ExecuteExceptionBB.cacheCloseCompleted);
    TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
        "ExecuteExceptionBB.signalCacheCreate",
        ExecuteExceptionBB.signalCacheCreate, 1, true, 60000);
    initialize("dataStore");
    populateRegion(aRegion);
    ExecuteExceptionBB.getBB().getSharedCounters().increment(
        ExecuteExceptionBB.cacheCreateCompleted);
  }

  /**
   * Task to populate the region
   */
  protected void populateRegion(Region region) {
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

  /**
   * Task to populate the region with custom partitioning (node pruning test)
   */
  protected void populateRegionWithCustomPartition(Region region) {
    for (int i = 0; i < NUM_KEYS; i++) {
      String keyName = "Key "
          + ParRegBB.getBB().getSharedCounters().incrementAndRead(
              ParRegBB.numOfPutOperations);
      Month routingObjectHolder = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      KeyResolver key = new KeyResolver(keyName, routingObjectHolder);
      Integer value = new Integer(i);
      region.put(key, value);
    }
    Log.getLogWriter().info(
        "Completed put for " + NUM_KEYS + " keys and region size is "
            + region.size());
  }

  /**
   * Task to update the BB with the buckets on each nodes and total buckets in
   * the PR
   */
  protected void updateBBWithNodes(Region region) {
    if (!(region instanceof PartitionedRegion)) {
      throw new TestException("This test should be using partitioned region");
    }
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    String localVM = ds.getDistributedMember().getId();

    List bucketListOnNode = ((PartitionedRegion)region)
        .getLocalBucketsListTestOnly();
    ParRegBB.getBB().getSharedMap().put(localVM, bucketListOnNode);

    HashSet allBucketIds;
    if (ParRegBB.getBB().getSharedMap().get(ALL_BUCKET_IDS) == null) {
      allBucketIds = new HashSet();
      allBucketIds.addAll(bucketListOnNode);
    }
    else {
      allBucketIds = (HashSet)ParRegBB.getBB().getSharedMap().get(
          ALL_BUCKET_IDS);
      allBucketIds.addAll(bucketListOnNode);
    }
    ParRegBB.getBB().getSharedMap().put(ALL_BUCKET_IDS, allBucketIds);
  }
  
  /**
   * Task to update the BB with the buckets on each nodes and total buckets in
   * the PR
   */
  protected void updateBB(Region region) {
    if (!(region instanceof PartitionedRegion)) {
      throw new TestException("This test should be using partitioned region");
    }
    if (ParRegBB.getBB().getSharedMap().get(KEY_SET) == null) {
      Set keySet = new HashSet(((PartitionedRegion)region).keySet());
      ParRegBB.getBB().getSharedMap().put(KEY_SET, keySet);

      Set singleBucketKeySet;

      try {
        do {
          singleBucketKeySet = new HashSet(((PartitionedRegion)region)
              .getSomeKeys(new Random(20)));
        } while (singleBucketKeySet == null || singleBucketKeySet.size() == 0);
        ParRegBB.getBB().getSharedMap().put(BUCKET_KEYS, singleBucketKeySet);
      }
      catch (Exception e) {
        throw new TestException(
            "Test issue - got the exception during partitionedRegion.getSomeKeys(rand)");
      }
    }
  }
  
  /**
   * Task to do verify whether the re-execution in case of customised
   * re-execution exceptions (in case of all buckets).
   * 
   * @param region
   */
  protected void doReExecuteExceptionAllBuckets(Region region) {

    Execution execution = FunctionService.onRegion(region);
    ArrayList list;

    try {
      list = (ArrayList)execution.execute(new ReExecutingFunction())
          .getResult();
    }
    catch (Exception exception) {
      throw new TestException(
          "Caught the exception during the execution of the function ReExecutionFunction ",
          exception);
    }

    if (list.size() == 0) {
      throw new TestException(
          "Re-executed function should have returned results. But the result list is empty");
    }

    // Work around for the bug 41298
    //execution = FunctionService.onRegion(region);

    try {
      list = (ArrayList)execution.execute(new NonReExecutingFunction())
          .getResult();
      throw new TestException(
          "NonReExecutingFunction should have thrown exception as isHA() is set false");
    }
    catch (Exception exception) {
      if ((exception.getCause() instanceof FunctionInvocationTargetException)
          && (exception.getCause().getCause() instanceof IllegalStateException)) {
        hydra.Log.getLogWriter().info(
            "Got the expected exception " + exception.getMessage());
      }
      else {
        throw new TestException("Got the exception " , exception);
      }
    }
  }
  
  
  /**
   * Task to do verify whether the re-execution in case of customised
   * re-execution exceptions (in case of multiple buckets).
   * 
   * @param region
   */
  protected void doReExecuteExceptionMultipleBuckets(Region region) {

    Set keySet = region.keySet();
    Execution execution = FunctionService.onRegion(region).withFilter(keySet);
    ArrayList list;

    try {
      list = (ArrayList)execution.execute(new ReExecutingFunction()).getResult(
          100, TimeUnit.SECONDS);
    }
    catch (Exception exception) {
      throw new TestException(
          "Caught the exception during the execution of the function ReExecutionFunction "
              + exception);
    }

    if (list.size() == 0) {
      throw new TestException(
          "Re-executed function should have returned results. But the result list is empty");
    }

    // Work around for the bug 41298
    //execution = FunctionService.onRegion(region).withFilter(keySet);

    try {
      list = (ArrayList)execution.execute(new NonReExecutingFunction())
          .getResult(100, TimeUnit.SECONDS);
      throw new TestException(
          "NonReExecutingFunction should have thrown exception as isHA() is set false");
    }
    catch (Exception exception) {
      if ((exception.getCause() instanceof FunctionInvocationTargetException)
          && (exception.getCause().getCause() instanceof IllegalStateException)) {
        hydra.Log.getLogWriter().info(
            "Got the expected exception " + exception.getMessage());
      }
      else {
        throw new TestException("Got the exception " , exception);
      }
    }
  }
  
  /**
   * Task to do verify whether the re-execution in case of customised
   * re-execution exceptions (in case of single bucket).
   * 
   * @param region
   */
  protected void doReExecuteExceptionSingleBucket(Region region) {

    Set keySet;

    if (region.getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
      keySet = (Set)ParRegBB.getBB().getSharedMap().get(BUCKET_KEYS);
    }
    else {
      try {
        do {
          keySet = ((PartitionedRegion)region).getSomeKeys(new Random(20));
        } while (keySet == null || keySet.size() == 0);
      }
      catch (Exception e) {
        throw new TestException("Test issue - Caught exception " , e);
      }
    }

    Execution execution = FunctionService.onRegion(region).withFilter(keySet);
    ArrayList list;

    try {
      list = (ArrayList)execution.execute(new ReExecutingFunction())
          .getResult();
    }
    catch (Exception exception) {
      throw new TestException(
          "Caught the exception during the execution of the function ReExecutionFunction "
              + exception);
    }

    if (list.size() != 1) {
      throw new TestException(
          "Re-executed function should have executed on one node. But gave results for "
              + list.size() + " nodes");
    }

    // Work around for the bug 41298
    //execution = FunctionService.onRegion(region).withFilter(keySet);

    try {
      list = (ArrayList)execution.execute(new NonReExecutingFunction())
          .getResult(100, TimeUnit.SECONDS);
      throw new TestException(
          "NonReExecutingFunction should have thrown exception as isHA() is set false");
    }
    catch (Exception exception) {
      if ((exception.getCause() instanceof FunctionInvocationTargetException)
          && (exception.getCause().getCause() instanceof IllegalStateException)) {
        hydra.Log.getLogWriter().info(
            "Got the expected exception " + exception.getMessage());
      }
      else {
        throw new TestException("Got the exception " , exception);
      }
    }
  }

}
