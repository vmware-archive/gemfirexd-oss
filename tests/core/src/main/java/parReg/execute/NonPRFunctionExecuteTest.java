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
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import parReg.ParRegBB;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionService;

public class NonPRFunctionExecuteTest {

  protected static NonPRFunctionExecuteTest testInstance;

  protected static Cache theCache;

  protected static Region<String, Integer> aRegion;
  
  protected static PartitionedRegion partitionedRegion;

  public static final int NUM_KEYS = 50;

  public static final String REG_NAMES = "Region names";

  public static synchronized void HydraTask_StartTask() {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    regionNames.remove("partitionedRegion");
    ParRegBB.getBB().getSharedMap().put(REG_NAMES, regionNames);
  }

  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
      testInstance.initialize("region");
    }
  }

  public synchronized static void HydraTask_initializeWithDifferentDP() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    Vector regionNames = (Vector)ParRegBB.getBB().getSharedMap().get(REG_NAMES);
    String regionForThisNode = (String)regionNames.remove(0);
    Log.getLogWriter().info(
        "Region description for this node " + regionForThisNode);
    testInstance.initialize(regionForThisNode);
    testInstance.initialize("partitionedRegion");
    ParRegBB.getBB().getSharedMap().put(REG_NAMES, regionNames);
  }
  
  public synchronized static void HydraTask_initializeServerWithDifferentDP() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    Vector regionNames = (Vector)ParRegBB.getBB().getSharedMap().get(REG_NAMES);
    String regionForThisNode = (String)regionNames.remove(0);
    if (regionForThisNode.equals("edge")) {
      regionForThisNode = (String)regionNames.remove(0);
    }
    Log.getLogWriter().info(
        "Region description for this node " + regionForThisNode);
    testInstance.initialize(regionForThisNode);
    BridgeHelper.startBridgeServer("bridge");
    ParRegBB.getBB().getSharedMap().put(REG_NAMES, regionNames);
  }
  
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    testInstance.initialize("edge");
  }

  public static void HydraTask_populateRegion() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    testInstance.populateRegion(aRegion);
    
    if (partitionedRegion != null) {
      testInstance.populateRegion(partitionedRegion);
    }

  }

  public static void HydraTask_registerFunction() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    FunctionService.registerFunction(new NonPRFunction());
    FunctionService.registerFunction(new NonPRHctTestFunction());
    FunctionService.registerFunction(new ReExecutingFunction());
    FunctionService.registerFunction(new NonReExecutingFunction());
  }

  public static void HydraTask_executeFunction() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    testInstance.executeFunction();
  }
  
  public static void HydraTask_reExecuteFunctionDistributedRegion() {
    if (aRegion.getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
      hydra.Log
          .getLogWriter()
          .info(
              "This node has the region with normal datapolicy. Hence not executing.");
      return;
    }
    testInstance.reExecuteFunctionDistributedRegion();
  }
  
  public static void HydraTask_clientReExecuteFunction(){
    testInstance.reExecuteFunctionDistributedRegion();
  }
  
  public static void HydraTask_executeOnRegionsFunction() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    testInstance.executeOnRegionsFunction();
  }
  
  public static void HydraTask_clientsExecuteFunction() {
    if (testInstance == null) {
      testInstance = new NonPRFunctionExecuteTest();
    }
    testInstance.clientsExecuteFunction();
  }

  protected void initialize(String regionDescriptName) {
    if (theCache == null)
      theCache = CacheHelper.createCache("cache1");
    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    Log.getLogWriter().info("Creating region " + regionName);
    RegionAttributes attributes = RegionHelper.getRegionAttributes(regionDescriptName);
    String poolName = attributes.getPoolName();
    if (poolName != null) {
      PoolHelper.createPool(poolName);
    }
    
    if(regionDescriptName.equals("partitionedRegion")){
      partitionedRegion = (PartitionedRegion)theCache.createRegion(regionName, RegionHelper
        .getRegionAttributes(regionDescriptName));
    }else{
      aRegion = theCache.createRegion(regionName, RegionHelper
          .getRegionAttributes(regionDescriptName));
    }
    Log.getLogWriter().info("Completed creating region " + aRegion.getName());
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

  protected void executeFunction() {
    Execution dataSet = FunctionService.onRegion(aRegion).withCollector(
        new ArrayListResultCollector());

    ArrayList list = new ArrayList();

    // Function execution with no args (for empty and normal)
    if (aRegion.getAttributes().getDataPolicy() == DataPolicy.NORMAL
        || aRegion.getAttributes().getDataPolicy() == DataPolicy.EMPTY) {
      try {
        list = (ArrayList)dataSet.execute(new NonPRFunction().getId())
            .getResult();
      }
      catch (Exception e) {
        if ((aRegion.getAttributes().getDataPolicy() == DataPolicy.NORMAL)
            && (e instanceof FunctionException)
            && e
                .getMessage()
                .contains(
                    "Function execution on region with DataPolicy.NORMAL is not supported")) {
          Log
              .getLogWriter()
              .info(
                  "Got the expected unsupported operation exception for datapolicy normal.");
          return;
        }
        else {
          throw new TestException(
              "Got the exception during function execution ", e);
        }
      }

      if (aRegion.getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
        throw new TestException(
            "Function execution did not throw Unsupported operation exception with datapolicy normal but returned result of size "
                + list.size());
      }
    }// Funcion execution with args for replicate region
    else if (aRegion.getAttributes().getDataPolicy() == DataPolicy.REPLICATE) {
      DistributedSystem ds = aRegion.getCache().getDistributedSystem();
      DistributedMember localVM = ds.getDistributedMember();

      try {
        list = (ArrayList)dataSet.withArgs((Serializable)localVM).execute(
            new NonPRFunction().getId()).getResult();
      }
      catch (Exception e) {
        throw new TestException("Caught exception during function execute ", e);
      }

    }
    else {
      throw new TestException("Illegal region dataPolicy "
          + aRegion.getAttributes().getDataPolicy());
    }

    Log.getLogWriter().info("List size is " + list.size());
    int expectedListSize = TestHelper.getNumVMs() * NUM_KEYS;

    if (list.size() != expectedListSize) {
      throw new TestException("Expected list size to be " + expectedListSize
          + " but received " + list.size());
    }
  }
  
  /**
   * Task to execute on regions()
   */
  protected void executeOnRegionsFunction() {
    HashSet regionSet = new HashSet();
    regionSet.add(aRegion);
    regionSet.add(partitionedRegion);
    Log.getLogWriter().info(
        "Executing on region with datapolicy "
            + aRegion.getAttributes().getDataPolicy());
    Execution dataSet = InternalFunctionService.onRegions(regionSet).withCollector(
        new ArrayListResultCollector());
    ArrayList list;
    try {
      list = (ArrayList)dataSet.execute(new NonPRFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Got exception ", e);
    }
    // Only one node of replicated and does not execute on normal and empty
    int expectedListSize = partitionedRegion.getAllNodes().size() + 1;

    if (list.size() != expectedListSize) {
      throw new TestException(
          "Expected the execution to have happened on "
              + expectedListSize
              + " (number of datastores + number of replicated nodes) but executed on "
              + list.size() + " and datapolicies are " + list);
    }
    else {
      Log.getLogWriter().info(
          "Executed on expected nodes with datapolicies " + list);
    }
  }
  
  protected void clientsExecuteFunction() {
    Execution dataSet = FunctionService.onRegion(aRegion).withCollector(
        new ArrayListResultCollector());

    ArrayList<Object> list = new ArrayList<Object>();

    try {
      list = (ArrayList<Object>)dataSet.execute(
          new NonPRHctTestFunction().getId()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Caught exception during function execute ", e);
    }

    Log.getLogWriter().info("List size is " + list.size());
    long expectedListSize = ParRegBB.getBB().getSharedCounters().read(
        ParRegBB.numOfPutOperations);

    if (list.size() != expectedListSize) {
      throw new TestException("Expected list size to be " + expectedListSize
          + " but received " + list.size());
    }
  }
  
  public void reExecuteFunctionDistributedRegion() {

    Execution execution = FunctionService.onRegion(aRegion);
    ArrayList list;

    try {
      list = (ArrayList)execution.execute(new ReExecutingFunction().getId())
          .getResult(100, TimeUnit.SECONDS);
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
    execution = FunctionService.onRegion(aRegion);

    try {
      list = (ArrayList)execution.execute(new NonReExecutingFunction().getId())
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


}
