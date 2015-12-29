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

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import parReg.ParRegUtil;
import rebalance.RebalanceBB;
import recovDelay.BucketState;
import util.PRObserver;
import util.TestException;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.execute.InternalExecution;

public class FunctionExecutionWithRebalancingTest extends ExecutionAndColocationTest {

  public static final String EXTRA_VM_IDS = "Extra_Vm_Ids";

  public static final String NEXT_VM_TO_BE_UP = "Next_Vm_To_Be_Up";

  private static final AtomicInteger opId = new AtomicInteger(0);

  private static FunctionExecutionWithRebalancingTest getTestInstance() {
    return (FunctionExecutionWithRebalancingTest)testInstance;
  }

  public synchronized static void HydraTask_HA_dataStoreInitialize() {
    if (testInstance == null) {
      RebalanceBB.getBB().getSharedCounters().increment(
          RebalanceBB.numDataStores);
    }
    ExecutionAndColocationTest.HydraTask_HA_dataStoreInitialize();

  }

  public static void HydraTask_addCapacityAndReBalance() {

    if (testInstance == null) {
      testInstance = new FunctionExecutionWithRebalancingTest();
    }

    boolean isNextTurn = ((FunctionExecutionWithRebalancingTest)testInstance)
        .isNextTurn();

    if (isNextTurn) {
      hydra.Log.getLogWriter().info("Got true, going to add capacity");
      ((FunctionExecutionWithRebalancingTest)testInstance)
          .addCapacityAndReBalance();
      HydraTask_createTheNextTurn();
      throw new StopSchedulingTaskOnClientOrder(
          "Completed the rebalancing for the node");
    }

  }

  public synchronized static void HydraTask_updateBBWithExtraVmIds() {
    Integer myVmId = new Integer(RemoteTestModule.getMyVmid());
    Set extraVmIdsSet;

    if (RebalanceBB.getBB().getSharedMap().get(EXTRA_VM_IDS) == null) {
      extraVmIdsSet = new HashSet();
    }
    else {
      extraVmIdsSet = (HashSet)RebalanceBB.getBB().getSharedMap().get(
          EXTRA_VM_IDS);
    }

    extraVmIdsSet.add(myVmId);
    RebalanceBB.getBB().getSharedMap().put(EXTRA_VM_IDS, extraVmIdsSet);
    RebalanceBB.getBB().printSharedMap();
  }

  public synchronized static void HydraTask_createTheNextTurn() {
    Log.getLogWriter().info(
        "HydraTask_createTheNextTurn"
            + RebalanceBB.getBB().getSharedMap().get(EXTRA_VM_IDS).toString());
    Set extraVmIdsSet = (HashSet)RebalanceBB.getBB().getSharedMap().get(
        EXTRA_VM_IDS);

    Iterator extraVmSetIterator = extraVmIdsSet.iterator();
    Integer nextVmToBeUp = null;

    if (extraVmSetIterator.hasNext()) {
      nextVmToBeUp = (Integer)extraVmSetIterator.next();
    }

    if (nextVmToBeUp != null) {
      RebalanceBB.getBB().getSharedMap().put(NEXT_VM_TO_BE_UP, nextVmToBeUp);
      extraVmIdsSet.remove(nextVmToBeUp);
      RebalanceBB.getBB().getSharedMap().put(EXTRA_VM_IDS, extraVmIdsSet);
      hydra.Log.getLogWriter().info(
          "Next Vm id to do rebalance is " + nextVmToBeUp);
    }
  }
  
  public static void HydraTask_doFEWithExceptionsAndVerify() {
    ((FunctionExecutionWithRebalancingTest)testInstance)
        .doFEWithExceptionsAndVerify();
  }

  public boolean isNextTurn() {
    hydra.Log.getLogWriter().info(
        "In isNextTurn() for the vm " + RemoteTestModule.getMyVmid());
    Integer nextTurnVmId = (Integer)RebalanceBB.getBB().getSharedMap().get(
        NEXT_VM_TO_BE_UP);
    hydra.Log.getLogWriter().info("next vm is " + nextTurnVmId);

    Integer myVmId = new Integer(RemoteTestModule.getMyVmid());

    if ((nextTurnVmId != null) && (nextTurnVmId.equals(myVmId))) {
      hydra.Log.getLogWriter().info("returning true");
      return true;
    }
    else {
      hydra.Log.getLogWriter().info("returning false");
      return false;
    }
  }

  public void addCapacityAndReBalance() {

    PRObserver.installObserverHook();
    Integer myVmId = new Integer(RemoteTestModule.getMyVmid());

    super.initInstance("dataStoreRegion");
    Log.getLogWriter().info(
        "After creating region, region size is " + aRegion.size());
    long numDataStores = RebalanceBB.getBB().getSharedCounters()
        .incrementAndRead(RebalanceBB.numDataStores);
    long numDataStoresBefore = numDataStores - 1;

    // wait for recovery to run in this vm
    if (aRegion.getAttributes().getPartitionAttributes().getRedundantCopies() > 0) {
      PRObserver.waitForRebalRecov(myVmId, 1, 1, null, null, false);
    }

    // rebalance to cause this new vm to be used in the PR and gain buckets
    Map[] mapArr = BucketState.getBucketMaps(aRegion);
    Map primaryMap = mapArr[0];
    Map bucketMap = mapArr[1];
    Log.getLogWriter().info(
        "Before rebalance, buckets: " + bucketMap + ", primaries: "
            + primaryMap);
    if (bucketMap.size() != numDataStoresBefore) {
      throw new TestException("Expected " + numDataStoresBefore
          + " vms to contain buckets, but " + bucketMap.size()
          + " vms contains buckets, buckeMap: " + bucketMap + ", primaryMap: "
          + primaryMap);
    }
    ParRegUtil.doRebalance();

    // verify after rebalance
    mapArr = BucketState.getBucketMaps(aRegion);
    primaryMap = mapArr[0];
    bucketMap = mapArr[1];
    Object value = primaryMap.get(myVmId);
    if (value == null) {
      primaryMap.put(myVmId, new Integer(0));
    }
    value = bucketMap.get(myVmId);
    if (value == null) {
      bucketMap.put(myVmId, new Integer(0));
    }
    Log.getLogWriter()
        .info(
            "After rebalance, buckets: " + bucketMap + ", primaries: "
                + primaryMap);
    if (((value == null) || ((Integer)value).intValue() == 0)) {
      throw new TestException(
          "New capacity vm did not gain any buckets, bucketMap " + bucketMap
              + ", primaryMap " + primaryMap);
    }
    if (bucketMap.size() != numDataStores) {
      throw new TestException("Expected " + numDataStores
          + " vms to contain buckets, but " + bucketMap.size()
          + " vms contains buckets, buckeMap: " + bucketMap + ", primaryMap: "
          + primaryMap);
    }
  }
  
  public void doFEWithExceptionsAndVerify() {
    doFuncExecsAndVerify();
  }

  public void doFuncExecsAndVerify() {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) {
      doFunctionExecutionAndVerifyResults((InternalExecution)FunctionService
          .onMembers(ds), new MemberResultsCollector());
    }
    if (aRegion != null) {
      doFunctionExecutionAndVerifyResults((InternalExecution)FunctionService
          .onRegion(aRegion), new MemberResultsCollector());
    }
  }

  public void doFunctionExecutionAndVerifyResults(InternalExecution dataSet,
      MemberResultsCollector resultCollector) {
    final int thisOpId = opId.incrementAndGet();
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.exceptionNodes);
    ExecuteExceptionBB.getBB().getSharedCounters().zero(
        ExecuteExceptionBB.sendResultsNodes);
    InternalExecution finalDataSet = (InternalExecution)dataSet.withCollector(
        resultCollector).withArgs(
        new PartialResultsExecutionFunction.SenderIdWithOpId(RemoteTestModule
            .getMyVmid(), thisOpId));
    finalDataSet.setWaitOnExceptionFlag(true);
    ResultCollector rc = null;

    try {
      hydra.Log.getLogWriter().info("Going to execute for opId=" + thisOpId);
      rc = finalDataSet.execute(new PartialResultsExecutionFunction());
      rc.getResult();
    }
    catch (FunctionException e) {
      hydra.Log.getLogWriter().info("Caught Exception " + e);
      Log.getLogWriter().info(
          "Result is " + resultCollector.getPartialResults());
      Log.getLogWriter().info("Exceptions are " + e.getExceptions());
      long numOfExceptionNodes = ExecuteExceptionBB.getBB().getSharedCounters()
          .read(ExecuteExceptionBB.exceptionNodes);
      long numOfSendResultNodes = ExecuteExceptionBB.getBB()
          .getSharedCounters().read(ExecuteExceptionBB.sendResultsNodes);
      if (e.getExceptions().size() != numOfExceptionNodes) {
        throw new TestException("Did not receive all the relevant exceptions "
            + numOfExceptionNodes + " and " + e.getExceptions());
      }
      // The following may not work always - what if exception comes first?
      //      
      // long expectedResultSize = numOfExceptionNodes + numOfSendResultNodes;
      // long receivedResultSize =
      // ((HashMap)resultCollector.getPartialResults())
      // .size();
      // if (receivedResultSize != expectedResultSize) {
      // throw new TestException("Expected results from " + expectedResultSize
      //            + " nodes but received " + receivedResultSize);
      //      }
    }

  }

}
