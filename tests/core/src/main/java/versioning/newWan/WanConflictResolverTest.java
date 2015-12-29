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
package versioning.newWan;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.ClientVmMgr;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import newWan.WANBlackboard;
import newWan.WANOperationsClient;
import newWan.WANOperationsClientBB;
import newWan.WANOperationsClientPrms;
import newWan.WANTest;
import newWan.WANTestPrms;
import util.PRObserver;
import util.SilenceListener;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;

/**
 * Provide support methods to test conflict detection of new wan events. 
 * @author rdiyewar 
 * @since 7.0 
 *
 */
public class WanConflictResolverTest extends WANTest {

  public static WanConflictResolverTest testInstance;
  
  public static Boolean singleThreadPerVMValidation = new Boolean(true); // flag to do validation by single thread in a vm  
  
  String ENTRY_OPS_PREFIX = "EntryOps: ";  
  int waitSec = TestConfig.tasktab().intAt(WANOperationsClientPrms.waitForSilenceListenerSec,
                           TestConfig.tab().intAt(WANOperationsClientPrms.waitForSilenceListenerSec, 30));
  
  protected void initialize(){
    testInstance.opsClient = new WANOperationsClient();       
    
    //initialise PRObserver for recovery/rebalance monitoring
    PRObserver.installObserverHook();
      
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));      
      regionDescriptNames.add(regionDescriptName);      
    }    
    testInstance.isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
  }
  
  /**
   * task to initialize server cache
   */
  public synchronized static void HydraTask_initServerTask(){
    if(testInstance == null){
      testInstance = new WanConflictResolverTest();
      testInstance.initialize();
      testInstance.initPeerCache();
      testInstance.initGatewaySender();
      testInstance.initDatastoreRegion();
      testInstance.initGatewayReceiver();
      testInstance.startGatewaySender();
      
      String bridgeConfig = ConfigPrms.getBridgeConfig();
      if(bridgeConfig != null){
        BridgeHelper.startBridgeServer(bridgeConfig);
      }
    }
  }
  
  /**
   * task to initialize edge cache.
   */
  public synchronized static void HydraTask_initEdgeTask() {
    if(testInstance == null){
      testInstance = new WanConflictResolverTest();
      testInstance.initialize();
      testInstance.isBridgeClient = true;
      testInstance.initEdgeClientCache();
    }    
  }
  
  public static void HydraTask_doOpsKillAndValidateHA () {
    testInstance.doOpsKillAndValidateHA();
  }
  
  public static void HydraTask_doOpsAndValidateHA () {
    testInstance.doOpsAndValidateHA();
  }
  
  public static void HydraTask_verifyResolvedEvent (){
    testInstance.verifyResolvedEvents();
  }
  /**
   * This task runs in coordination with doOpsAndValidateHA in multiple cycles. 
   * In each cycle, one thread from a thread group running doOpsKillAndValidateHA 
   * is a coordinator, rest all threads from a group runs doOpsAndValidateHA.
   * The task in conf must set maxThreads = 1. 
   * HydraTask_doOpsAndValidateHA does 
   *    1) waits for others to enter HydraTask_doOpsHA
   *    2) Wait for silence listener
   *    3) Write snapshot to BB and invoke all other threads to validate region
   *    4) Do validate the region data
   *    5) wait for others to finished validation
   *    6) Do entry operations and invoke others to do entry operations
   *    7) Do failover if enabled
   *    8) Check for termination condition
   *    9) Reset all counters as a cycle is completed.
   */
  public void doOpsKillAndValidateHA() {
    SharedCounters sc = WANOperationsClientBB.getBB().getSharedCounters();
    int numThreadsForTask = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
        
    // wait till one cycle of operation is done.
    long cycleCounter = sc.incrementAndRead(WANOperationsClientBB.NumCycle);
    long entryCount = sc.incrementAndRead(WANOperationsClientBB.NumStartedDoingOps);
    logger.info(ENTRY_OPS_PREFIX + "Started as leader with entryCount=" + entryCount + " for cycleCounter=" 
                + cycleCounter + ", waiting for others to enter.");
    
    // Enable others to start operations
    // wait till every body else started the operations. 
    sc.setIfLarger(WANOperationsClientBB.IsReady, 1);
    TestHelper.waitForCounter(WANOperationsClientBB.getBB(),"WANOperationsClientBB.NumStartedDoingOps",
        WANOperationsClientBB.NumStartedDoingOps, numThreadsForTask, true, 500000);
    logger.info(ENTRY_OPS_PREFIX + "All threads have entered in current execution cycle cycleCounter=" + cycleCounter);
    
    // block others till current cycle is not completed
    sc.zero(WANOperationsClientBB.IsReady);
    WANOperationsClientBB.getBB().getSharedCounters().zero(WANOperationsClientBB.InvokeEntryOperation);
    
    // wait for silence listener
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Waiting for Silence Listener for " + waitSec + " seconds ...");
    SilenceListener.waitForSilence(waitSec, 5000);
  
    // write snapshot to bb
    opsClient.writeRegionSnapshotToBB();
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Done writing snapshot to BB, cycleCounter=" + cycleCounter);
    
    // invoke validations
    sc.setIfLarger(WANOperationsClientBB.InvokeValidation, 1);
    validateEntryOperationFromBBSnapshot();
    long validate = sc.incrementAndRead(WANOperationsClientBB.NumDoneValidation);
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Done validating snapshot, validateCounter=" + validate + " cycleCounter=" + cycleCounter);
    MasterController.sleepForMs(700); // sleep so that other threads get sufficient time to invoke validation.
    
    // wait for every one finish validation
    TestHelper.waitForCounter(WANOperationsClientBB.getBB(),"WANOperationsClientBB.NumDoneValidation",
        WANOperationsClientBB.NumDoneValidation, numThreadsForTask, true, 500000);    
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Done everyone validating the snapshot, cycleCounter=" + cycleCounter);
    
    // now disable validation
    WANOperationsClientBB.getBB().getSharedCounters().zero(WANOperationsClientBB.InvokeValidation);
    
    // Enable others to do entry operations
    WANOperationsClientBB.getBB().getSharedCounters().setIfLarger(WANOperationsClientBB.InvokeEntryOperation, 1);
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Enabling entry operations, cycleCounter=" + cycleCounter);
    
    if (WANOperationsClientPrms.enableFailover()){
      // restart vms
      int numVMsToStop = TestConfig.tasktab().intAt(StopStartPrms.numVMsToStop,
          TestConfig.tab().intAt(StopStartPrms.numVMsToStop));
      if (numVMsToStop <= 0) {
        throw new TestException("doOpsKillAndValidateHA: Not doing anything as number of vm requested to kill is " + numVMsToStop);
      }
  
      String clientName = WANOperationsClientPrms.getClientNamePrefixForHA();
      if (clientName == null) {
        throw new TestException("newWan.WANOperationsClientPrms-clientNamePrefixForHA can not be null : " + clientName);
      }
  
      PRObserver.initialize();
      logger.info(ENTRY_OPS_PREFIX + "doOpsKillAndValidateHA: Looking for " + numVMsToStop + " vm with client name " + clientName);
      Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, clientName);
      List vmList = (List)(tmpArr[0]);
      List stopModeList = (List)(tmpArr[1]);
      List otherVms = (List)(tmpArr[2]);
      // this asychronously starts the thread to stop and start
      List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);
  
      // meanwhile do operations
      doEntryOperationAllRegions();
      long numDoneOps = sc.incrementAndRead(WANOperationsClientBB.NumFinishedDoingOps);
      logger.info(ENTRY_OPS_PREFIX + "Done entry operation, counter is " + numDoneOps + " in current cycle, cycleCounter=" + cycleCounter);
      
      Iterator itr = stopModeList.iterator();
      while(itr.hasNext()){
        String stopMode = (String)itr.next();
        if (ClientVmMgr.toStopMode(stopMode) == ClientVmMgr.NICE_KILL) {
          sc.increment(WANOperationsClientBB.NumFinishedDoingOps);
        }  
      }

      // wait for restarted vm to join
      StopStartVMs.joinStopStart(vmList, threads);
      logger.info(ENTRY_OPS_PREFIX + "doOpsKillAndValidateHA: done restarting " + vmList);
      
      // wait to recovery after vm restart
      for (int j = 0; j < regionDescriptNames.size(); j++) {
        String regionDescriptName = (String)(regionDescriptNames.get(j));
        if (!regionDescriptName.startsWith("client")
            && !regionDescriptName.startsWith("accessor")) {
          RegionDescription rd = RegionHelper
              .getRegionDescription(regionDescriptName);
          DataPolicy policy = rd.getDataPolicy();
          if (policy.equals(DataPolicy.PARTITION)
              || policy.equals(DataPolicy.PERSISTENT_PARTITION)) {
            waitForRecovery(vmList, otherVms);
            break;
          }
        }
      }
    } else{
      // just do operations
      doEntryOperationAllRegions();
      long numDoneOps = sc.incrementAndRead(WANOperationsClientBB.NumFinishedDoingOps);
      logger.info(ENTRY_OPS_PREFIX + "Done entry operation, counter is " + numDoneOps + " in current cycle, cycleCounter=" + cycleCounter);
    }
  
    // wait for others to finish entry operations. also check if task scheduling is stopped
    long startTime = System.currentTimeMillis();
    long endTime = startTime + 600000;
    boolean doOpsFinishied = false;
    while(!doOpsFinishied) {
      try{
        TestHelper.waitForCounter(WANOperationsClientBB.getBB(),"WANOperationsClientBB.NumFinishedDoingOps",
            WANOperationsClientBB.NumFinishedDoingOps, numThreadsForTask, false, 30000);  
        doOpsFinishied = true;
      }catch (TestException te){        
        Boolean isStopEvent = (Boolean)WANOperationsClientBB.getBB().getSharedMap().get(WANOperationsClientBB.IS_TASK_SCHEDULING_STOPPED);
        if(isStopEvent != null && isStopEvent.booleanValue() == true){
          //StopSchedulingOrder has called
          doOpsFinishied = true;
        }
      }
      
      long time = System.currentTimeMillis();
      if(time >= endTime){
        long numfinished = sc.read(WANOperationsClientBB.NumFinishedDoingOps);
        throw new TestException("After waiting for 600000 millis, newWan.WANOperationsClientBB.WANOperationsClientBB.NumFinishedDoingOps has value of "
            + numfinished + ", but expected it to become " + numThreadsForTask);
      }
    }

    logger.info(ENTRY_OPS_PREFIX + "Finished current cycle. Zeroing NumFinishedDoingOps and NumStartedDoingOps. cycleCounter=" + cycleCounter);
  
    //check for termination condition
    opsClient.checkForTermicationCondition();
    
    // reset numDoingOps counter, and increment cycle counter
    sc.zero(WANOperationsClientBB.NumFinishedDoingOps);
    sc.zero(WANOperationsClientBB.NumStartedDoingOps);
    sc.zero(WANOperationsClientBB.NumDoneValidation);
    
  }
  
  /**
   * This task runs in coordination with doOpsKillAndValidateHA in multiple cycles.
   * The threadgoup should be same for doOpsKillAndValidateHA and doOpsAndValidateHA
   * doOpsAndValidateHA does following
   *    1) Wait for validation condition 
   *    2) Do validation
   *    3) Wait for entry operation condition
   *    4) Do entry operations
   */
  public void doOpsAndValidateHA() { 
    SharedCounters sc = WANOperationsClientBB.getBB().getSharedCounters();
    
    long isready =  sc.read(WANOperationsClientBB.IsReady);
    if(isready == 0){
      logger.info("returning from doOpsAndValidateHA as WANOperationsClientBB.IsReady=0");
      return;
    }

    long cycleCounter = sc.read(WANOperationsClientBB.NumCycle);
    long entryCount = sc.incrementAndRead(WANOperationsClientBB.NumStartedDoingOps);
    logger.info(ENTRY_OPS_PREFIX + "Entering with entryCount=" + entryCount + " for cycleCounter=" + cycleCounter); 
    
    // wait for validations condition
    TestHelper.waitForCounter(WANOperationsClientBB.getBB(),"WANOperationsClientBB.InvokeValidation",
        WANOperationsClientBB.InvokeValidation, 1, true, 600000, 200);
    validateEntryOperationFromBBSnapshot();     
    long validate = sc.incrementAndRead(WANOperationsClientBB.NumDoneValidation);
    Log.getLogWriter().info(ENTRY_OPS_PREFIX + "Done validation, counter is " + validate + " in current cycle, cycleCounter=" + cycleCounter);
    
    // wait for invoke notification for entry operations
    TestHelper.waitForCounter(WANOperationsClientBB.getBB(),"WANOperationsClientBB.InvokeEntryOperation",
        WANOperationsClientBB.InvokeEntryOperation, 1, true, 500000);

    //do entry operations
    doEntryOperationAllRegions();    
    long numDoneOps = sc.incrementAndRead(WANOperationsClientBB.NumFinishedDoingOps);
    logger.info(ENTRY_OPS_PREFIX + "Done entry operation, counter is " + numDoneOps + " in current cycle, cycleCounter=" + cycleCounter);
  }

  /**
   * verify wan event resolved by resolver
   */
  public void verifyResolvedEvents(){
    if (WANOperationsClientPrms.getTaskTerminationMethod() == WANOperationsClientPrms.NUM_EVENT_RESOLVED){
      int expectedEventResolved = WANOperationsClientPrms.getTaskTerminatorThreshold();
      long eventResolved = WANOperationsClientBB.getBB().getSharedCounters().read(WANOperationsClientBB.WanEventResolved);
      if (eventResolved < expectedEventResolved){
        throw new TestException ("Wan event resolved are " + eventResolved + ", does not reached to expected " + expectedEventResolved);
      }
    }
  }
  
  /**
   * validate all regions against snapshot from blackboard 
   */
  protected void validateEntryOperationFromBBSnapshot() {
    if (singleThreadPerVMValidation.booleanValue()) {
      singleThreadPerVMValidation = false;
      Log.getLogWriter().info("validateEntryOperationFromBBSnapshot: Validation started");

      Region aRegion = null;
      Set rootRegions = CacheHelper.getCache().rootRegions();
      for (Iterator rit = rootRegions.iterator(); rit.hasNext();) {
        aRegion = (Region)rit.next();
        opsClient.verifyRegionContents(aRegion, opsClient.getBBSnapshot(aRegion));
      }
      Log.getLogWriter().info("validateDoEntryOperation: Validation complete");
      singleThreadPerVMValidation = true;
    }
  }
}
