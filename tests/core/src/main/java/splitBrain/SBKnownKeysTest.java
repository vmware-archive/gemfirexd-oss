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
package splitBrain; 

import hydra.*;
import parReg.*;
import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import getInitialImage.*;

public class SBKnownKeysTest extends KnownKeysTest {

protected Region controllerRegion;           // region used to exceed ack threshold

public synchronized static void HydraTask_HA_accessorInitialize() {
  if (testInstance == null) {
     testInstance = new SBKnownKeysTest();
     SBKnownKeysTest instance = (SBKnownKeysTest)testInstance;
     instance.initInstance("accessorRegion");
     if (isBridgeConfiguration) {
        ParRegUtil.registerInterest(testInstance.aRegion);
     }
     ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
     instance.controllerRegion = RegionHelper.createRegion("accessorControllerReg");
  }
}

public synchronized static void HydraTask_HA_dataStoreInitialize() {
  PRObserver.installObserverHook();
  if (testInstance == null) {
     testInstance = new SBKnownKeysTest();
     SBKnownKeysTest instance = (SBKnownKeysTest)testInstance;
     instance.initInstance("dataStoreRegion");
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                         new Integer(RemoteTestModule.getMyVmid()));
     ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
     Log.getLogWriter().info("Installing MembershipNotifierHook");
     SBUtil.addMembershipHook(new MembershipNotifierHook()); 
     instance.controllerRegion = RegionHelper.createRegion("dataStoreControllerReg");
  }
}

/** Take care of initialization done only once per run
 */
public static void HydraTask_initialize() {
   String clientName = RemoteTestModule.getMyClientName();
   if (clientName.indexOf("dataStore") >= 0) {
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.NumVMsInDS);
   } else if (clientName.indexOf("accessor") >= 0) {
      if (!isBridgeConfiguration) {
         SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.NumVMsInDS);
      }
   }
}

/** Hydra task to target a VM to become unhealthy.
 */
public static void HydraTask_causeUnhealthiness() {
   ((SBKnownKeysTest)testInstance).causeUnhealthiness();
   long counter = SplitBrainBB.getBB().getSharedCounters().read(SplitBrainBB.OpsComplete); 
   if (counter > 0) {
      throw new StopSchedulingTaskOnClientOrder("Ops tasks have signaled completion");
   }
}

/** Recreate a datastore region after a disconnect.
 */
public static void initDataStoreAfterDisconnect() {
   ((SBKnownKeysTest)testInstance).initInstance("dataStoreRegion");
   Log.getLogWriter().info(testInstance.aRegion.getFullPath() + " size is " + testInstance.aRegion.size());
   ((SBKnownKeysTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerReg");
   if (((SBKnownKeysTest)testInstance).isBridgeConfiguration) {
      BridgeHelper.startBridgeServer("bridge");
   }
}
    
public static void HydraTask_doOps() {
   try {
      KnownKeysTest.HydraTask_doOps();
      ControllerBB.checkForError();
   } catch (StopSchedulingTaskOnClientOrder order) {
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.OpsComplete); 
      throw order;
   }
}


/** Recreate a datastore region after a forced disconnect.
 */
public static void initDataStoreAfterForcedDisconnect() {
   ((SBKnownKeysTest)testInstance).initInstance("dataStoreRegion");
   Log.getLogWriter().info(testInstance.aRegion.getFullPath() + " size is " + testInstance.aRegion.size());
   ((SBKnownKeysTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerReg");
   if (((SBKnownKeysTest)testInstance).isBridgeConfiguration) {
      BridgeHelper.startBridgeServer("bridge");
   }
}

// ======================================================================== 
// method to handle ack threshold alerts caused by slow listeners

/** Cause a change in the health of target vms.
 *
 */
protected void causeUnhealthiness() {
   PRObserver.initialize();
   logExecutionNumber();
   int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
   SplitBrainBB.getBB().getSharedCounters().zero(SplitBrainBB.NumVMsStopped);
   SplitBrainBB.getBB().getSharedCounters().add(SplitBrainBB.NumVMsStopped, numVMsToStop);
   Log.getLogWriter().info("In SBKnownKeys.causeUnhealthiness, choosing " + numVMsToStop + " vm(s) to target...");
   List vmList = getDataStoreVms();
   List targetVms = new ArrayList();
   List unhealthinessList = new ArrayList();
   List playDeadList = new ArrayList();
   for (int i = 1; i <= numVMsToStop; i++) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size()-1);
      targetVms.add(vmList.get(randInt));
      vmList.remove(randInt);
      unhealthinessList.add(TestConfig.tab().stringAt(SplitBrainPrms.unhealthiness));
      playDeadList.add(new Boolean(TestConfig.tab().booleanAt(SplitBrainPrms.playDead)));
   }

   UnhealthyUtil.causeUnhealthiness(targetVms, controllerRegion);
   for (int i = 0; i < targetVms.size(); i++) {
      ClientVmInfo info = (ClientVmInfo)(targetVms.get(i));
      ControllerBB.reset(info.getVmid().intValue());
   }

   // wait for recovery to run
   PRObserver.waitForRebalRecov(targetVms, 1, 2, null, null, false);
}

// ======================================================================== 
// other methods

/** Log the execution number of this serial task.
 */
static protected void logExecutionNumber() {
   long exeNum = SplitBrainBB.getBB().getSharedCounters().incrementAndRead(SplitBrainBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}
    
/** Check if we have run for the desired length of time. We cannot use 
 *  hydra's taskTimeSec parameter because of a small window of opportunity 
 *  for the test to hang due to the test's "concurrent round robin" type 
 *  of strategy. Here we set a blackboard counter if time is up and this
 *  is the last concurrent round.
 */
protected void checkForLastIteration() {
   // determine if this is the last iteration
   long taskStartTime = 0;
   final String bbKey = "TaskStartTime";
   Object anObj = SplitBrainBB.getBB().getSharedMap().get(bbKey);
   int secondsToRun = TestConfig.tab().intAt(SplitBrainPrms.secondsToRun, 1800);
   if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      SplitBrainBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
   } else {
      taskStartTime = ((Long)anObj).longValue();
   }
   if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.TimeToStop);
   } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
         (secondsToRun - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
   }
}

}
