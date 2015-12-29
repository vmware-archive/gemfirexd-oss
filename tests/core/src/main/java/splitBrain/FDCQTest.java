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

import java.util.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.distributed.*;
import java.lang.reflect.*;
import java.io.File;
import cq.*;

public class FDCQTest extends CQTest {
    
protected Region fdRegion = null; // region to help cause forced disconnects

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of CQTest in a client.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new FDCQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("clientRegion");
      ((FDCQTest)testInstance).fdRegion = testInstance.initializeRegion("fdRegionClient");
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         testInstance.registerInterest(testInstance.aRegion);
         if (CQsOn) {
            testInstance.initializeQueryService();
            testInstance.queryMap = testInstance.generateQueries(testInstance.queryDepth);
            testInstance.initializeCQs();
         } else {
            Log.getLogWriter().info("Not creating CQs because CQUtilPrms.CQsOn is " + CQsOn);
         }
      }
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new FDCQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverRegion");
      ((FDCQTest)testInstance).fdRegion = testInstance.initializeRegion("fdRegionServer");
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
      Log.getLogWriter().info("Installing MembershipNotifierHook");
      SBUtil.addMembershipHook(new MembershipNotifierHook());
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

// ========================================================================
// test methods

/** While other threads are doing ops, stop or close the region of servers 
 *  (not clients), reinstate the servers/regions, pause and verify. This 
 *  method coordinates the use of blackboard counters with HADoEntryOps 
 *  for pausing.
 */
protected void HAController() {
   logExecutionNumber();
   concVerifyCoordinator = new MethodCoordinator(CQTest.class.getName(), "concVerify");
   checkForLastIteration();
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ExceptionCounter);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ConcurrentLeader);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Reinitialized);

   // cause a forced disconnect in a server
   int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
   Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "bridge");
   List vmList = (List)(tmpArr[0]);

   // enable the forced disconnects
   disconnected = true; // so entry ops will know it's ok to get an exception
   for (int i = 0; i < vmList.size(); i++) {
      int vmid = ((Integer)((ClientVmInfo)(vmList.get(i))).getVmid()).intValue();
      ControllerBB.enableSlowListener(vmid); // returns immediately
      ControllerBB.enablePlayDead(vmid); // returns immediately
   }

   // cause a forced disconnect; this returns after the forced disconnect occurs
   Log.getLogWriter().info("Calling put to cause a forced disconnect on " + vmList);
   fdRegion.put(new Long(System.currentTimeMillis()), "");
   Log.getLogWriter().info("Done calling put to cause a forced disconnect on " + vmList);
   ControllerBB.checkForError();

   // signal its ok to run reinitialization
   for (int i = 0; i < vmList.size(); i++) {
      ControllerBB.checkForError();
      int vmid = ((Integer)((ClientVmInfo)(vmList.get(i))).getVmid()).intValue();
      ControllerBB.signalReadyForInit(vmid);
   }

   // wait for reinitialization
   for (int i = 0; i < vmList.size(); i++) {
      ControllerBB.checkForError();
      int vmid = ((Integer)((ClientVmInfo)(vmList.get(i))).getVmid()).intValue();
      ControllerBB.waitForInitialization(vmid);
   }

   // check for errors
   ControllerBB.checkForError();
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.SnapshotWritten);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.FinishedVerify);

   // now get all vms to pause for verification
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.Pausing);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.Pausing", 
                             CQUtilBB.Pausing, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.SyncUp);

   if (isBridgeConfiguration) { 
      // wait for 30 seconds of client silence to allow everything to be pushed to clients
      SilenceListener.waitForSilence(30, 5000);
   }
   concVerifyCoordinator.executeOnce(this, new Object[0]);
   if (!concVerifyCoordinator.methodWasExecuted()) {
      throw new TestException("Test problem: concVerify did not execute");
   }

   // wait for everybody to finish verify
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.FinishedVerify);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.FinishedVerify", 
                             CQUtilBB.FinishedVerify, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Pausing);
   // reset the ForcedDiscListener for each targetVM
   for (int i = 0; i < vmList.size(); i++) {
      int vmid = ((Integer)((ClientVmInfo)(vmList.get(i))).getVmid()).intValue();
      ControllerBB.reset(vmid);
   }
   disconnected = false;


   // sync up with the HADoEntryOps threads; this will avoid a new
   // HAController task getting scheduled before all the HADoEntryOPs
   // threads have zeroed any counters after the verify
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.SyncUp);
   TestHelper.waitForCounter(CQUtilBB.getBB(),
                             "CQUtilBB.SyncUp",
                             CQUtilBB.SyncUp,
                             numThreadsInClients,
                             true,
                             -1,
                             1000);

   // see if it's time to stop the test
   long counter = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num HAController executions is " + 
            CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.ExecutionNumber));
}

/** Handle an exception thrown by doing general operations during HA.
 */
protected void handleException(Exception anExcept) {
   if (anExcept instanceof com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException) {
      if (ControllerBB.isSlowListenerEnabled()) {
         // this VM is expecting a forced disconnect; ok
      } else {
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } 
   Log.getLogWriter().info("Caught " + anExcept + "; expected, continuing test");
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.ExceptionCounter);
}

}
