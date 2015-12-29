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

import memscale.MemScaleBB;

import util.*;
import hydra.*;
import parReg.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/** Test class for serial and concurrent partitioned region tests with
 *  verification and forced disconnects for HA.
 */
public class SBParRegTest extends parReg.ParRegTest {
    
protected static String IsDataStoreVmStr = "IsDataStoreVM_";
protected Region controllerRegion;           // region used to cause forced disconnects
protected List vmList = new ArrayList();

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of ParRegTest in this VM
 *  for HA testing with a PR accessor.
 */
public synchronized static void HydraTask_HA_initializeAccessor() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new SBParRegTest();
      testInstance.initializeRegion("accessorRegion");
      testInstance.initializeInstance();
      testInstance.isDataStore = false;
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         ParRegUtil.registerInterest(testInstance.aRegion);
      } else { 
         Log.getLogWriter().info("Installing MembershipNotifierHook");
         SBUtil.addMembershipHook(new MembershipNotifierHook()); 
      }
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("accessorControllerRegion");
      SplitBrainBB.getBB().getSharedMap().put(IsDataStoreVmStr + RemoteTestModule.getMyVmid(),
                                              new Boolean(false)); // not a datastore
      ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.NumVMsInDS);
      ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM
 *  for HA testing with a PR data store.
 */
public synchronized static void HydraTask_HA_initializeDataStore() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new SBParRegTest();
      testInstance.isDataStore = true;
      testInstance.initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      RegionAttributes attr = testInstance.aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      long recoveryDelay = prAttr.getRecoveryDelay();
      long startupRecoveryDelay = prAttr.getStartupRecoveryDelay();
      ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(recoveryDelay));
      ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay", new Long(startupRecoveryDelay));
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = false;
         BridgeHelper.startBridgeServer("bridge");
      }
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerRegion");
      SplitBrainBB.getBB().getSharedMap().put(IsDataStoreVmStr + RemoteTestModule.getMyVmid(),
                                              new Boolean(true)); // a datastore
      ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
      Log.getLogWriter().info("Installing MembershipNotifierHook");
      SBUtil.addMembershipHook(new MembershipNotifierHook()); 
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.NumVMsInDS);
      ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Recreate an accessor region after a forced disconnect.
 */
public static void initAccessorAfterForcedDisconnect() {
   testInstance.HA_reinitializeRegion();
   ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("accessorControllerRegion");
   testInstance.isDataStore = false;
   if (testInstance.isBridgeConfiguration) {
      testInstance.isBridgeClient = true;
      ParRegUtil.registerInterest(testInstance.aRegion);
   }
}
    
/** Recreate a datastore region after a forced disconnect.
 */
public static void initDataStoreAfterForcedDisconnect() {
   PRObserver.initialize(RemoteTestModule.getMyVmid());
   testInstance.HA_reinitializeRegion();
   ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerRegion");
   testInstance.isDataStore = true;
   if (testInstance.isBridgeConfiguration) {
      testInstance.isBridgeClient = false;
      BridgeHelper.startBridgeServer("bridge");
   }
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM
 *  for HA testing.
 */
public synchronized static void HydraTask_HA_reinitializeAccessor() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new SBParRegTest();
      testInstance.HA_reinitializeRegion();
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("accessorControllerRegion");
      testInstance.initializeInstance();
      testInstance.isDataStore = false;
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         ParRegUtil.registerInterest(testInstance.aRegion);
      }
   }
   ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Creates and initializes the singleton instance of ParRegTest in this VM
 *  for HA testing.
 */
public synchronized static void HydraTask_HA_reinitializeDataStore() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      testInstance = new SBParRegTest();
      testInstance.HA_reinitializeRegion();
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerRegion");
      testInstance.initializeInstance();
      testInstance.isDataStore = true;
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = false;
         BridgeHelper.startBridgeServer("bridge");
      }
   }
   ControllerBB.getBB().getSharedMap().put("" + ProcessMgr.getProcessId(), new Integer(RemoteTestModule.getMyVmid()));
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** If this vm has a startup delay, wait for it.
 */
public static void HydraTask_waitForStartupRecovery() {
   List startupVMs = new ArrayList(StopStartBB.getBB().getSharedMap().getMap().values());
   List vmsExpectingRecovery = StopStartVMs.getMatchVMs(startupVMs, "dataStore");
   vmsExpectingRecovery.addAll(StopStartVMs.getMatchVMs(startupVMs, "bridge"));
   if (vmsExpectingRecovery.size() == 0) {
      throw new TestException("No startup vms to wait for");
   }
   long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
   if (startupRecoveryDelay >= 0) {
      List prNames = new ArrayList();
      prNames.add("/partitionedRegion");
      prNames.add("/controllerReg");
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, 2, prNames, null, false);
   }
}

// ========================================================================
// override methods to do a forced disconnect

/** Stop vms with stop/start or forced disconnects
 */
protected void HAController() {
   logExecutionNumber();
   offHeapVerifyTargetCount = getOffHeapVerifyTargetCount();
   registerInterestCoordinator = new MethodCoordinator(ParRegTest.class.getName(), "registerInterest");
   checkForLastIteration();
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.ExceptionCounter);
   ControllerBB.getBB().getSharedCounters().zero(ControllerBB.ExceptionCounter);
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.ConcurrentLeader);
   cycleVms();
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.Reinitialized);
   numThreadsInClients = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
   Log.getLogWriter().info("numThreadsInClients = " + numThreadsInClients);
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.SnapshotWritten);
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.FinishedVerify);
   MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.finishedMemCheck);

   // now get all vms to pause for verification
   ParRegBB.getBB().getSharedCounters().increment(ParRegBB.Pausing);
   TestHelper.waitForCounter(ParRegBB.getBB(), 
                             "ParRegBB.Pausing", 
                             ParRegBB.Pausing, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.rebalanceCompleted);
   reset();
   if (isBridgeConfiguration) { 
      // wait for 30 seconds of client silence to allow everything to be pushed to clients
      SilenceListener.waitForSilence(30, 5000);
   }
   if (isBridgeClient && (bridgeOrderingWorkaround.equalsIgnoreCase("registerInterest"))) {
      // lynn - Workaround for bug 35662, ordering problem in bridge clients.
      // The workaround is to re-registerInterest doing a full GII; this causes the
      // clients to become aligned with the servers.
      registerInterestCoordinator.executeOnce(this, new Object[0]);
      if (!registerInterestCoordinator.methodWasExecuted()) {
         throw new TestException("Test problem: RegisterInterest did not execute");
      }
   } 
   concVerify();

   // wait for everybody to finish verify
   ParRegBB.getBB().getSharedCounters().increment(ParRegBB.FinishedVerify);
   TestHelper.waitForCounter(ParRegBB.getBB(), 
                             "ParRegBB.FinishedVerify", 
                             ParRegBB.FinishedVerify, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.Pausing);
   
   // rebalance and verify again
   ParRegUtil.doRebalance();
   verifyFromSnapshot();
   ParRegBB.getBB().getSharedCounters().increment(ParRegBB.rebalanceCompleted);

   // see if it's time to stop the test
   long counter = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num HAController executions is " + 
            ParRegBB.getBB().getSharedCounters().read(ParRegBB.ExecutionNumber));
}

/** Reset the map counters
 *
 */
protected void reset() {
   for (int i = 0; i < vmList.size(); i++) {
      ClientVmInfo info = (ClientVmInfo)(vmList.get(i));
      int vmid = info.getVmid().intValue();
      ControllerBB.reset(vmid);
   }
}

/** override 
 */
public void doRROpsAndVerify() {
   super.doRROpsAndVerify();
   reset();
}

/** Choose numVMsToStop vms, then cause them to get a forced disconnect.
 *  This causes a failure for partitioned regions to recover from.
 *
 */
protected void cycleVmsNoWait() {
   Log.getLogWriter().info("Cycling vms by causing alerts followed by stop start or forced disconnects...");
   PRObserver.initialize();
   Object[] tmp = StopStartVMs.getOtherVMsWithExclude(numVMsToStop, "admin");
   vmList = (List)(tmp[0]);
   cycleVMs_notChosenVMs = (List)(tmp[2]);

   SplitBrainBB.getBB().getSharedCounters().zero(SplitBrainBB.NumVMsStopped);
   SplitBrainBB.getBB().getSharedCounters().add(SplitBrainBB.NumVMsStopped, numVMsToStop);
   UnhealthyUtil.causeUnhealthiness(vmList, controllerRegion);
   ControllerBB.checkForError();
   ControllerBB.checkEventCounters(ControllerBB.NumForcedDiscEventsKey, vmList, 1);
   Log.getLogWriter().info("Done cycling vms");
}

protected void waitForRecoveryAfterCycleVMs() {
   // Wait for recovery to complete
   List dataStoreNotChosen = StopStartVMs.getMatchVMs(cycleVMs_notChosenVMs, "dataStore");
   dataStoreNotChosen.addAll(StopStartVMs.getMatchVMs(cycleVMs_notChosenVMs, "bridge"));
   List dataStoreTargetVMs = StopStartVMs.getMatchVMs(vmList, "dataStore");
   dataStoreTargetVMs.addAll(StopStartVMs.getMatchVMs(vmList, "bridge"));
   long recoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("recoveryDelay"))).longValue();
   long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
   List prNames = new ArrayList();
   prNames.add("partitionedRegion");
   prNames.add("controllerReg");

   PRObserver.waitForRecovery(recoveryDelay, startupRecoveryDelay, dataStoreTargetVMs,
              dataStoreNotChosen, numVMsToStop, 2, prNames, null);

   // clear exception counter (vms that received forced disconnect would have bumped it)
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.ExceptionCounter);
   for (int i = 0; i < vmList.size(); i++) {
      ClientVmInfo info = (ClientVmInfo)(vmList.get(i));
      int vmid = info.getVmid().intValue();
      ControllerBB.waitForInitialization(vmid);
   }
   Log.getLogWriter().info("Done waiting for recovery");
}

/** Handle an exception thrown by doing general operations during HA.
 *  Overriding this allows threads to accept a forced disconnect.
 */
protected void handleException(Exception anExcept) {
   boolean thisVMReceivedForcedDisc = ControllerBB.isPlayDeadEnabled();
   String errStr = anExcept.toString();
   boolean disconnectError = 
           (errStr.indexOf(
              "This connection to a distributed system has been disconnected") >= 0) ||
           (errStr.indexOf(
              "System is disconnecting") >= 0);

   if (anExcept instanceof CancelException) {
      if ( thisVMReceivedForcedDisc || cacheIsClosed || disconnected) {
         // a thread in this VM closed the cache or disconnected from the dist system
         // or we expecte a forced disconnect in this vm; all is OK
      } else { // no reason to get this error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof LockServiceDestroyedException) {
      if ((lockOperations && thisVMReceivedForcedDisc) ||
          (lockOperations && disconnected) ||
          (lockOperations && cacheIsClosed)) {
         // we can expect this error if we are doing locking and we got
         // a forcedDisconnect or we have disconnected or we have closed the cache
      } else {
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof IllegalStateException) {
      if (disconnectError) {
         if (disconnected || thisVMReceivedForcedDisc) {  
            // we got a disconnect 
         } else { // no reason to get this error
            throw new TestException(TestHelper.getStackTrace(anExcept));
         }
      } else { // got IllegalStateException, but it's not a disconnect error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof CacheLoaderException) {
      if (isBridgeConfiguration && 
              (thisVMReceivedForcedDisc || regionLocallyDestroyed || disconnected || cacheIsClosed)) {
      } else { // got exception, but it's not a bridge loader being shutdown
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof BridgeWriterException) {
      if (isBridgeConfiguration && 
              (thisVMReceivedForcedDisc || regionLocallyDestroyed || disconnected || cacheIsClosed)) {
      } else { // got exception, but it's not a bridge writer being shutdown
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof RegionDestroyedException) {
      if (regionLocallyDestroyed) {
         // we got a RegionDestroyedException and we did destroy the region; OK
      } else { // got RegionDestroyedException, but we didn't destroy it
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else {
      throw new TestException("Got unexpected exception " + TestHelper.getStackTrace(anExcept));
   }
   Log.getLogWriter().info("Caught " + anExcept + "; expected, continuing test");
   ParRegBB.getBB().getSharedCounters().increment(ParRegBB.ExceptionCounter);
   long counter = ControllerBB.getBB().getSharedCounters().incrementAndRead(ControllerBB.ExceptionCounter);
   Log.getLogWriter().info("ControllerBB.ExceptionCounter is now " + counter); 
   ControllerBB.waitForInitialization(); // wait for reinitialization in this vm
}

public String HA_reinitializeRegion() {
   PRObserver.installObserverHook();
   PRObserver.initialize(RemoteTestModule.getMyVmid());
   String regionConfigName = super.HA_reinitializeRegion();
   boolean isDataStore = ((Boolean)(SplitBrainBB.getBB().getSharedMap().get(IsDataStoreVmStr + RemoteTestModule.getMyVmid()))).booleanValue();
   if (isDataStore) {
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("dataStoreControllerRegion");
   } else {
      ((SBParRegTest)testInstance).controllerRegion = RegionHelper.createRegion("accessorControllerRegion");
   }
   Log.getLogWriter().info("Installing MembershipNotifierHook");
   SBUtil.addMembershipHook(new MembershipNotifierHook()); 
   return regionConfigName;
}

/** Lose the partitioned region by doing each of the following in no
 *  particular order:
 *     1) close the cache
 *     2) disconnect from the distributed system
 *     3) locally destroy the PR
          (this is skipped if a bridge server; see bug 36812)
 *     4) close the PR
          (this is skipped if a bridge server; see bug 36812)
 *  Then recreate the PR after each, verifying the PR state.
 */
protected void loseAndRecreatePR() {
   Object[] choices = new Object[] {new Integer(CLOSE_CACHE), new Integer(DISCONNECT), 
                                    new Integer(LOCAL_DESTROY), new Integer(CLOSE_REGION)};
   if (isBridgeConfiguration && !isBridgeClient) { // this is a bridge server
      // don't do a local destroy or close region; see bug 36812
      choices = new Object[] {new Integer(CLOSE_CACHE), new Integer(DISCONNECT)};
   }
   List aList = Arrays.asList(choices);
   List otherVMs = new ArrayList(ClientVmMgr.getOtherClientVmids());

   // mix up the choices so this runs in a different order each time
   for (int i = 0; i < aList.size(); i++) {
      Collections.swap(aList, i, TestConfig.tab().getRandGen().nextInt(0, aList.size()-1));
   }

   // iterate through all the choices
   int numPRsToRecover = 0;
   for (int i = 0; i < aList.size(); i++) {
      numPRsToRecover = 2;
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      int choice = ((Integer)(aList.get(i))).intValue();
      // close the cache
      if (choice == CLOSE_CACHE) {
         Log.getLogWriter().info("Losing PR by closing the cache...");
         CacheHelper.closeCache();
      } else if (choice == DISCONNECT) {
         Log.getLogWriter().info("Losing PR by disconnecting from the distributed system...");
         DistributedSystemHelper.disconnect();
      } else if (choice == LOCAL_DESTROY) { // 
         Log.getLogWriter().info("Losing PR by locally destroying the partitioned region...");
         aRegion.localDestroyRegion();
         numPRsToRecover = 1;
      } else if (choice == CLOSE_REGION) {
         Log.getLogWriter().info("Losing PR by closing the partitioned region...");
         aRegion.close();
         numPRsToRecover = 1;
      }
      Log.getLogWriter().info("Recreating the partitioned region...");
      HA_reinitializeRegion();
      Log.getLogWriter().info("Done recreating the partitioned region...");
      if (isBridgeConfiguration) {
         if (testInstance.isBridgeClient) {
            ParRegUtil.registerInterest(testInstance.aRegion);
         } else {
            BridgeHelper.startBridgeServer("bridge");
         }
      }
      if (isDataStore) {
         RegionAttributes attr = aRegion.getAttributes();
         PartitionAttributes prAttr = attr.getPartitionAttributes();
         PRObserver.waitForRecovery(prAttr.getRecoveryDelay(), prAttr.getStartupRecoveryDelay(), 
                 new Integer(RemoteTestModule.getMyVmid()), otherVMs, 1, numPRsToRecover, null, null);
      }
      verifyFromSnapshot();
   }
}

/** Return the number of threads across the entire test that call off-heap memory validation.
 *  This number is the target counter value to wait for to proceed after off-heap memory validation.
 * @return The number of threads across the entire test that call off-heap memory validation.
 */
public int getOffHeapVerifyTargetCount() {
  return TestHelper.getNumThreads() -1; // -1 for the admin thread
}

public void initializeInstance() {
   super.initializeInstance();
   testInstance.offHeapVerifyTargetCount = getOffHeapVerifyTargetCount();
}

}
