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

import parReg.*;
import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/** A version of parReg.ParRegTest that allows time to re-establish 
 *  redundancy after the network connections have been dropped before
 *  validation.  Surviving side VMs execute concurrent operations,
 *  and then validate the cache and PR metadeta.  In this version, 
 *  VMs will wait SplitBrainPrms.dropWaitTimeSec after a network drop
 *  before performing validation.
 */
public class PRNetDownTest extends parReg.ParRegTest {

protected static String CONCURRENT_LEADER_THREADID = "ConcurrentLeaderThreadId";

public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new PRNetDownTest();
      testInstance.initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
      testInstance.isDataStore = getIsDataStore();
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
}

public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new PRNetDownTest();
      testInstance.initializeRegion("clientRegion");
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         ParRegUtil.registerInterest(testInstance.aRegion);
      }
      testInstance.isDataStore = getIsDataStore();
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
}

/** Hydra task method for concurrent tests with verification.
 */
public static void HydraTask_doConcOpsAndVerify() {
   ((PRNetDownTest)testInstance).doConcOpsAndVerify();
}

/** Do random operations and verification for concurrent tests.
 *  The task starts up and all threads concurrently do random
 *  operations. The operations run for maxTaskGranularitySec or
 *  numOpsPerTask, depending the on the defined hydra parameters, 
 *  then all threads will pause. During the pause, one thread goes
 *  first and writes all known keys/values to the blackboard. Then
 *  all other threads read the blackboard and verify they have the
 *  same view. After all threads are done with verification, the
 *  task ends.
 *
 *  In this PRNetDownTest version, the ConcurrentLeader coordinates with 
 *  SBUtil.dropConnections via a hydra.blackboard.SharedLock.  Either the 
 *  ConcurrentLeader holds the lock (and verification is taking place) OR
 *  the dropConnections thread holds the lock and verification must wait
 *  for the lock to be released.
 */
protected void doConcOpsAndVerify() {
   // wait for all threads to be ready to do this task, then do random ops
   long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.ReadyToBegin);
   if (counter == 1) {
      logExecutionNumber();
   }
   registerInterestCoordinator = new MethodCoordinator(ParRegTest.class.getName(), "registerInterest");
   concVerifyCoordinator = new MethodCoordinator(ParRegTest.class.getName(), "concVerify");
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.ConcurrentLeader);
   numThreadsInClients = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
   Log.getLogWriter().info("numThreadsInClients = " + numThreadsInClients);
   TestHelper.waitForCounter(ParRegBB.getBB(), 
                             "ParRegBB.ReadyToBegin", 
                             ParRegBB.ReadyToBegin, 
                             numThreadsInClients,
                             true, 
                             -1,
                             1000);
   checkForLastIteration();

   Log.getLogWriter().info("Zeroing ShapshotWritten");
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.SnapshotWritten);

   // do random operations 
   try {
      doEntryOperations(aRegion);
   } catch (Exception e) {
      boolean rootCauseIsFD = false;
      if (e instanceof GemFireException) {
         GemFireException gfe = (GemFireException)e;
         Throwable rootCause = gfe.getRootCause();
         if (rootCause instanceof ForcedDisconnectException) {
            rootCauseIsFD = true;
         }
      } else {
         throw new TestException("doEntryOperations caught unexpected Exception " + TestHelper.getStackTrace(e));
      }
   }

   // wait for all threads to pause, then do the verify
   Log.getLogWriter().info("Zeroing FinishedVerify");
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.FinishedVerify);
   ParRegBB.getBB().getSharedCounters().increment(ParRegBB.Pausing);
   TestHelper.waitForCounter(ParRegBB.getBB(), 
                             "ParRegBB.Pausing", 
                             ParRegBB.Pausing, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   Log.getLogWriter().info("Zeroing ReadyToBegin");
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.ReadyToBegin);

   // in a bridge configuration, wait for the message queues to finish pushing
   // to the clients
   if (isBridgeConfiguration) {
      // wait for 30 seconds of client silence
      SilenceListener.waitForSilence(30, 5000);

      if (isBridgeClient && (bridgeOrderingWorkaround.equalsIgnoreCase("registerInterest"))) {
         // lynn - Workaround for bug 35662, ordering problem in bridge clients.
         // The workaround is to re-registerInterest doing a full GII; this causes the
         // clients to become aligned with the servers.
         registerInterestCoordinator.executeOnce(this, new Object[0]);
         if (!registerInterestCoordinator.methodWasExecuted()) {
            throw new TestException("Test problem: RegisterInterest did not execute");
         }
      }
   }

   try {
      // do verification: concurrentLeader will obtain the lock in concVerify
      concVerifyCoordinator.executeOnce(this, new Object[0]);
      if (!concVerifyCoordinator.methodWasExecuted()) {
         throw new TestException("Test problem: concVerify did not execute");
      }
      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.FinishedVerify);

      // wait for everybody to finish verify, then exit 
      TestHelper.waitForCounter(ParRegBB.getBB(), 
                                "ParRegBB.FinishedVerify", 
                                ParRegBB.FinishedVerify, 
                                numThreadsInClients,
                                true, 
                                -1,
                                5000);

   } finally {
      // Once everyone has finished validation, release the lock.
      // The drop includes time for redundancy to be re-established,
      // see SplitBrainPrms.dropWaitTimeSec
      Integer tid = (Integer)SplitBrainBB.getBB().getSharedMap().get(CONCURRENT_LEADER_THREADID);
      if (tid.intValue() == RemoteTestModule.getCurrentThread().getThreadId()) {
         SharedLock criticalCodeSection = SplitBrainBB.getBB().getSharedLock();
         criticalCodeSection.unlock();
         Log.getLogWriter().fine("PRNetDown released the SharedLock!");
      }
   }

   Log.getLogWriter().info("Zeroing Pausing");
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.Pausing);

   counter = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num executions is " + 
            ParRegBB.getBB().getSharedCounters().read(ParRegBB.ExecutionNumber));
}

/** Modified version of ParRegTest.concVerify().  The ConcurrentLeader
 *  coordinates with SBUtil.dropConnections via a blackboard.SharedLock
 *  to protect verification from network drops.  The lock is released
 *  in doConcOpsAndVerify (once all VMs had verified against the snapshot
 *  in the blackboard.
 */
protected void concVerify() {
   boolean leader = false;
   boolean isSurvivingSide = true;
   if (RemoteTestModule.getMyHost().equalsIgnoreCase(SplitBrainBB.getLosingSideHost())) {
      isSurvivingSide = false;
   }
   if (isSurvivingSide || isBridgeClient) { // losingSide VMs cannot write the snapshot (region could be closed after FD
      leader = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.ConcurrentLeader) == 1;
   }
   if (leader) {
      int tid = RemoteTestModule.getCurrentThread().getThreadId();
      SplitBrainBB.getBB().getSharedMap().put(CONCURRENT_LEADER_THREADID, new Integer(tid));
      Log.getLogWriter().info("In concVerify, this thread is the concurrent leader");
      // protect this critical section of code from dropped network connections
      // during verification via the blackboard.SharedLock(), see 
      // SBUtil.dropConnection()
      SharedLock criticalCodeSection = SplitBrainBB.getBB().getSharedLock();
      criticalCodeSection.lock();
      Log.getLogWriter().fine("PRNetDown obtained the SharedLock!");

      // this is the first thread to verify; all other threads will wait for this thread to
      // write its view of the partitioned region to the blackboard and they will read it and
      // match it
      regionSnapshot = new HashMap();
      destroyedKeys = new HashSet();
      Log.getLogWriter().info("This thread is the concurrentLeader, creating region snapshot..."); 
      Set keySet = aRegion.keySet();
      Iterator it = keySet.iterator();
      while (it.hasNext()) {
         Object key = it.next();
         Object value = null;
         if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
            value = aRegion.get(key);
         }
         if (value instanceof BaseValueHolder)
            regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
         else
            regionSnapshot.put(key, value);
      }
      Log.getLogWriter().info("Done creating region snapshot with " + regionSnapshot.size() + " entries; " + regionSnapshot);
      ParRegBB.getBB().getSharedMap().put(ParRegBB.RegionSnapshot, regionSnapshot);
      Log.getLogWriter().info("Done creating destroyed keys with " + destroyedKeys.size() + " keys");
      ParRegBB.getBB().getSharedMap().put(ParRegBB.DestroyedKeys, destroyedKeys);
      long snapshotWritten = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.SnapshotWritten);
      Log.getLogWriter().info("Incremented SnapshotWritten, now is " + snapshotWritten);
   } else { 
      Log.getLogWriter().info("In concVerify, this thread is waiting for the concurrent leader to write the snapshot");
      // this thread is not the first to verify; it will wait until the first thread has
      // written its state to the blackboard, then it will read it and verify that its state matches
      TestHelper.waitForCounter(ParRegBB.getBB(), 
                                "ParRegBB.SnapshotWritten", 
                                ParRegBB.SnapshotWritten, 
                                1, 
                                true, 
                                -1,
                                2000);
      try {
         verifyFromSnapshot();
      } catch (Exception e) {
         boolean rootCauseIsFD = false;
         if (e instanceof GemFireException) {
            GemFireException gfe = (GemFireException)e;
            Throwable rootCause = gfe.getRootCause();
            if (rootCause instanceof ForcedDisconnectException) {
               rootCauseIsFD = true;
            }
         } else {
            throw new TestException("verifyFromSnapshot threw unexpected Exception " + TestHelper.getStackTrace(e));
         }
      }
   }
}

}
