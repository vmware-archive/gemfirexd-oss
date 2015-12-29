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
package recovDelay; 

import getInitialImage.*;
import parReg.*;
import hydra.*;
import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;

public class EventsDuringRecoveryTest extends KnownKeysTest {

protected static boolean thisVMRestarted = false;

// ======================================================================== 
// initialization tasks/methods 

public synchronized static void HydraTask_HA_accessorInitialize() {
  if (testInstance == null) {
     testInstance = new EventsDuringRecoveryTest();
     ((KnownKeysTest)testInstance).initInstance("accessorRegion");
     if (isBridgeConfiguration) {
        ParRegUtil.registerInterest(testInstance.aRegion);
     }
  }
}

public synchronized static void HydraTask_HA_dataStoreInitialize() {
  if (testInstance == null) {
     testInstance = new EventsDuringRecoveryTest();
     ((KnownKeysTest)testInstance).initInstance("dataStoreRegion");
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                         new Integer(RemoteTestModule.getMyVmid()));
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
     RegionAttributes attr = testInstance.aRegion.getAttributes();
     PartitionAttributes prAttr = attr.getPartitionAttributes();
     ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(prAttr.getRecoveryDelay()));
     ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay", new Long(prAttr.getStartupRecoveryDelay()));
  }
}

public synchronized static void HydraTask_HA_dataStoreReinitialize() {
  if (testInstance == null) {
     Log.getLogWriter().info("Reinitializing with EventPRObserver");
     EventPRObserver.installObserverHook();
     EventPRObserver.initialize(RemoteTestModule.getMyVmid());
     testInstance = new EventsDuringRecoveryTest();
     ((KnownKeysTest)testInstance).initInstance("dataStoreRegion");
     thisVMRestarted = true;
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                         new Integer(RemoteTestModule.getMyVmid()));
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
     RegionAttributes attr = testInstance.aRegion.getAttributes();
     PartitionAttributes prAttr = attr.getPartitionAttributes();
     ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(prAttr.getRecoveryDelay()));
     ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay", new Long(prAttr.getStartupRecoveryDelay()));
  }
}

/** Hydra task to stop/start numVmsToStop at a time.
 */
public static void HydraTask_stopStartVms() {
   PRObserver.initialize();
   int numVMsToStop = TestConfig.tab().intAt(ParRegPrms.numVMsToStop);
   Log.getLogWriter().info("In stopStartVms, choosing " + numVMsToStop + " vm(s) to stop...");
   Object[] anArr = StopStartVMs.getOtherVMs(numVMsToStop, "dataStore");
   List targetVms = (List)(anArr[0]);
   List stopModes = (List)(anArr[1]);
   List vmList = (List)(anArr[2]);
   StopStartVMs.stopStartVMs(targetVms, stopModes);
   RecovDelayBB.getBB().getSharedMap().put("stopped vms", targetVms);

   // wait for recovery to run
   long recoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("recoveryDelay"))).longValue();
   long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
   PRObserver.waitForRecovery(recoveryDelay, startupRecoveryDelay, 
              targetVms, vmList, numVMsToStop, 1, null, null);
      

   long timeToStop = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
   if (timeToStop > 0) { // ops have ended
      throw new TestException("Ops did not run long enough to span a stop/start; this is a test tuning issue");
   }
}

/** Hydra task to execution ops, then stop scheduling.
 *  In this test, we want to do ops in the data store vms after the
 *  test has stopped and restarted some of those, which occurs only once.
 *  This is because the test wants to verify all ops have occurred while 
 *  recovery is in progress, so once the restarted vms have completed their 
 *  init tasks, async recovery keeps running and then the data store vms 
 *  can do ops. 
 */
public static void HydraTask_doOps() {
   if (!thisVMRestarted) { 
      if (RemoteTestModule.getMyClientName().indexOf("dataStore") >= 0) {
         // either this datastore did not get chosen to be stopped, or we haven't stopped yet
         List stoppedVMs = (List)(RecovDelayBB.getBB().getSharedMap().get("stopped vms"));
         if (stoppedVMs == null) { // vms have NOT been stopped/restarted yet
            Log.getLogWriter().info("Not allowing this data store vm to run ops; vms have not yet stopped/restarted");
            MasterController.sleepForMs(2000);
            return;
         }
      }
   }

   BitSet availableOps = new BitSet(operations.length);
   availableOps.flip(FIRST_OP, LAST_OP+1);
   testInstance.doOps(availableOps);
   if (availableOps.cardinality() == 0) {
      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.TimeToStop);
      throw new StopSchedulingTaskOnClientOrder("Finished with ops");
   }
}

//=========================================================================
// Verification method for listener counters

/** Verify event counters occurred during recovery.
 */
public static void HydraTask_checkForErrors() {
   // only check for errors in those vms that were stopped and restarted
   // as these are the vms that ran recovery
   if (!thisVMRestarted) {
      Log.getLogWriter().info("Not checking counters in this vm because it was not stopped and restarted");
      return;
   }

   StringBuffer aStr = new StringBuffer();
   CountListener.logCounters();
   int threshold = 5;

   // most events will be isRemote because the data is likely stored in the PR elsewhere, rather
   // than this vm; to avoid the test failing because fewer entries are stored in the current vm 
   // vs. all other datastores, summ the non-remote counts and use this 
   // When the individual not-remote counts are below the threshold, it's likely because the test
   // randomly just didn't get non-remote events due to primary data being physically stored in other
   // vms.
   int summOfIsNotRemoteCounters = CountListener.afterCreateCount_isNotRemote + 
      CountListener.afterCreatePutAllCount_isNotRemote +
      CountListener.afterDestroyCount_isNotRemote + 
      CountListener.afterUpdateCount_isNotRemote + 
      CountListener.afterDestroyCount_isNotRemote;
   int summThreshold = 10;

   // create
   if (CountListener.afterCreateCount_isRemote < threshold) {
      aStr.append("This vm might have blocked during operations; it received " + 
                  CountListener.afterCreateCount_isRemote + " remote create events during recovery\n");
   }
   if (CountListener.afterCreateCount_isNotRemote < threshold) {
      if (summOfIsNotRemoteCounters < summThreshold) {
         aStr.append("This vm might have blocked during operations; it received " + 
                     CountListener.afterCreateCount_isNotRemote + " create events during recovery\n");
      }
   }
   if (CountListener.afterCreatePutAllCount_isRemote < threshold) {
      aStr.append("This vm might have blocked during operations; it received " + 
                  CountListener.afterCreatePutAllCount_isRemote + " remote putAll create events during recovery\n");
   }
   if (CountListener.afterCreatePutAllCount_isNotRemote < threshold) {
      if (summOfIsNotRemoteCounters < summThreshold) {
         aStr.append("This vm might have blocked during operations; it received " + 
                     CountListener.afterCreatePutAllCount_isNotRemote + " putAll create events during recovery\n");
      }
   }

   // destroy
   if (CountListener.afterDestroyCount_isRemote < threshold) {
      aStr.append("This vm might have blocked during operations; it received " + 
                  CountListener.afterDestroyCount_isRemote + " remote destroy events during recovery\n");
   }
   if (CountListener.afterDestroyCount_isNotRemote < threshold) {
      if (summOfIsNotRemoteCounters < summThreshold) {
         aStr.append("This vm might have blocked during operations; it received " + 
                     CountListener.afterDestroyCount_isNotRemote + " destroy events during recovery\n");
      }
   }

   // update
   if (CountListener.afterUpdateCount_isRemote < threshold) {
      aStr.append("This vm might have blocked during operations; it received " + 
                  CountListener.afterUpdateCount_isRemote + " remote update events during recovery\n");
   }
   if (CountListener.afterUpdateCount_isNotRemote < threshold) {
      if (summOfIsNotRemoteCounters < summThreshold) {
         aStr.append("This vm might have blocked during operations; it received " + 
                     CountListener.afterUpdateCount_isNotRemote + " update events during recovery\n");
      }
   }

   // invalidate
   if (CountListener.afterInvalidateCount_isRemote < threshold) {
      aStr.append("This vm might have blocked during operations; it received " + 
                  CountListener.afterInvalidateCount_isRemote + " remote update events during recovery\n");
   }
   if (CountListener.afterInvalidateCount_isNotRemote < threshold) {
      if (summOfIsNotRemoteCounters < summThreshold) {
         aStr.append("This vm might have blocked during operations; it received " + 
                     CountListener.afterInvalidateCount_isNotRemote + " invalidate events during recovery\n");
      }
   }

   if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
   }
}

}
