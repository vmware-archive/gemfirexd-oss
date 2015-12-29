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

import parReg.*;
import hydra.*;
import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;

public class KillDuringRecoveryTest extends KnownKeysTest {

// ======================================================================== 
// initialization tasks/methods 

public synchronized static void HydraTask_HA_accessorInitialize() {
  if (testInstance == null) {
     testInstance = new KillDuringRecoveryTest();
     ((KnownKeysTest)testInstance).initInstance("accessorRegion");
     if (isBridgeConfiguration) {
        ParRegUtil.registerInterest(testInstance.aRegion);
     }
  }
}

public synchronized static void HydraTask_HA_dataStoreInitialize() {
  if (testInstance == null) {
     PRObserver.installObserverHook();
     PRObserver.initialize(RemoteTestModule.getMyVmid());
     testInstance = new KillDuringRecoveryTest();
     ((KnownKeysTest)testInstance).initInstance("dataStoreRegion");
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                         new Integer(RemoteTestModule.getMyVmid()));
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
  }
}

/** Stop and start ParRegPrms.numVMsToStop. Do not return until the VMs
 *  have restarted and reinitialized
 */
public void stopStartVms() {
   long exeNum = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning stopStartVms with execution number " + exeNum);
   PRObserver.initialize();
   int numVMsToStop = TestConfig.tab().intAt(ParRegPrms.numVMsToStop);
   RegionAttributes attr = aRegion.getAttributes();
   PartitionAttributes prAttr = attr.getPartitionAttributes();
   long recoveryDelay = prAttr.getRecoveryDelay();
   long startupRecoveryDelay = prAttr.getStartupRecoveryDelay();
   Log.getLogWriter().info("In RecovDelayTest.stopStartVms(), stopping and restarting " + numVMsToStop + " vm(s)");
   if (recoveryDelay >= 0) { // recover when vm is stopped
      if (startupRecoveryDelay >= 0) {  // recovery for both stopping and starting
         throw new TestException("Test configuration problem: Recovery is set to " +
             "run for both stopping and starting, recoveryDelay " + recoveryDelay + 
             ", startupRecoveryDelay " + startupRecoveryDelay + ", this is not supported by this test");
      }

      // We want to stop one vm, then kill some others when they are running recovery so we
      // have to specify more than 1 vm to kill
      if (numVMsToStop <= 1) {
         throw new TestException("Test configuration problem: ParRegPrms.numVMsToStop is 1 " +
               "so cannot stop any more vms while recovery is running; numVMsToStop must be > 1");
      }

      // choose all the vms to stop
      Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "dataStore");
      List targetVMs = (List)(tmpArr[0]);
      List stopModes = (List)(tmpArr[1]);
      List survivingVMs = (List)(tmpArr[2]);

      // choose one VM to stop first to trigger recovery in the others
      ClientVmInfo leaderVM = (ClientVmInfo)(targetVMs.get(0));
      targetVMs.remove(0);
      int leaderStopMode = ClientVmMgr.toStopMode((String)(stopModes.get(0)));
      stopModes.remove(0);

      // create threads to wait for recovery to start in the other targeted vm(s), then stop them
      Thread[] threadArr = new Thread[targetVMs.size()];
      for (int i = 0; i < targetVMs.size(); i++) {
         final ClientVmInfo info = (ClientVmInfo)(targetVMs.get(i));
         final int stopMode = ClientVmMgr.toStopMode((String)(stopModes.get(i)));
         final int vmID = info.getVmid();
         Thread workThread = new Thread(new Runnable() {
            public void run() {
               try {
                 Log.getLogWriter().info("Starting monitor thread to wait for departed recovery in vmID " + vmID); 
                  PRObserver.waitForRebalRecovToStart(vmID, Long.MAX_VALUE, 50);
                  ClientVmMgr.stop("Test is stopping " + info + " during recovery", 
                                   stopMode, ClientVmMgr.ON_DEMAND, info);
                 Log.getLogWriter().info("Terminating monitor thread to wait for departed recovery in vmID " + vmID); 
               } catch (ClientVmNotFoundException e) {
                  throw new TestException(TestHelper.getStackTrace(e));
               }
            }
         });
         workThread = new HydraSubthread(workThread);
         workThread.setName("Monitor thread for kill during departed recovery");
         threadArr[i] = workThread;
         threadArr[i].start();
      }

      // stop the first vm
      Log.getLogWriter().info("Test is stopping 1 vm out of " + numVMsToStop + " to trigger " +
         "recovery in remaining vms so " + (numVMsToStop-1) + " additional vm(s) can be stopped " +
         "during recovery; vm to first be stopped is " + leaderVM + ", vms to follow: " +
         targetVMs);
      try {
         ClientVmMgr.stop("Test is stopping first vmID " + leaderVM, 
                          leaderStopMode, ClientVmMgr.ON_DEMAND, leaderVM);
      } catch (ClientVmNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }

      // wait for threads to complete
      for (int i = 0; i < threadArr.length; i++) {
         try {
            threadArr[i].join();
         } catch (InterruptedException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      } 

      // wait for all remaining vms to recover
      PRObserver.waitForRebalRecov(survivingVMs, numVMsToStop, 1, null, null, true);

      // start the stopped vms
      targetVMs.add(leaderVM);
      StopStartVMs.startVMs(targetVMs);
   } else { // no recovery on a stopped vm
      if (startupRecoveryDelay >= 0) { // recovery on a started vm

         // choose all the vms to stop
         Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "dataStore");
         List targetVMs = (List)(tmpArr[0]);
         List stopModes = (List)(tmpArr[1]);
         List survivingVMs = (List)(tmpArr[2]);

         // Stop them
         StopStartVMs.stopVMs(targetVMs, stopModes);

         // verify no recovery
         PRObserver.verifyNoRebalRecov(targetVMs);
         PRObserver.verifyNoRebalRecov(survivingVMs);

         // Start them so they can be killed during startup recovery.
         // Killing during startup is tricky; we can't let the threads
         // (to be spawned below) kill the starting vms when we detect that
         // recovery has begun in those vms because hydra doesn't allow us to kill
         // a vm unless it has completed its init tasks. Start the vms now...this
         // start call doesn't return until the vms have completed their init task.
         // Then we can kill them. With async recovery, the init tasks will complete
         // but recovery can still be running. The test has to be tuned with a 
         // sufficient data size such that recovery takes a while, plus it can be 
         // tuned with a startup delay to help increase the chances that when the 
         // below threads do start running, they will stop the vms while recovery 
         // is running.
         StopStartVMs.startVMs(targetVMs);

         // start threads to wait for startup recovery, then stop them again
         Thread[] threadArr = new Thread[targetVMs.size()];
         for (int i = 0; i < targetVMs.size(); i++) {
            final ClientVmInfo info = (ClientVmInfo)(targetVMs.get(i));
            final int stopMode = ClientVmMgr.toStopMode((String)(stopModes.get(i)));
            final int vmID = info.getVmid();
            Thread workThread = new Thread(new Runnable() {
               public void run() {
                  try {
                     Log.getLogWriter().info("Starting monitor thread to wait for startup recovery in vmID " + vmID); 
                     PRObserver.waitForRebalRecovToStart(vmID, Long.MAX_VALUE, 50);
                     ClientVmMgr.stop("Test is stopping " + vmID + " during recovery", 
                                      stopMode, ClientVmMgr.ON_DEMAND, info);
                     Log.getLogWriter().info("Terminating monitor thread to wait for startup recovery in vmID " + vmID); 
                  } catch (ClientVmNotFoundException e) {
                     throw new TestException(TestHelper.getStackTrace(e));
                  }
               }
            });
            workThread = new HydraSubthread(workThread);
            workThread.setName("Monitor thread for kill during startup recovery");
            threadArr[i] = workThread;
            workThread.start();
         }

         // wait for the threads to complete; when done the vms will again be stopped
         for (int i = 0; i < threadArr.length; i++) {
            try {
               threadArr[i].join();
            } catch (InterruptedException e) {
               throw new TestException(TestHelper.getStackTrace(e));
            }
         } 
         
         // start them again and wait for recovery
         PRObserver.initialize();
         StopStartVMs.startVMs(targetVMs);
         PRObserver.waitForRebalRecov(targetVMs, 1, 1, null, null, false);
      } else {
         throw new TestException("Test configuration problem: Recovery is not set to " +
             "run for stopping or starting, recoveryDelay " + recoveryDelay + 
             ", startupRecoveryDelay " + startupRecoveryDelay);
      }
   }
   Log.getLogWriter().info("Done in KillDuringRecoveryTest.stopStartVms()");
}

}
