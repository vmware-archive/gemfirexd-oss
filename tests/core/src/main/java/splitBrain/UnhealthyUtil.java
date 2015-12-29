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

public class UnhealthyUtil {

/** Cause a changed in the health of target vms.
 *
 */
public static void causeUnhealthiness(List vmList, final Region controllerRegion) {
   Log.getLogWriter().info("In UnhealthyUtil.causeUnhealthiness, targeting " + vmList.size() + " vm(s) to become unhealthy...");
   List unhealthinessList = new ArrayList();
   List playDeadList = new ArrayList();
   for (int i = 1; i <= vmList.size(); i++) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size()-1);
      unhealthinessList.add(TestConfig.tab().stringAt(SplitBrainPrms.unhealthiness));
      playDeadList.add(new Boolean(TestConfig.tab().booleanAt(SplitBrainPrms.playDead)));
   }

   // start threads to cause unhealthiness so they happen concurrently
   List threadList = new ArrayList();
   for (int i = 0; i < vmList.size(); i++) {
      final ClientVmInfo info = (ClientVmInfo)(vmList.get(i));
      final String unhealthiness = (String)(unhealthinessList.get(i));
      final boolean playDead = ((Boolean)(playDeadList.get(i))).booleanValue();
      Thread aThread = new HydraSubthread(new Runnable() {
         public void run() {
            try {
               Log.getLogWriter().info("In thread to cause unhealthiness");
               causeUnhealthiness(info, unhealthiness, playDead, controllerRegion);
               Log.getLogWriter().info("Done in thread to cause unhealthiness");
            } catch (Exception e) {
               String errStr = TestHelper.getStackTrace(e);
               Log.getLogWriter().info(errStr);
               ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
            }
         }
      });
      aThread.start();
      threadList.add(aThread);
   }
   Log.getLogWriter().info("Joining unhealthiness threads");
   for (int i = 0; i < threadList.size(); i++) {
      Thread aThread = (Thread)(threadList.get(i));
      try {
         ControllerBB.checkForError();
         aThread.join();
         Log.getLogWriter().info("Done with join for thread " + aThread.getName());
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   ControllerBB.checkForError();
   Log.getLogWriter().info("Done joining unhealthiness threads");
}

/** Cause an alert or forcedDisconnect in the given vm.
 *
 *  @param targetVM The vm to become unhealthy in some way.
 *  @param unhealthiness Either "sick" or "slow".
 *  @param playDead Either true (for playDead) or false (not dead).
 */
protected static void causeUnhealthiness(ClientVmInfo targetVM, String unhealthiness, boolean playDead, Region controllerRegion) {
   if (unhealthiness.equalsIgnoreCase("slow")) {
      if (playDead) {
         slowAndDeadUnhealthiness(targetVM, controllerRegion);
      } else {
         slowOnlyUnhealthiness(targetVM, controllerRegion);
      }
   } else if (unhealthiness.equalsIgnoreCase("sick")) {
      if (playDead) {
         sickAndDeadUnhealthiness(targetVM, controllerRegion);
      } else {
         sickOnlyUnhealthiness(targetVM, controllerRegion);
      }
   } else {
      throw new TestException("Unknown " + unhealthiness);
   }
   ControllerBB.checkForError();
}

protected static void slowOnlyUnhealthiness(ClientVmInfo targetVM, Region controllerRegion) {
   int vmid = targetVM.getVmid().intValue();
   ControllerBB.enableSlowListener(vmid);

   // put returns when alert listener detects an alert on the target vm 
   Log.getLogWriter().info("Calling put to cause slowness in vm " + vmid);
   controllerRegion.put(new Long(System.currentTimeMillis()), "");
   Log.getLogWriter().info("Done calling put to cause slowness in vm " + vmid);

   // restart the vm
   try {
      ClientVmMgr.stop("Stopping vm " + vmid + " because the system received alerts that it is not healthy", 
         ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)),
         ClientVmMgr.ON_DEMAND,
         targetVM);
      ClientVmMgr.start("Restarting vm " + vmid + " after it became unhealthy", targetVM);
      ControllerBB.signalInitIsComplete(vmid);
      ListenerBB.checkAfterRemoteRegionCrashEvents();
      ListenerBB.getBB().getSharedCounters().zero(ListenerBB.numAfterRemoteRegionCrashEvents);
   } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   ControllerBB.checkForError();
}

protected static void sickOnlyUnhealthiness(ClientVmInfo targetVM, final Region controllerRegion) {
   final int vmid = targetVM.getVmid().intValue();
   ControllerBB.enableSickness(vmid);

   // put to encounter sickness; put returns after vm is killed (below)
   String threadName = Thread.currentThread().getName();
   Thread aThread = new Thread(new Runnable() {
      public void run() {
        // put returns immediately (after beSick is called) unless the sick mbr
        // has just created the bucket and the primary bucket holder is sending
        // it dual cacheop/notification messages
        Log.getLogWriter().info("Calling put to cause sickness in vm " + vmid);
        controllerRegion.put(new Long(System.currentTimeMillis()), "");
        Log.getLogWriter().info("Done calling put to cause sickness in vm " + vmid);

        // now make sure we encounter sickeness by doing another put
        Log.getLogWriter().info("Calling put to encounter sickness in vm " + vmid);
        controllerRegion.put(new Long(System.currentTimeMillis()), "");
        Log.getLogWriter().info("Done calling put to encounter sickness in vm " + vmid);
      }
   }, "PutThreadFor_" + threadName);
   aThread.start();

   // wait for the alert to be recognized, then restart
   ControllerBB.waitForSevereAlert(vmid); 
   try {
      ClientVmMgr.stop("Stopping vm " + vmid + " because the system received alerts that it is not healthy", 
         ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)),
         ClientVmMgr.ON_DEMAND,
         targetVM);
      // join waits for the put to complete
      try {
         Log.getLogWriter().info("Waiting for put to complete since vm with id " + vmid + " has been stopped");
         aThread.join();
         Log.getLogWriter().info("Done waiting for put to complete since vm with id " + vmid + " has been stopped");
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      ClientVmMgr.start("Restarting vm " + vmid + " after it became unhealthy", targetVM);
      ControllerBB.signalInitIsComplete(vmid);
      ListenerBB.checkAfterRemoteRegionCrashEvents();
      ListenerBB.getBB().getSharedCounters().zero(ListenerBB.numAfterRemoteRegionCrashEvents);
   } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   ControllerBB.checkForError();
}

protected static void slowAndDeadUnhealthiness(ClientVmInfo targetVM, Region controllerRegion) {
   int vmid = targetVM.getVmid().intValue();
   ControllerBB.enableSlowListener(vmid);
   ControllerBB.enablePlayDead(vmid);

   // put returns after a forced disconnect is detected in the target vm
   Log.getLogWriter().info("Calling put to cause slowness and play dead in vm " + vmid);
   controllerRegion.put(new Long(System.currentTimeMillis()), "");
   Log.getLogWriter().info("Done calling put to cause slowness and play dead in vm " + vmid);
   ControllerBB.checkForError();

   // reinitialize target vm
   ControllerBB.checkForError(); 
   ControllerBB.signalReadyForInit(vmid); // signal that its ok to run reinitialization
   ControllerBB.waitForInitialization(vmid); // wait for reinitialization
   ListenerBB.checkAfterRemoteRegionCrashEvents();
   ListenerBB.getBB().getSharedCounters().zero(ListenerBB.numAfterRemoteRegionCrashEvents);
   ControllerBB.checkForError(); 
}

protected static void sickAndDeadUnhealthiness(ClientVmInfo targetVM, Region controllerRegion) {
   int vmid = targetVM.getVmid().intValue();
   ControllerBB.enableSickness(vmid);
   ControllerBB.enablePlayDead(vmid);

   // put returns immediately (after beSick and playDead is called)
   Log.getLogWriter().info("Calling put to cause sickness and play dead in vm " + vmid);
   controllerRegion.put(new Long(System.currentTimeMillis()), "");
   Log.getLogWriter().info("Done calling put to cause sickness and play dead in vm " + vmid);

   // put to encounter sickness and play dead; put returns when a forced disconnect occurs
   Log.getLogWriter().info("Calling put to encounter sickness and play dead in vm " + vmid);
   controllerRegion.put(new Long(System.currentTimeMillis()), "");
   Log.getLogWriter().info("Done calling put to encounter sickness and play dead in vm " + vmid);

   // spawned thread in target vm waits for a forced disconnect, then reinitializes
   ControllerBB.signalReadyForInit(vmid);
   ControllerBB.waitForInitialization(vmid); // wait for reinitialization
   ListenerBB.checkAfterRemoteRegionCrashEvents();
   ListenerBB.getBB().getSharedCounters().zero(ListenerBB.numAfterRemoteRegionCrashEvents);
   ControllerBB.checkForError();
}

}
