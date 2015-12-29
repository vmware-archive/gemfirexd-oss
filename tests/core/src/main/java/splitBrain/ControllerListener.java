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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import hydra.blackboard.*;
import util.*;

import java.util.*;

/** Listener to control a vm that becomes wither sick, slow, sick and dead, or
 *  slow and dead.
 *
 *  How to use: 
 *      Install this listener in a region in a vm where it is desired for 
 *         that vm to beccome unhealthy in some way.
 *      When ready to cause unhealthiness, call any of:
 *            1) ControllerBB.enableSlowListener(vmID)
 *            2) ControllerBB.enableSickness(vmID)
 *            4) ControllerBB.enableSlowListener(vmID) AND ControllerBB.enableDead(vmID)
 *            5) ControllerBB.enableSickness(vmID) AND ControllerBB.enableDead(vmID)
 *         This makes the target vm ready to respond to unhealthiness, but it 
 *         will not occur until an afterCreate event is invoked in the next step. 
 *      Do a put from any vm such that it will invoke afterCreate on the target
 *         vm(s). The afterCreate call will wait until severe alerts are recognized
 *         for cases 1 and 2 and will wait until a forced disconnect occurs for cases
 *         3 and 4,
 * 
 *      Call ControllerBB.reset(vmID) to clear the bb of any signals used.
 *         After this call, the given vm id can become slow again if desired. 
 *      It is recommended that the user frequently call checkForError() to throw
 *         any errors detected by this listener. 
 *
 * Notes: Only install this listener in one region per vm. It is not intended
 *        to work with multiple regions in a single vm.
 *        Don't forget to enable alerts in the product (it is off by
 *        default) by setting ack-severe-alert-threshold (this begins after
 *        ack-wait-threshold has elapsed). 
 */
public class ControllerListener extends util.AbstractListener 
                                implements CacheListener, Declarable {

public static boolean validateForcedDisconnects = true;

//================================================================================ 
// event methods

public void afterDestroy(EntryEvent event) { }
public void afterInvalidate(EntryEvent event) { } 
public void afterUpdate(EntryEvent event) { }
public void afterRegionInvalidate(RegionEvent event) { }
public void afterRegionClear(RegionEvent event) { }
public void afterRegionCreate(RegionEvent event) { }
public void afterRegionLive(RegionEvent event) { }
public void init(java.util.Properties prop) { }
public void close() { }

public void afterCreate(EntryEvent event) { 
   try {
      doAfterCreate(event);
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }
}

/** Listener invocation to control slow, sick and dead vms.
 */
protected void doAfterCreate(EntryEvent event) {
   final DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   final Cache theCache = CacheHelper.getCache();
   if (ControllerBB.isSlowListenerEnabled()) { // slow 
      logCall("afterCreate", event);
      if (ControllerBB.isPlayDeadEnabled()) { // slow and dead
         SBUtil.playDead();
         waitForFDThenInit(ds, theCache);
      } else { // slow and not dead
         try {
            ControllerBB.waitForSevereAlert(); // AckAlertListener signals it
         } catch (TestException e) {
            Log.getLogWriter().info(e.getMessage());
            ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, e.getMessage());
         } 
      }
      Log.getLogWriter().info("Returning from doAfterCreate");
   } else if (ControllerBB.isSicknessEnabled()) {
      logCall("afterCreate", event);
      SBUtil.beSick();
      if (ControllerBB.isPlayDeadEnabled()) {
         SBUtil.playDead();
         // since we must return right away (we are sick, not slow and waiting around
         // means we are slow), spawn a thread to wait for a forced disconnect and reinit
         String threadName = Thread.currentThread().getName();
         Thread monitorThread = new Thread(new Runnable() {
            public void run() {
               waitForFDThenInit(ds, theCache);
               Log.getLogWriter().info("Done in wait for forced disconnect thread");
            } 
         }, "waitForFDThenInitThread" + threadName);
         monitorThread.start();
      } 
      Log.getLogWriter().info("Returning from doAfterCreate");
   }      
}

public void afterRegionDestroy(RegionEvent event) { 
   try {
      doAfterRegionDestroy(event);
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }
}

/** Recognize a forced disconnect
 */
protected void doAfterRegionDestroy(RegionEvent event) {
   String eventStr = logCall("afterRegionDestroy", event);
   Operation op = event.getOperation();
   if (op.equals(Operation.FORCED_DISCONNECT)) {
      Log.getLogWriter().info("Received an afterRegionDestroy event with Operation.FORCED_DISCONNECT");
      ControllerBB.incMapCounter(ControllerBB.NumForcedDiscEventsKey);
      if (validateForcedDisconnects) {
         if (ControllerBB.isPlayDeadEnabled()) { // we were trying to cause this
            // all is OK
         } else {
            String errStr = "Got an unexpected forced disconnect event invoked in vmID " +
                            RemoteTestModule.getMyVmid() + ": " + eventStr;
            Log.getLogWriter().info(errStr);
            ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
         }
      }
   }   
}

/** Can be overridden in subclasses for anything that needs to be done
 *  after a disconnect.
 */
public void initAfterDisconnect() {

}

/** Can be overridden in subclasses for reinitializing after
 *  forced disconnects.
 */
public void afterDisconnect() {

}

/** Wait for a ForcedDisconnect, wait for all threads in this
 *  vm to get exceptions, then reinitialize
 */
protected void waitForFDThenInit(DistributedSystem ds, Cache theCache) {
   // wait for forced disconnect
   try {
      ForcedDiscUtil.waitForForcedDiscConditions(ds, theCache);
   } catch (TestException e) {
      Log.getLogWriter().info(e.getMessage());
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, e.getMessage());
   } 
   afterDisconnect();

   // reinitialize now that everything has been recognized
   Log.getLogWriter().info("ControllerListener: Waiting to be ready for initialization...");
   while (true) {
      if (ControllerBB.isReadyForInit()) {
         break;
      } else {
         MasterController.sleepForMs(2000);
      }
   }
   initAfterDisconnect();
   Log.getLogWriter().info("ControllerListener: Done with initialization");
   ControllerBB.signalInitIsComplete();
}

}
