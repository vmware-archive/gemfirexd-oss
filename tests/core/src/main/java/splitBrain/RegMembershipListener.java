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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;

public class RegMembershipListener extends AbstractListener implements RegionMembershipListener {

/** used to check the events are valid 
 */
static public List eventsReceivedInThisVM = new ArrayList();

/** Do some checking of the afterRemoteRegionCrash event that occurs in the current vm.
 *  More checking can be done by called checkEventCounters(). Between this and the
 *  checkEventCounters call, it should ensure that we got the appropriate number of
 *  events from the appropriate vms.
 */
public synchronized void afterRemoteRegionCrash(RegionEvent event) {
   try {
      doAfterRemoteRegionCrash(event);
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }
}

protected void doAfterRemoteRegionCrash(RegionEvent event) {
   String eventStr = logCall("afterRemoteRegionCrash", event);
   ListenerBB.getBB().getSharedCounters().increment(ListenerBB.numAfterRemoteRegionCrashEvents);

   // make sure this event does not refer to the current vm; it should be from another vm
   int thisPid = ProcessMgr.getProcessId();
   if (event.getDistributedMember().getProcessId() == thisPid) { 
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + 
          "; event should not refer to member " + event.getDistributedMember();
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }   

   // make sure that we don't get two events from the same distributed member AND the same region
   if (eventsReceivedInThisVM == null) {
      eventsReceivedInThisVM.add(event);
   } else {
      for (int i = 0; i < eventsReceivedInThisVM.size(); i++) {
         RegionEvent priorEvent = (RegionEvent)(eventsReceivedInThisVM.get(i));
         if (priorEvent.getDistributedMember().getProcessId() == event.getDistributedMember().getProcessId()) {
            if (priorEvent.getRegion().getFullPath().equals(event.getRegion().getFullPath())) {
               // got two events with same distributed member and region
               String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + 
                   "; event " + eventStr + " refers to the same pid and region name as prior event " + priorEvent;
               Log.getLogWriter().info(errStr);
               ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
            }        
         }
      }
   }

   // make sure the event is from a vm that either expects a forced disconnect
   // or is expected to get shutdown after receiving alerts
   int eventPid = event.getDistributedMember().getProcessId();
   int vmId = ControllerBB.getVmIdForPid(eventPid);      
   if (!ControllerBB.isPlayDeadEnabled(vmId) && !ControllerBB.isAlertEnabled(vmId)) {
      // this vm got an event to signal a forced disconnect in another vm, but that other vm
      // was not targeted for a forced disconnect
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + 
          "; event " + eventStr + " was unexpected because this vm was not targeted for a forced disconnect";
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }
}

public void afterRemoteRegionCreate(RegionEvent event) {
    logCall("afterRemoteRegionCreate", event);
    ListenerBB.getBB().getSharedCounters().increment(ListenerBB.numAfterRemoteRegionCreateEvents);
}

public void afterRemoteRegionDeparture(RegionEvent event) {
    logCall("afterRemoteRegionDeparture", event);
    ListenerBB.getBB().getSharedCounters().increment(ListenerBB.numAfterRemoteRegionDepartureEvents);
}

public void close() {
   Log.getLogWriter().info("Invoked " + this.getClass().getName() + ": close");
}

public void initialMembers(Region aRegion, DistributedMember[] initialMembers) {
    StringBuffer aStr = new StringBuffer();
    aStr.append("Invoked " + this.getClass().getName() + ": initialMembers with: ");
    for (int i = 0; i < initialMembers.length; i++) {
       aStr.append(initialMembers[i] + " ");
    }
    Log.getLogWriter().info(aStr.toString());
}

// silent listener methods
public void afterCreate(EntryEvent event) { }
public void afterUpdate(EntryEvent event) { }
public void afterInvalidate(EntryEvent event) { }
public void afterDestroy(EntryEvent event) { }
public void afterRegionClear(RegionEvent event) { }
public void afterRegionCreate(RegionEvent event) { }
public void afterRegionDestroy(RegionEvent event) { }
public void afterRegionInvalidate(RegionEvent event) { }
public void afterRegionLive(RegionEvent event) { }

}
