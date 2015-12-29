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
import hydra.blackboard.*;
import java.util.*;
import util.*;

/**
 * Ack alert Blackboard
 *
 */
public class ControllerBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "controllerBB_Blackboard";
static String BB_TYPE = "RMI";

// singleton instance of this Blackboard
public static ControllerBB bbInstance = null;

// sharedCounters
public static int ExceptionCounter; 
public static int metCounter; 

// sharedMap keys
public static final String ErrorKey = "Error";
public static final String EnableSlowListenerKey = "EnableSlowListener_";
public static final String EnableSickKey = "EnableSick_";
public static final String EnableDeadKey = "EnableDead_";
public static final String ReadyForInitKey = "ReadyForInit_";
public static final String InitIsCompleteKey = "InitIsComplete_";
public static final String MembershipFailureBegunKey = "MembershipFailureBegun_";
public static final String MembershipFailureCompleteKey = "MembershipFailureComplete_";
public static final String SevereAlertKey = "SevereAlertKey_";
public static final String IsSickKey = "IsSick_";
public static final String IsDeadKey = "IsDead_";
public static final String NumForcedDiscEventsKey = "NumForcedDiscEvents_";
public static final String VmRestarted = "VmRestarted_";


/**
 *  Get the Blackboard
 */
public static ControllerBB getBB() {
   if (bbInstance == null) {
      synchronized ( ControllerBB.class ) {
         if (bbInstance == null) {
            bbInstance = new ControllerBB(BB_NAME, BB_TYPE);
         }
      }
   }
   return bbInstance;
}
   
/*
 *  Zero-arg constructor for remote method invocations.
 */
public ControllerBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ControllerBB(String name, String type) {
   super(name, type, ControllerBB.class);
}

//================================================================================
// Methods that used the sharedMap to key on a prefix plus a vmid

/** Alerts are enable if enabled for sick or slow, but if dead
 *  is also enabled, we expect a forced disconnect, not an alert.
 */
public static boolean isAlertEnabled(int vmID) {
   if (isSicknessEnabled(vmID) || isSlowListenerEnabled(vmID)) {
      return !isPlayDeadEnabled(vmID);
   }
   return false;
} 

//================================================================================
// Slow listener 

/** Enable slow listeners for the current vm
 */
public static void enableSlowListener() {
   enableSlowListener(RemoteTestModule.getMyVmid());
}
/** Enable slow listeners for the given vmid
 */
public static void enableSlowListener(int vmID) {
   Log.getLogWriter().info("ControllerBB: enabling slow listeners for vmID " + vmID);
   Object key = ControllerBB.EnableSlowListenerKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Return whether slow listeners are enabled for the current vm.
 */
public static boolean isSlowListenerEnabled() {
   String key = ControllerBB.EnableSlowListenerKey + RemoteTestModule.getMyVmid();
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}
/** Return whether slow listeners are enabled for the given vm.
 */
public static boolean isSlowListenerEnabled(int vmID) {
   String key = ControllerBB.EnableSlowListenerKey + vmID;
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


//================================================================================
// Initialization

/** Signal that this vm is ready for intialization.
 */
public static void signalReadyForInit() {
   signalReadyForInit(RemoteTestModule.getMyVmid());
}
/** Signal that the given vm is ready for intialization.
 */
public static void signalReadyForInit(int vmID) {
   Log.getLogWriter().info("ControllerBB: ready for init for vmID " + vmID);
   ControllerBB.getBB().getSharedMap().put(ControllerBB.ReadyForInitKey + vmID, "");
}



/** Return whether this vm is ready for initialization.
 */
public static boolean isReadyForInit() {
   return isReadyForInit(RemoteTestModule.getMyVmid());
}
/** Return whether this vm is ready for initialization (typically
 *  after disconnecting).
 */
public static boolean isReadyForInit(int vmID) {
   String key = ControllerBB.ReadyForInitKey + vmID;
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


/** Signal that initialization for this VM is complete.
 */
public static void signalInitIsComplete() {
   signalInitIsComplete(RemoteTestModule.getMyVmid());
}
/** Signal that initialization for the given VM is complete.
 */
public static void signalInitIsComplete(int vmID) {
   Log.getLogWriter().info("ControllerBB: signaling init is complete for vmID " + vmID);
   String key = ControllerBB.InitIsCompleteKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Wait for the current vm to complete initialization
 */
public static void waitForInitialization() {
   waitForInitialization(RemoteTestModule.getMyVmid());
}
/** Wait for the given vm to complete initialization
 */
public static void waitForInitialization(int vmID) {
   Log.getLogWriter().info("Waiting for initialize to complete for vmID " + vmID);
   String key = ControllerBB.InitIsCompleteKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done waiting for initialize to complete for vmID " + vmID);
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}


//================================================================================
// Restarted vms

/** Signal that the given vm has restarted.
 */
public static void signalVmRestarted(int vmID) {
   Log.getLogWriter().info("ControllerBB: signaling init is complete for vmID " + vmID);
   String key = ControllerBB.VmRestarted + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}

/** Wait for the given vm to restart.
 */
public static void waitForRestartedVm(int vmID) {
   Log.getLogWriter().info("Waiting for vmID " + vmID + " to restart");
   String key = ControllerBB.InitIsCompleteKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done Waiting for vmID " + vmID + " to restart");
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}


//================================================================================
// Membership failure

/** Signal that the membershipFailure has begun.
 */
public static void signalMembershipFailureBegun() {
   Log.getLogWriter().info("ControllerBB: signaling membership failure begun for vmID " + RemoteTestModule.getMyVmid());
   String key = ControllerBB.MembershipFailureBegunKey + RemoteTestModule.getMyVmid();
   ControllerBB.getBB().getSharedMap().put(key, new Long(System.currentTimeMillis()));
}
/** Signal that the membershipFailure is complete.
 */
public static void signalMembershipFailureComplete() {
   Log.getLogWriter().info("ControllerBB: signaling membership failure complete for vmID " + RemoteTestModule.getMyVmid());
   String key = ControllerBB.MembershipFailureCompleteKey + RemoteTestModule.getMyVmid();
   ControllerBB.getBB().getSharedMap().put(key, new Long(System.currentTimeMillis()));
}


/** Return the currentTimeMillis value of when membership failure started
 */
public static long getMembershipFailureStartTime() {
   String key = ControllerBB.MembershipFailureBegunKey + RemoteTestModule.getMyVmid();
   return ((Long)(ControllerBB.getBB().getSharedMap().get(key))).longValue(); 
}
/** Return the currentTimeMillis value of when membership failure completed
 */
public static long getMembershipFailureCompletionTime() {
   String key = ControllerBB.MembershipFailureCompleteKey + RemoteTestModule.getMyVmid();
   return ((Long)(ControllerBB.getBB().getSharedMap().get(key))).longValue(); 
}


/** Return whether membership failure is complete for this vm.
 */
public static boolean isMembershipFailureComplete() {
   String key = ControllerBB.MembershipFailureCompleteKey + RemoteTestModule.getMyVmid();
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


/** Wait for membership failure to complete.
 */
public static void waitMembershipFailureComplete() {
   waitMembershipFailureComplete(RemoteTestModule.getMyVmid());
}
/** Wait for membership failure to complete.
 */
public static void waitMembershipFailureComplete(int vmID) {
   Log.getLogWriter().info("Waiting for vmID " + vmID + " to complete membership failure");
   String key = ControllerBB.MembershipFailureCompleteKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done waiting for vmID " + vmID + " to complete membership failure");
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}


//================================================================================
// Sick

/** Enable sickness for the current vm
 */
public static void enableSickness() {
   enableSickness(RemoteTestModule.getMyVmid());
}
/** Enable sickness for the given vmid
 */
public static void enableSickness(int vmID) {
   Log.getLogWriter().info("ControllerBB: enabling sickness for vmID " + vmID);
   Object key = ControllerBB.EnableSickKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Return whether sickness is enabled for the current vm.
 */
public static boolean isSicknessEnabled() {
   String key = ControllerBB.EnableSickKey + RemoteTestModule.getMyVmid();
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}
/** Return whether sickness is enabled for the given vm.
 */
public static boolean isSicknessEnabled(int vmID) {
   String key = ControllerBB.EnableSickKey + vmID;
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


/** Signal that the current vm is sick.
 */
public static void signalIsSick() {
   signalIsSick(RemoteTestModule.getMyVmid());
}
/** Signal that the given vm is sick.
 */
public static void signalIsSick(int vmID) {
   String key = ControllerBB.IsSickKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Wait for the current vm to become sick.
 */
public static void waitForIsSick() {
   waitForIsSick(RemoteTestModule.getMyVmid());
}
/** Wait for the given vmid to become sick.
 */
public static void waitForIsSick(int vmID) {
   Log.getLogWriter().info("Waiting for vmID " + vmID + " to become sick");
   String key = ControllerBB.IsSickKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done waiting for vmID " + vmID + " to become sick");
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}

//================================================================================
// Dead


/** Enable becoming dead for the current vm
 */
public static void enablePlayDead() {
   enablePlayDead(RemoteTestModule.getMyVmid());
}
/** Enable becoming dead for the given vmid
 */
public static void enablePlayDead(int vmID) {
   Log.getLogWriter().info("ControllerBB: enabling play dead for vmID " + vmID);
   Object key = ControllerBB.EnableDeadKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Return whether the current vm is enabled for being dead.
 */
public static boolean isPlayDeadEnabled() {
   String key = ControllerBB.EnableDeadKey + RemoteTestModule.getMyVmid();
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}
/** Return whether the given vm is enabled for being dead.
 */
public static boolean isPlayDeadEnabled(int vmID) {
   String key = ControllerBB.EnableDeadKey + vmID;
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


/** Signal that the current vm is dead.
 */
public static void signalIsDead() {
   signalIsDead(RemoteTestModule.getMyVmid());
}
/** Signal that the given vm is dead.
 */
public static void signalIsDead(int vmID) {
   String key = ControllerBB.IsDeadKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Wait for the current vm to become dead.
 */
public static void waitForIsDead() {
   waitForIsDead(RemoteTestModule.getMyVmid());
}
/** Wait for the given vmid to become dead.
 */
public static void waitForIsDead(int vmID) {
   Log.getLogWriter().info("Waiting for vmID " + vmID + " to become dead");
   String key = ControllerBB.IsDeadKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done waiting for vmID " + vmID + " to become dead");
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}


//================================================================================
// Severe alert

/** Signal that a severe alert occurred in the current vm.
 */
public static void signalSevereAlert() {
   signalSevereAlert(RemoteTestModule.getMyVmid());
}
/** Signal that a severe alert occurred for the given vm.
 */
public static void signalSevereAlert(int vmID) {
   String key = ControllerBB.SevereAlertKey + vmID;
   ControllerBB.getBB().getSharedMap().put(key, "");
}


/** Return whether the current vm has received a severe alert.
 */
public static boolean receivedSevereAlert() {
   return receivedSevereAlert(RemoteTestModule.getMyVmid());
}
/** Return whether the given vm has received a severe alert.
 */
public static boolean receivedSevereAlert(int vmID) {
   String key = ControllerBB.SevereAlertKey + vmID;
   return ControllerBB.getBB().getSharedMap().containsKey(key);
}


/** Wait for the current vm to recognize a severe alert.
 */
public static void waitForSevereAlert() {
   waitForSevereAlert(RemoteTestModule.getMyVmid());
}
/** Wait for the given vmid to recognize a severe alert.
 */
public static void waitForSevereAlert(int vmID) {
   Log.getLogWriter().info("Waiting for vmID " + vmID + " to recognize a severe alert");
   String key = ControllerBB.SevereAlertKey + vmID;
   while (true) {
      if (ControllerBB.getBB().getSharedMap().containsKey(key)) {
         Log.getLogWriter().info("Done waiting for vmID " + vmID + " to recognize a severe alert");
         return;
      } else {
         MasterController.sleepForMs(2000);
      }
      checkForError();
   }
}

//================================================================================
// Other methods

/** Get the value of the shared map counter for this vm.
 */
public static int getMapCounter(String keyBase) {
   String key = keyBase + RemoteTestModule.getMyVmid();
   Object value = ControllerBB.getBB().getSharedMap().get(key);
   if (value == null) {
      return 0;
   } else {
      int intVal = ((Integer)value).intValue();
      return intVal;
   }
}

/** Atomically increment a counter in the ControllerBB sharedMap.
 *
 *  @param keyBase A string from ControllerBB that prefixes a vm id
 *                 to use as a key in the ControllerBB shared map.
 *                 The VM id used is the current vm's id. 
 */
public static void incMapCounter(String keyBase) {
   String key = keyBase + RemoteTestModule.getMyVmid();
   Log.getLogWriter().info("Incrementing sharedMap counter " + key);
   SharedLock slock = ControllerBB.getBB().getSharedLock();
   slock.lock();
   try {
      int newValue = 0;
      Object value = ControllerBB.getBB().getSharedMap().get(key);
      if (value == null) {
         newValue = 1;
      } else {
         newValue = ((Integer)value).intValue() + 1;
      }
      ControllerBB.getBB().getSharedMap().put(key, new Integer(newValue));
      Log.getLogWriter().info("Incremented sharedMap counter, count is now " + newValue);
   } finally {
     slock.unlock();
   }
}

/** Check if an error has been recorded in the blackboard.
 */
public static void checkForError() {
   Object error = ControllerBB.getBB().getSharedMap().get(ControllerBB.ErrorKey);
   if (error != null) {
      throw new TestException(error.toString());
   }
}

/** Return the vmid for the given pid, assuming this was recorded in the bb
 *  during init tasks.
 */
public static int getVmIdForPid(int pid) {
   int vmid = ((Integer)(ControllerBB.getBB().getSharedMap().get("" + pid))).intValue();
   return vmid;
}

/** Clear all blackboard signals.
 */
public static void reset(int vmID) {
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.ErrorKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.EnableSlowListenerKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.EnableSickKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.EnableDeadKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.ReadyForInitKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.InitIsCompleteKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.MembershipFailureBegunKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.MembershipFailureCompleteKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.SevereAlertKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.IsSickKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.IsDeadKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.NumForcedDiscEventsKey + vmID);
   ControllerBB.getBB().getSharedMap().remove(ControllerBB.VmRestarted + vmID);
   Log.getLogWriter().info("ControllerBB: reset vmID " + vmID);
}

/** Check event counters in the ControllerBB shared map.
 *  For example, if baseKey is ControllerBB.NumForcedDiscEventsKey and vmList is a 
 *  list containing vmId 2, and expectedCount is 1, then the event count of 
 *  key with base NumForcedDiscEvents plus vmId 2 is expected to be 1, and any 
 *  other key found in the map that is prefixed by baseKey should have value be 0.
 *  Throws an exception if the conditions are not satisfied.
 *  
 *  @param baseKey The base string of the key to check (the prefix to the vm id)
 *  @param vmList The vms (ClientVmInfo) for the baseKey should have a value of expectedCount.
 *  @param expectedCount The expected value of any baseKeys that refer to the
 *         given vmIds, otherwise the expectedCount is 0.
 */
public static void checkEventCounters(String baseKey, List vmList, int expectedCount) {
   Map aMap = ControllerBB.getBB().getSharedMap().getMap();
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      String key = (String)(it.next());
      if (key.startsWith(baseKey)) { // found a key prefixed by baseKey
         int value = ((Integer)(aMap.get(key))).intValue(); // value for the key
         boolean foundInVmIdList = false;
         for (int i = 0; i < vmList.size(); i++) { 
            int vmId = ((Integer)((ClientVmInfo)(vmList.get(i))).getVmid()).intValue();
            if (key.endsWith("_" + vmId)) { // found a baseKey with a vmid in the vmList 
               foundInVmIdList = true;
               if (value != expectedCount) {
                  throw new TestException("Expected value for BB key " + key + " to be " + expectedCount + 
                                          " but it is " + value); 
               }
            }
         }
         if (!foundInVmIdList) { // this key's value should be 0
            if (value != 0) {
               throw new TestException("Expected value for BB key " + key + " to be 0 but it is " + value); 
            }
         }
      }
   }
}
}
