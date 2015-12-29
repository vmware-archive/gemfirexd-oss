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
import util.*;

import java.util.*;

import com.gemstone.gemfire.distributed.*;

/**
 * SplitBrain Blackboard
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.5
 */
public class SplitBrainBB extends Blackboard {
   
// Blackboard creation variables
static String SPLITBRAIN_BB_NAME = "SplitBrain_Blackboard";
static String SPLITBRAIN_BB_TYPE = "RMI";

// These lists contain vm names (e.g. vm_1, vm_2)
static String ELIGIBLE_COORDINATORS = "EligibleCoordinators";
static String FORCED_DISCONNECT_LIST = "forcedDisconnectList";
static String RECONNECTED_LIST = "reconnectedList";
static String EXPECT_RECONNECTED_LIST = "expectedReconnectedList";
static String EXPECT_FORCED_DISCONNECTS = "losingPartitionList";
static String LOSING_SIDE_HOST = "";

// CrashEventMemberList, used for tracking proper receipt of memberCrashedEvents
// by both losing and surviving side of partition
static String CRASH_EVENT_MEMBER_LIST = "crashEventMemberList";

// singleton instance of this Blackboard
public static SplitBrainBB bbInstance = null;

// sharedCounters
public static int ExecutionNumber;  // the serial execution number
public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
public static int TimeToRespond;    // used to sync all threads when they respond to becoming sick
public static int FinishedTask;     // used to sync all threads at the end of a task
public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException
public static int NumVMsInDS;       // the number of vms in the ds; used to determine the number of 
                                    // afterRemoteRegionCrashed events for forced disconnects
public static int NumVMsStopped;    // the current number of vms being stopped by the test
public static int OpsComplete;      // signals when ops are complete in a known keys test

// rebalance/splitBrain dropConnection control counter
public static int dropConnectionComplete;  // signals when SBUtil.dropConnection has completed (including dropWaitTimeSec)

// gii NetworkPartitionTest Counters (regionSizes)
public static int loadClientRegionSize;   // size across all loadClients

public static int adminForcedDisconnects; // count of admin VMs that got forced-disconnects
public static int expectedAdminForcedDisconnects; // expected number of admin VMS that will get forced-disconnects

/**
 *  Get the Blackboard
 */
public static SplitBrainBB getBB() {
   if (bbInstance == null) {
      synchronized ( SplitBrainBB.class ) {
         if (bbInstance == null) 
            bbInstance = new SplitBrainBB(SPLITBRAIN_BB_NAME, SPLITBRAIN_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/*
 *  Zero-arg constructor for remote method invocations.
 */
public SplitBrainBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public SplitBrainBB(String name, String type) {
   super(name, type, SplitBrainBB.class);
}

/**
 *  Initialize the SplitBrain Blackboard
 */
public static void HydraTask_initialize() {
   SharedMap aMap = getBB().getSharedMap();
   aMap.put(ELIGIBLE_COORDINATORS, new HashSet());
   aMap.put(FORCED_DISCONNECT_LIST, new HashSet());
   aMap.put(RECONNECTED_LIST, new HashSet());
   aMap.put(EXPECT_FORCED_DISCONNECTS, new HashSet());
   aMap.put(CRASH_EVENT_MEMBER_LIST, new HashSet());
   aMap.put(EXPECT_RECONNECTED_LIST, new HashSet());
}

/**
 *  Add clientName to list of eligibleCoordinators.  This method is meant
 *  to be called by client managed locators after they have determined they
 *  are not the current coordinator.
 */
public synchronized static void addEligibleCoordinator(String clientName) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   // coordinate access among VMs
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(ELIGIBLE_COORDINATORS);
   aSet.add(clientName);
   aMap.put(ELIGIBLE_COORDINATORS, aSet);
   bb.getSharedLock().unlock();
   Log.getLogWriter().info("After adding " + clientName + ", EligibleCoordinators = " + aSet);
}

/**
 *  Add vmId to list of clients expected on the losing partition (these clients/locators should
 *  see ForcedDisconnectExceptions (in Shutdown or CacheClosed Exceptions or in the RegionDestroyedEvent).
 */
public synchronized static void addExpectForcedDisconnect( int vmid ) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   String clientVm = "vm_" + vmid;
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(EXPECT_FORCED_DISCONNECTS);
   if (!aSet.contains(clientVm)) {
      aSet.add(clientVm);
      aMap.put(EXPECT_FORCED_DISCONNECTS, aSet);
      Log.getLogWriter().info("After adding " + clientVm + ", ExpectForcedDisconnect = " + aSet);
   }
   bb.getSharedLock().unlock();
}

/**
 *  Easy access to list of clientVms that expect to receive forced disconnects
 */
public synchronized static Set getExpectForcedDisconnects() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(EXPECT_FORCED_DISCONNECTS));
}

/**
 *  Add clientVm to a list of client Vms which received 
 *  - RegionDestroyedEvent with Operation = FORCED_DISCONNECT
 *  - CacheClosedException Caused by ForcedDisconnectException
 *  - ShutdownException Caused by ForcedDisconnectException
 */
public synchronized static void addDisconnectedClient(int vmid) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   String clientVm = "vm_" + vmid;
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(FORCED_DISCONNECT_LIST);
   if (!aSet.contains(clientVm)) {
      aSet.add(clientVm);
      aMap.put(FORCED_DISCONNECT_LIST, aSet);
   }
   bb.getSharedLock().unlock();
   Log.getLogWriter().info("After adding " + clientVm + ", ForcedDisconnectList = " + aSet);
}

/**
 *  Easy access to list of clients that reported forced disconnects
 */
public synchronized static Set getForcedDisconnectList() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(FORCED_DISCONNECT_LIST));
}

/**
 *  Add clientVm to a list of client Vms which received 
 *  - RegionDestroyedEvent with Operation = FORCED_DISCONNECT
 *  - CacheClosedException Caused by ForcedDisconnectException
 *  - ShutdownException Caused by ForcedDisconnectException
 */
public synchronized static void addExpectedReconnectedClient(int vmid) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   String clientVm = "vm_" + vmid;
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(EXPECT_RECONNECTED_LIST);
   if (!aSet.contains(clientVm)) {
      aSet.add(clientVm);
      aMap.put(EXPECT_RECONNECTED_LIST, aSet);
   }
   bb.getSharedLock().unlock();
   Log.getLogWriter().info("After adding " + clientVm + ", ExpectedReconnectList = " + aSet);
}

/**
 *  Easy access to list of clients that reported forced disconnects
 */
public synchronized static Set getExpectedReconnectList() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(EXPECT_RECONNECTED_LIST));
}

/**
 *  Add clientVm to a list of client Vms which were disconnected
 *  and then became reconnected 
 */
public synchronized static void addReconnectedClient(int vmid) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   String clientVm = "vm_" + vmid;
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(RECONNECTED_LIST);
   if (!aSet.contains(clientVm)) {
      aSet.add(clientVm);
      aMap.put(RECONNECTED_LIST, aSet);
   }
   bb.getSharedLock().unlock();
   Log.getLogWriter().info("After adding " + clientVm + ", ReconnectList = " + aSet);
}

/**
 *  Easy access to list of clients that reported reconnecting
 */
public synchronized static Set getReconnectedList() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(RECONNECTED_LIST));
}

/** 
 *  Get list of eligibleCoordinators
 */
public static Set getEligibleCoordinators() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(ELIGIBLE_COORDINATORS));
}

/**
 *  Invoke in a losing side VM to post hostname to BB (as the losing side host)
 */
public static void postSelfAsLosingSideHost() {
   putLosingSideHost(RemoteTestModule.getMyHost());
}

/**
 *  Put the losingSideHost (actual hostname) out to blackboard
 */
public static void putLosingSideHost(String hostName) {
   Log.getLogWriter().info("Posting " + hostName + " as losingSideHost");
   SharedMap aMap = getBB().getSharedMap();
   aMap.put(LOSING_SIDE_HOST, hostName);
}

/**
 *  Get the losingSideHost (actual hostname) from the blackboard
 */
public static String getLosingSideHost() {
   SharedMap aMap = getBB().getSharedMap();
   return ((String)aMap.get(LOSING_SIDE_HOST));
}

/**
 *  Add to a list of members expected to receive memberCrashed events.
 */
public synchronized static void addMember(DistributedMember dm, boolean inSurvivingSide) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   // coordinate access among VMs
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(CRASH_EVENT_MEMBER_LIST);
   aSet.add(new CrashEventMemberInfo(dm, inSurvivingSide));
   aMap.put(CRASH_EVENT_MEMBER_LIST, aSet);
   bb.getSharedLock().unlock();
   Log.getLogWriter().info("After adding " + dm + ", CrashEventMemberList = " + aSet);
}

/**
 *  Update member memberCrashed event count
 */
public synchronized static void updateMember(DistributedMember dm, boolean reportedBySurvivingSide) {
   Blackboard bb = getBB();
   SharedMap aMap = bb.getSharedMap();
   // coordinate access among VMs
   bb.getSharedLock().lock();
   Set aSet = (Set)aMap.get(CRASH_EVENT_MEMBER_LIST);
   // Get the member for given DM
   CrashEventMemberInfo m = null;
   for (Iterator it=aSet.iterator(); it.hasNext(); ) {
      m = (CrashEventMemberInfo)it.next();
      if (m.getDm().equals(dm)) {
         m.incrementCounter(reportedBySurvivingSide);
         break;
      }
   }
   aMap.put(CRASH_EVENT_MEMBER_LIST, aSet);
   bb.getSharedLock().unlock();

   // don't throw this until we've released the lock!
   if (m == null) {
      throw new TestException("Received Unexpected memberCrashed event for " + dm.toString());
   }

   Log.getLogWriter().info("After updating " + dm + ", CrashEventMemberList = " + aSet);
}

/**
 *  Easy access to list of members expecting memberCrashedEvents
 */
public synchronized static Set getMembers() {
   SharedMap aMap = getBB().getSharedMap();
   return ((Set)aMap.get(CRASH_EVENT_MEMBER_LIST));
}

}
