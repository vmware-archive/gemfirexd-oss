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
import com.gemstone.gemfire.cache.*;
import util.*;

/**
 * MLRio Use Case Blackboard
 */
public class MLRioBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "MLRio_Blackboard";
static String BB_TYPE = "RMI";

// singleton instance of this Blackboard
public static MLRioBB bbInstance = null;

// sharedCounters for BridgeMembershipListener (Client Events)
public static int actualClientJoinedEvents;
public static int actualClientDepartedEvents;
public static int actualClientCrashedEvents;

// shared Counters for BridgeMemberhipListener (Server Events)
public static int actualServerJoinedEvents;
public static int actualServerDepartedEvents;
public static int actualServerCrashedEvents;

// Shared Counters for MLRio (with conflation) for missed updates
public static int missedUpdates;
public static int duplicateUpdates;

// Shared counter for MLRio (RegionDestroyedEvent(FORCED_DISCONNECT))
public static int forcedDisconnects;

// Publishers (putSequentialKeys) increment this when they are Publishing
// (on a per task invocation basis)
public static int Publishing;          // see putSequentialKeys
public static int donePublishing;      // see putSequentialKeys
public static int numPublishers;       // set by publishers on 1st invocation
public static int validationComplete;  // seee validateSequentialKeys

// Execution number counter (owned by forceDisconnect/healthController tasks)
public static int executionNumber;

//Shared counter for dynamic region creations
public static int dynamicRegionCreates;
public static int dynamicRegionClients;

// The healthController uses a SharedLock SharedCondition in order to 
// notify the publishers that it has completed the current round of 
// validation.
public static final String healthControllerComplete = "healthControllerComplete";

/**
 *  Get the Blackboard
 */
public static MLRioBB getBB() {
   if (bbInstance == null) {
      synchronized ( MLRioBB.class ) {
         if (bbInstance == null) 
            bbInstance = new MLRioBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/*
 *  Zero-arg constructor for remote method invocations.
 */
public MLRioBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public MLRioBB(String name, String type) {
   super(name, type, MLRioBB.class);
}

/**
 * Clear the ClientMembership Event counters (events processed by BridgeServers)
 */
public static void clearClientMembershipCounters() {
   SharedCounters sc = MLRioBB.getBB().getSharedCounters();
   sc.zero(MLRioBB.actualClientJoinedEvents);
   sc.zero(MLRioBB.actualClientDepartedEvents);
   sc.zero(MLRioBB.actualClientCrashedEvents);
}

/**
 * Clear the ServerMembership Event counters (events processed by edge clients)
 */
public static void clearServerMembershipCounters() {
   SharedCounters sc = MLRioBB.getBB().getSharedCounters();
   sc.zero(MLRioBB.actualServerJoinedEvents);
   sc.zero(MLRioBB.actualServerDepartedEvents);
   sc.zero(MLRioBB.actualServerCrashedEvents);
}

/**
 * Clear all MembershipEvent counters
 */
public static void clearMembershipCounters() {
   clearClientMembershipCounters();
   clearServerMembershipCounters();
}
}
