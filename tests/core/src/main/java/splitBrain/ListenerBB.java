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
 * Listener Blackboard
 *
 */
public class ListenerBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "Listener_Blackboard";
static String BB_TYPE = "RMI";

// singleton instance of this Blackboard
public static ListenerBB bbInstance = null;

// sharedCounters for RegionMembershipListener
public static int numAfterRemoteRegionCrashEvents;  
public static int numAfterRemoteRegionCreateEvents;  
public static int numAfterRemoteRegionDepartureEvents;  

// sharedCounters for SystemMembershipListener
public static int numMemberCrashEvents;  
public static int numMemberJoinedEvents;  
public static int numMemberLeftEvents;  

/**
 *  Get the Blackboard
 */
public static ListenerBB getBB() {
   if (bbInstance == null) {
      synchronized ( ListenerBB.class ) {
         if (bbInstance == null) 
            bbInstance = new ListenerBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/*
 *  Zero-arg constructor for remote method invocations.
 */
public ListenerBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ListenerBB(String name, String type) {
   super(name, type, ListenerBB.class);
}

/** Check for the the proper number of afterRemoteRegionCrashEvents
 */
public static void checkAfterRemoteRegionCrashEvents() {
   long numCrashEvents = ListenerBB.getBB().getSharedCounters().read(ListenerBB.numAfterRemoteRegionCrashEvents);
   long numVMsInDS = SplitBrainBB.getBB().getSharedCounters().read(SplitBrainBB.NumVMsInDS);
   long numVMsStopped = SplitBrainBB.getBB().getSharedCounters().read(SplitBrainBB.NumVMsStopped);
   // the number of expected crash events is: each vm that did not receive a forced disconnect gets
   // an event for the vm(s) that did receive a forced disconnect, plus the vm(s) that received that
   // forced disconnect get an event for each of the surviving vms that have that region defined.
// lynn - waiting for specification as to how many crash events to expect
//   long numExpectedCrashEvents = 2 * (numVMsInDS - numVMsStopped);
   long numExpectedCrashEvents = numVMsInDS - numVMsStopped;
   Log.getLogWriter().info("numVMsInDS: " + numVMsInDS + ", numVMsStopped: " + numVMsStopped +
                           ", numExpectedCrashEvents: " + numExpectedCrashEvents);
   TestHelper.waitForCounter(ListenerBB.getBB(), 
                             "ListenerBB.numAfterRemoteRegionCrashEvents", 
                             ListenerBB.numAfterRemoteRegionCrashEvents, 
                             numExpectedCrashEvents, 
                             true, 
                             -1,
                             2000);
}

}
