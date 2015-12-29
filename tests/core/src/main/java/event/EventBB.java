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
package event;

import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task
 * threads in an {@link EventTest} do.  For instance, it has counters
 * that are incremented when region operations like <code>put</code>
 * and <code>invalidate</code> are performed.  It also keeps track of 
 *
 * @author Lynn Gallinat
 * @since 2.0
 */
public class EventBB extends Blackboard {
   
// Blackboard creation variables
static String EVENT_BB_NAME = "Event_Blackboard";
static String EVENT_BB_TYPE = "RMI";

  /** Key in the blackboard sharedMap for the hydra parameter {@link
   * hydra.Prms#serialExecution} */
static String SERIAL_EXECUTION = "SerialExecution";

// Key in the blackboard sharedMap for keeping track of region names currently in use
static String CURRENT_REGION_NAMES = "CurrentRegionNames";

// Proxy SerialRoundRobin keys
// RegionOperation (executed by the leader of the RR)
static String ROUND_ROBIN = "RoundRobin";
static String REGION_OP = "RegionOp";
// RegionName- target of operation (region)
static String TARGET_REGION = "TargetRegion";
// Key for first client in the round robin
static String RR_LEADER = "RRLeader";

// Key in the blackboard SharedMap for keeping track of operations performed
// so they can be duplicated in the dataStore
// This map actually contains a list of tx.Operations, which can then be
// duplicated in the ShadowEventRegion
static String SHADOW_OPERATIONS = "ShadowOperations";

// Counter for the serial execution number for each task
public static int EXECUTION_NUMBER;

// Counters for number of times test did certain operations
public static int NUM_CREATE;
public static int NUM_UPDATE;
public static int NUM_DESTROY;
public static int NUM_INVALIDATE;
public static int NUM_REGION_DESTROY;
public static int NUM_REGION_INVALIDATE;
public static int NUM_REGION_CREATE;
public static int NUM_LOCAL_DESTROY;
public static int NUM_LOCAL_INVALIDATE;
public static int NUM_LOCAL_REGION_DESTROY;
public static int NUM_LOCAL_REGION_INVALIDATE;
public static int NUM_CLOSE;
public static int NUM_CLEAR;

// As each VM participates in the RR, it increments this value
// the leader in the roundRobin always resets to 0
public static int numInRound;   
// Counters for per RR Round execution of region operations (operation execution count)
public static int EXPECTED_REMOTE_REGION_CREATE;
public static int EXPECTED_REMOTE_REGION_DEPARTED;
// Counters for RegionListener (event receipt count)
public static int actualRegionCreateEvents;
public static int actualRegionDepartedEvents;

// Counter for a thread to signal it did something
// lynn are these still used?
public static int REQUEST_REGION_UPDATE;
public static int REGION_UPDATE_COMPLETE;
public static int CONTINUE_MONITOR;

// Region event test statistics
// lynn are these still used?
public static int MAX_GET_CURR_REGION_NAMES_MILLIS;
public static int MAX_NUM_GET_CURR_REGION_NAMES;
public static int MAX_FAULT_IN_REGIONS_MILLIS;
public static int MAX_WRITE_CURR_REGION_NAMES_MILLIS;
public static int MAX_NUM_WRITE_CURR_REGION_NAMES;

// singleton instance of this Blackboard
public static EventBB bbInstance = null;

/**
 *  Get the EventBB
 */
public static EventBB getBB() {
   if (bbInstance == null) {
      synchronized ( EventBB.class ) {
         if (bbInstance == null) 
            bbInstance = new EventBB(EVENT_BB_NAME, EVENT_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Initialize the EventBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   aMap.put(SERIAL_EXECUTION, new Boolean(TestConfig.tab().booleanAt(hydra.Prms.serialExecution)));
   aMap.put(ROUND_ROBIN, new Boolean(TestConfig.tab().booleanAt(hydra.Prms.roundRobin, false)));
   clearRegionOpInfo();
   getBB().printSharedCounters();
}

public static boolean isSerialExecution() {
   return (((Boolean)(EventBB.getBB().getSharedMap().get(EventBB.SERIAL_EXECUTION))).booleanValue());
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public EventBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public EventBB(String name, String type) {
   super(name, type, EventBB.class);
}
   
  /**
   * Sets the value count with the given name to zero.
   */
public static void zero(String counterName, int counter) {
   EventBB.getBB().getSharedCounters().zero(counter);
   Log.getLogWriter().info(counterName + " has been zeroed");
}
   
  /**
   * Increments the counter with the given name
   */
public static long incrementCounter(String counterName, int counter) {
   long counterValue = EventBB.getBB().getSharedCounters().incrementAndRead(counter);
   Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
   return counterValue;
}
   
  /**
   * Adds a given amount to the value of a given counter
   */
public static long add(String counterName, int counter, int amountToAdd) {
   long counterValue = EventBB.getBB().getSharedCounters().add(counter, amountToAdd);
   Log.getLogWriter().info("After adding " + amountToAdd + ", " + counterName + " is " + counterValue);
   return counterValue;
}

/** 
 *  Utility method to determine if running in serial round robin execution mode
 *
 *  @see hydra.Prms.serialExecution
 *  @see hydra.Prms.roundRobin
 */
public static boolean isSerialRR() {
   boolean serialExecution  = (((Boolean)(EventBB.getBB().getSharedMap().get(EventBB.SERIAL_EXECUTION))).booleanValue());
   boolean serialRR = (((Boolean)(EventBB.getBB().getSharedMap().get(EventBB.ROUND_ROBIN))).booleanValue());
   return serialExecution && serialRR;
}

/** 
 *  Using the hydra threadGroupName (RRLeader), determines if this client is the
 *  first in a RoundRobin (hydra client scheduling prm).
 *
 *  @returns True if the current thread is the first one in a round, false
 *           otherwise.
 *
 *  @see hydra.Prms.roundRobin
 */
public static boolean isRRLeader() {
   Blackboard bb = EventBB.getBB();
   if (RemoteTestModule.getCurrentThread().getThreadGroupName().equals(EventBB.RR_LEADER)) {
      Log.getLogWriter().info("I am the leader of the serialRoundRobin");
      return true;
   }
   return false;
}

/**
 *  Put the region operation information (operation (create, destroy or invalidate) and
 *  target region) into the BB
 *
 *  @param operation - region operation (create, destroy, invalidate)
 *  @param region - target of the region operation
 *
 */
public static void putRegionOpInfo(int operation, String regionName) {
   Blackboard bb = EventBB.getBB();
   Log.getLogWriter().info("putRegionOpInfo to BB: operation = " + operation + " regionName = " + regionName);
   bb.getSharedMap().put(EventBB.REGION_OP, new Integer(operation));
   bb.getSharedMap().put(EventBB.TARGET_REGION, regionName);
}

/**
 *  Clear the region operation information from the BB
 */
public static void clearRegionOpInfo() {
   Blackboard bb = EventBB.getBB();
   Log.getLogWriter().info("clearRegionOpInfo()");
   bb.getSharedMap().put(EventBB.REGION_OP, new Integer(ProxyEventTest.NO_OPERATION));
}

/**
 *  Get the Region Operation from the BB (used in proxy serial region event tests)
 */
public static int getRegionOp() {
   Blackboard bb = EventBB.getBB();
   Integer operation = (Integer)(bb.getSharedMap().get(EventBB.REGION_OP));
   Log.getLogWriter().info("getRegionOp from EventBB returns " + operation);
   return operation.intValue();
}

/**
 *  Get the targeted region from the BB (used in proxy serial region event tests)
 */ 
public static String getTargetRegion() {
   Blackboard bb = EventBB.getBB();
   String regionName = (String)(bb.getSharedMap().get(EventBB.TARGET_REGION));
   Log.getLogWriter().info("getTargetRegion from EventBB returns " + regionName);
   return regionName;
}

/**
 *  Clear the remote region (actual) counters for serialRoundRobin tests
 *
 *  @see EventBB.EXPECTED_REGION_CREATE
 *  @see EventBB.NUM_REMOTE_REGION_DEPARTED
 */
public static void clearRemoteRegionCounters() {
   zero("EventBB.EXPECTED_REMOTE_REGION_CREATE", EventBB.EXPECTED_REMOTE_REGION_CREATE);
   zero("EventBB.EXPECTED_REMOTE_REGION_DEPARTED", EventBB.EXPECTED_REMOTE_REGION_DEPARTED);
   zero("EventBB.actualRegionCreateEvents", EventBB.actualRegionCreateEvents);
   zero("EventBB.actualRegionDepartedEvents", EventBB.actualRegionDepartedEvents);
}

}
