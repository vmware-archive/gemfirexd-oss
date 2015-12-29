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
package hct;

import java.util.*;
import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task
 * threads in an {@link BridgeNotify} do.  For instance, it has counters
 * that are incremented when region operations like <code>put</code>
 * and <code>invalidate</code> are performed.  
 *
 * @author Lynn Gallinat & Lynn Hughes-Godfrey
 * @since 5.1
 */
public class BridgeNotifyBB extends Blackboard {
   
// Blackboard creation variables
public static String BRIDGE_BB_NAME = "Bridge_Blackboard";
public static String BRIDGE_BB_TYPE = "RMI";

// singleton instance of blackboard
private static BridgeNotifyBB bbInstance = null;

  /** Key in the blackboard sharedMap for the hydra parameter {@link
   * hydra.Prms#serialExecution} */
public static String SERIAL_EXECUTION = "SerialExecution";
public static String RECEIVE_VALUES_AS_INVALIDATES = "receiveValuesAsInvalidates";
public static String SINGLE_KEYS_REGISTERED = "singleKeysRegistered";
public static String numListeners = "numListeners";

// Counter for the serial execution number for each task
public static int EXECUTION_NUMBER;

// Counters for number of times test did certain operations
public static int NUM_CREATE;
public static int NUM_UPDATE;
public static int NUM_PUTALL_CREATE;
public static int NUM_PUTALL_UPDATE;
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

//added counters to track putAll create and putAll update
public static int EXPECTED_PUTALL_CREATE;
public static int EXPECTED_PUTALL_UPDATE;
public static int EXPECTED_CREATE;
public static int EXPECTED_UPDATE;

// Counter (to keep track of the VMs with listeners 
public static int NUM_LISTENERS;

/**
 *  Get the BridgeNotifyBB
 */
public static BridgeNotifyBB getBB() {
   if (bbInstance == null) {
      synchronized ( BridgeNotifyBB.class ) {
         if (bbInstance == null) 
            bbInstance = new BridgeNotifyBB(BRIDGE_BB_NAME, BRIDGE_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Initialize the BridgeNotifyBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   aMap.put(SERIAL_EXECUTION, new Boolean(TestConfig.tab().booleanAt(hydra.Prms.serialExecution)));
   aMap.put(SINGLE_KEYS_REGISTERED, new ArrayList());
   getBB().printSharedCounters();
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public BridgeNotifyBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public BridgeNotifyBB(String name, String type) {
   super(name, type, BridgeNotifyBB.class);
}
   
  /**
   * Sets the value count with the given name to zero.
   */
public static void zero(String counterName, int counter) {
   BridgeNotifyBB.getBB().getSharedCounters().zero(counter);
   Log.getLogWriter().info(counterName + " has been zeroed");
}
   
  /**
   * Increments the counter with the given name
   */
public static long incrementCounter(String counterName, int counter) {
   long counterValue = BridgeNotifyBB.getBB().getSharedCounters().incrementAndRead(counter);
   Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
   return counterValue;
}
   
  /**
   * Adds a given amount to the value of a given counter
   */
public static long add(String counterName, int counter, int amountToAdd) {
   long counterValue = BridgeNotifyBB.getBB().getSharedCounters().add(counter, amountToAdd);
   Log.getLogWriter().info("After adding " + amountToAdd + ", " + counterName + " is " + counterValue);
   return counterValue;
}

}
