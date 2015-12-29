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
package roles;

import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps counters for role loss/gain, 
 * actualQueuedEvents received (those with events.isQueued() == true)
 *
 * @author lhughes
 * @since 5.0
 */
public class RolesBB extends Blackboard {
   
// Blackboard creation variables
static String ROLES_BB_NAME = "Roles_Blackboard";
static String ROLES_BB_TYPE = "RMI";

// singleton instance of blackboard
private static RolesBB bbInstance = null;

/** Key in the blackboard sharedMap for the hydra parameter 
 * {@link hydra.Prms#serialExecution} 
 */
static String SERIAL_EXECUTION = "SerialExecution";

/**
 *  Key in the blackboard sharedMap for the current availability of the
 *  region for this client (full sharedMap key = RegionAvailable_clientX).
 *  This is updated by the RegionListener on RoleLoss/RoleGain events.
 */
public static String RegionAvailablePrefix = "RegionAvailable_";

// Counters for number of times test did certain operations
public static int expectedRoleLossEvents;
public static int actualRoleLossEvents;

public static int expectedRoleGainEvents;
public static int actualRoleGainEvents;

public static int expectedQueuedEvents;
public static int actualQueuedEvents;

public static int regionAccessExceptions;
public static int regionDistributionExceptions;

/**
 *  Get the RolesBB
 */
public static RolesBB getBB() {
   if (bbInstance == null) {
      synchronized ( RolesBB.class ) {
         if (bbInstance == null) 
            bbInstance = new RolesBB(ROLES_BB_NAME, ROLES_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Initialize the RolesBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   aMap.put(SERIAL_EXECUTION, new Boolean(TestConfig.tab().booleanAt(hydra.Prms.serialExecution)));
   getBB().printSharedCounters();
}

public static boolean isSerialExecution() {
   return (((Boolean)(RolesBB.getBB().getSharedMap().get(RolesBB.SERIAL_EXECUTION))).booleanValue());
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public RolesBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public RolesBB(String name, String type) {
   super(name, type, RolesBB.class);
}

}
