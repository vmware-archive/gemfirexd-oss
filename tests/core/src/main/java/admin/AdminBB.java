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
package admin;

//import hydra.*;
import java.util.*;
import hydra.blackboard.Blackboard;
//import hydra.blackboard.SharedCounters;
//import hydra.blackboard.SharedMap;

/**
 *
 * Manages the blackboard for AdminTests in this package
 *
 */

public class AdminBB extends Blackboard {
   
public static AdminBB blackboard;

// Blackboard creation variables
static String ADMIN_BB_NAME = "Admin_Blackboard";
static String ADMIN_BB_TYPE = "RMI";

// Blackboard counters
public static int recycleRequests;
public static int joinedNotifications;
public static int departedNotifications;
public static int maxCacheServers;
public static int runningCacheServers;
public static int stopCacheServerRequests;
public static int startCacheServerRequests;
public static int roleGainEvents;
public static int roleLossEvents;

// Blackboard shared map keys
public static String activeMembers = "activeMembers";
// There will be dynamic keys named <rootRegion>_RegionCount

/**
 *  Initialize the AdminBB with the sharedMap of active members
 *  The SharedMap contains an ArrayList of active members (DistributedMember entries)
 */
public static void HydraTask_initialize() {
  AdminBB.getInstance().getSharedMap().put(AdminBB.activeMembers, new ArrayList());
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public AdminBB() {
}

public AdminBB( String name, String type ) {
  super( name, type, AdminBB.class );
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public static synchronized AdminBB getInstance() {
   if ( blackboard == null) 
     synchronized( AdminBB.class ) {
       if (blackboard == null)
         blackboard = new AdminBB( ADMIN_BB_NAME, ADMIN_BB_TYPE);
     }
   return blackboard;
}
   
}
