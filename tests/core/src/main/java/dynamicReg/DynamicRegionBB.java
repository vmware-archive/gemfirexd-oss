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
package dynamicReg;

import hydra.*;
import hydra.blackboard.*;
import tx.OpList;

public class DynamicRegionBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "DynamicRegion_Blackboard";
static String BB_TYPE = "RMI";

public static DynamicRegionBB bbInstance = null;

// Counter for the serial execution number for each task
public static int RoundRobinNumber;   // the current number of "rounds"
public static int ExecutionNumber;    // the current task number

// SharedCounters
public static int PROCESS_EVENTS;    // controls CacheListener event processing

// counter to divide VMs into using a unique key set
public static int UniqueKeyCounter;

// SharedMap keys 
public static String RoundRobinStartThread = "RoundRobinStartThread";
public static String OpListKey = "OpList_";
public static String DestroyedRegionsKey = "DestroyedRegions";
public static String VM_PID = "VmPid";
public static String UpdateStrategy = "UpdateStrategy";
public static String RegDefForPIDKey = "RegionDefForPID_" + ProcessMgr.getProcessId();

// Counters for events expected in local cacheListener (only)
public static int LOCAL_CREATE;
public static int LOCAL_CREATE_ISLOAD;
public static int LOCAL_UPDATE;
public static int LOCAL_UPDATE_ISLOAD;
public static int LOCAL_DESTROY;
public static int LOCAL_INVALIDATE;
public static int LOCAL_LOCAL_DESTROY;
public static int LOCAL_LOCAL_INVALIDATE;

// Counters for events expected in remote cache & tx listeners (collapsed)
public static int REMOTE_CREATE;
public static int REMOTE_CREATE_ISLOAD;
public static int REMOTE_UPDATE;
public static int REMOTE_UPDATE_ISLOAD;
public static int REMOTE_DESTROY;
public static int REMOTE_INVALIDATE;
public static int REMOTE_LOCAL_DESTROY;
public static int REMOTE_LOCAL_INVALIDATE;


/** Convenience method for writing a list of operations
 */
public static void putSingleOpList(OpList opList) {
   Object key = OpListKey;
   DynamicRegionBB.getBB().getSharedMap().put(key, opList);
   Log.getLogWriter().info("DynamicRegionBB put into shared map key " + key + ", value " + opList); 
}

/** Convenience method for reading the list of operations 
 */
public static OpList getSingleOpList() {
   Object key = OpListKey;
   OpList opList = (OpList)(DynamicRegionBB.getBB().getSharedMap().get(key));
   Log.getLogWriter().info("DynamicRegionBB read from shared map key " + key + ", value " + opList); 
   return opList;
}

/** Convenience method for writing a list of operations for the current 
 *  client thread.
 */
public static void putOpList(OpList opList) {
   Object key = OpListKey + Thread.currentThread().getName();
   DynamicRegionBB.getBB().getSharedMap().put(key, opList);
   Log.getLogWriter().info("DynamicRegionBB put into shared map key " + key + ", value " + opList); 
}

/** Convenience method for reading the list of operations for the current 
 *  client thread.
 */
public static OpList getOpList() {
   Object key = OpListKey + Thread.currentThread().getName();
   OpList opList = (OpList)(DynamicRegionBB.getBB().getSharedMap().get(key));
   Log.getLogWriter().info("DynamicRegionBB read from shared map key " + key + ", value " + opList); 
   return opList;
}

/**
 *  Convenience method for obtaining the key created for the opList
 *  serial tests may put this key on the BB for other threads to use
 *  for opList access
 */
public static String getOpListKey() {
   return OpListKey + Thread.currentThread().getName();
}


/** Increment the given counter */
public static void inc(int whichCounter) {
   DynamicRegionBB.getBB().getSharedCounters().increment(whichCounter);
}

/** Clear the event counters (associated with number of operations performed */
public void zeroEventCounters() {
   SharedCounters sc = getSharedCounters();
   sc.zero(LOCAL_CREATE);
   sc.zero(LOCAL_CREATE_ISLOAD);
   sc.zero(LOCAL_UPDATE);
   sc.zero(LOCAL_UPDATE_ISLOAD);
   sc.zero(LOCAL_DESTROY);
   sc.zero(LOCAL_INVALIDATE);
   sc.zero(LOCAL_LOCAL_DESTROY);
   sc.zero(LOCAL_LOCAL_INVALIDATE);

   sc.zero(REMOTE_CREATE);
   sc.zero(REMOTE_CREATE_ISLOAD);
   sc.zero(REMOTE_UPDATE);
   sc.zero(REMOTE_UPDATE_ISLOAD);
   sc.zero(REMOTE_DESTROY);
   sc.zero(REMOTE_INVALIDATE);
   sc.zero(REMOTE_LOCAL_DESTROY);
   sc.zero(REMOTE_LOCAL_INVALIDATE);
}

/**
 *  Get the instance of TxBB
 */
public static DynamicRegionBB getBB() {
   if (bbInstance == null) {
      synchronized ( DynamicRegionBB.class ) { 
         if (bbInstance == null) 
            bbInstance = new DynamicRegionBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public DynamicRegionBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public DynamicRegionBB(String name, String type) {
   super(name, type, DynamicRegionBB.class);
}
   
}
