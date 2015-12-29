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
package rebalance;

import hydra.blackboard.Blackboard;

public class RebalanceBB extends Blackboard {

// Blackboard creation variables
static String BB_NAME = "Rebalance_Blackboard";
static String BB_TYPE = "RMI";

// SharedCounters
// track number of CacheServers (also used as part of cacheServerName)
public static int numCacheServers; 

// Maintain a count of the number of regions (multi-region tests)
// that have finished with redundancy recovery
public static int recoveryRegionCount;

// for parRegCreateDestroyWithCacheServer, protects regions during cacheServer startup
// to ensure regions exist (to rebalance)
public static int criticalSection;

// Coordinate HAResourceObserver with killTargetVm and cancelRebalance
public static int processedStart;
public static int numTargetEventsProcessed;

// capacity tests
public static int numExceededLMMAlerts;
public static int numDataStores;
public static String LocalMaxMemoryKey = "LocalMaxMemory_";
public static String LocalSizeKey = "LocalSize_";

// Single instance of the BB 
public static RebalanceBB bbInstance = null;

// BB SharedMap Keys
public static String targetVmInfo = "targetVmInfo";
public static String PartitionedRegionDetails_before = "PartitionedRegionDetails_before";
public static String ErrorKey = "error";

/**
 *  Get the BB
 */
public static RebalanceBB getBB() {
   if (bbInstance == null) {
      synchronized ( RebalanceBB.class ) {
         if (bbInstance == null) 
            bbInstance = new RebalanceBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public RebalanceBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public RebalanceBB(String name, String type) {
   super(name, type, RebalanceBB.class);
}
   
}
