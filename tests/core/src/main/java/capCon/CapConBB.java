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
package capCon;

//import util.*;
import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;
import java.util.TreeSet;

public class CapConBB extends Blackboard {
   
// Blackboard variables
static String CAPCON_BB_NAME = "CapCon_Blackboard";
static String CAPCON_BB_TYPE = "RMI";

// Blackboard shared map keys
static String TEST_SETTINGS = "TestSettings";

// Blackboard shared map keys; used for eviction tests
static String OUT_OF_ORDER_SET = "OutOfOrderSet";   
static String FIX_USE_CACHE_LOADER = "fixUseCacheLoader";
static String USE_CACHE_LOADER = "useCacheLoader";

// Blackboard shared map keys; used for dynamic tests
static String CURRENT_POINT = "CurrentPoint";   
static String RANDOM_CAPACITY_CHANGES = "RandomCapacityChanges";   

// Blackboard counters
public static int MIN_NUM_KEYS_AT_LRU_EVICTION;
public static int MIN_NUM_KEYS_AT_MEMLRU_EVICTION;
public static int MIN_REGION_BYTES_AT_MEMLRU_EVICTION;
public static int MAX_NUM_KEYS;
public static int MAX_REGION_SIZE_IN_BYTES;
public static int EXECUTION_NUMBER;
public static int SyncCounter;

// Serial test counters
public static int LAST_TERMINATOR_CHECK;

// Evict test counters
public static int HIGHEST_SEQ_NAME_ID;
public static int MAX_LRU_DIFF;

// Dynamic test counters
public static int NUM_CAPACITY_CHANGES;
public static int NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE;

private static CapConBB bbInstance = null;

/**
 *  Get the CapConBB
 */
public static CapConBB getBB() {
   if (bbInstance == null) {
      synchronized ( CapConBB.class ) {
         if (bbInstance == null) 
            bbInstance = new CapConBB(CAPCON_BB_NAME, CAPCON_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Initialize the CapConBB
 */
public static void HydraTask_initialize() {
   getBB().initialize();
}

/**
 *  Initialize the bb
 *  This saves hydra params in the blackboard that must only be
 *  read once per test run.
 */
public void initialize() {
   hydra.blackboard.SharedMap aMap = this.getSharedMap();
   aMap.put(OUT_OF_ORDER_SET, new TreeSet());
   try {
      boolean abool = TestConfig.tab().booleanAt(CapConPrms.randomCapacityChanges);
      aMap.put(RANDOM_CAPACITY_CHANGES, new Boolean(abool));
   } catch (HydraConfigException e) {
      // don't need to specify this param in all tests
   }
   getBB().getSharedMap().put(FIX_USE_CACHE_LOADER, new Boolean(TestConfig.tab().booleanAt(CapConPrms.fixUseCacheLoader, false)));
   getBB().getSharedMap().put(USE_CACHE_LOADER, new Boolean(TestConfig.tab().booleanAt(CapConPrms.useCacheLoader)));
}

public static boolean fixUseCacheLoader() {
   return (((Boolean)(CapConBB.getBB().getSharedMap().get(FIX_USE_CACHE_LOADER))).booleanValue());
}

public static boolean useCacheLoader() {
   return (((Boolean)(CapConBB.getBB().getSharedMap().get(USE_CACHE_LOADER))).booleanValue());
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public CapConBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public CapConBB(String name, String type) {
   super(name, type, CapConBB.class);
}
   
}
