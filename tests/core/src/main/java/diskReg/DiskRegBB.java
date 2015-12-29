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
package diskReg;

import hydra.*;
import hydra.blackboard.Blackboard;

public class DiskRegBB extends Blackboard {
   
// Blackboard variables
static String DISKREG_BB_NAME = "DiskReg_Blackboard";
static String DISKREG_BB_TYPE = "RMI";

// Key in the blackboard to keep the name of the disk region backup directory
// Note that the DiskRegBB uses this for a 'List' for diskDirs (a la RegionDefinition).
public static String BACKUP_DIR_LIST = "BackupDirList";

// shared map keys
public static String FIX_USE_CACHE_LOADER = "fixUseCacheLoader";
public static String USE_CACHE_LOADER = "useCacheLoader";
//public static String REGION_DEFINITION = "RegionDefinition";
public static String REGION_CONFIG_NAME = "RegionConfigKey";

// Blackboard counters
public static int EXECUTION_NUMBER;
public static int WHICH_DISK_REGION_ATTRS;

// Blackboard counter for task synchronization (recovery tests)
public static int cacheReady;
public static int completedOperations;

public static DiskRegBB bbInstance = null;

/**
 *  Initialize the DiskRegBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
   getBB().getSharedMap().put(FIX_USE_CACHE_LOADER, new Boolean(TestConfig.tab().booleanAt(DiskRegPrms.fixUseCacheLoader, false)));
   getBB().getSharedMap().put(USE_CACHE_LOADER, new Boolean(TestConfig.tab().booleanAt(DiskRegPrms.useCacheLoader)));
   getBB().getSharedMap().put(REGION_CONFIG_NAME, TestConfig.tab().stringAt(ConfigPrms.regionConfig));
}

public static boolean fixUseCacheLoader() {
   return (((Boolean)(DiskRegBB.getBB().getSharedMap().get(FIX_USE_CACHE_LOADER))).booleanValue());
}

public static boolean useCacheLoader() {
   return (((Boolean)(DiskRegBB.getBB().getSharedMap().get(USE_CACHE_LOADER))).booleanValue());
}

/**
 *  Get the DiskRegBB
 */
public static DiskRegBB getBB() {
   if (bbInstance == null) {
      synchronized ( DiskRegBB.class ) {
         if (bbInstance == null) 
            bbInstance = new DiskRegBB(DISKREG_BB_NAME, DISKREG_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public DiskRegBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public DiskRegBB(String name, String type) {
   super(name, type, DiskRegBB.class);
}
   
}
