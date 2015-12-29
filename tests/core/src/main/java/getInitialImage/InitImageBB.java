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
package getInitialImage;

//import util.*;
//import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;
//import java.util.TreeSet;

public class InitImageBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "InitImage_Blackboard";
static String BB_TYPE = "RMI";

// Blackboard shared map keys
public static String KEY_INTERVALS = "KeyIntervals";
public static String DATAPOLICY1_ATTR = "DataPolicyAttr1";
public static String DATAPOLICY2_ATTR = "DataPolicyAttr2";

public static final String BACKUP_DIR_LIST = "BackupDirList";

// RegionTest related shared map keys
public static final String REGION_INVALIDATE_LIST = "RegionInvalidateList";
public static final String REGION_DESTROY_LIST    = "RegionDestroyList";
public static final String REGION_TO_DESTROY      = "RegionToDestroy";

// Blackboard counters for key intervals and tracking number of keys
public static int NUM_NEW_KEYS_CREATED;
public static int NUM_ORIGINAL_KEYS_CREATED;
public static int SHOULD_ADD_COUNT;
public static int NUM_GETS;       
public static int LASTKEY_INVALIDATE;
public static int LASTKEY_LOCAL_INVALIDATE;
public static int LASTKEY_DESTROY;
public static int LASTKEY_LOCAL_DESTROY;
public static int LASTKEY_UPDATE_EXISTING_KEY;
public static int LASTKEY_GET;
public static int LAST_QUERY;

// Blackboard counters for RegionTests
public static int NUM_REGIONS_CREATED;
public static int NUM_REGIONS_DESTROYED;
public static int startGiiMonitoring;
public static int getInitialImageInProgress;
public static int getInitialImageComplete;
public static int cacheClosedSignal;

// Blackboard counters for number of operations completed while gii is in progress
public static int GII_IN_PROGRESS;
public static int INVALIDATE_COMPLETED;
public static int LOCAL_INVALIDATE_COMPLETED;
public static int DESTROY_COMPLETED;
public static int LOCAL_DESTROY_COMPLETED;
public static int NEW_KEY_COMPLETED;
public static int UPDATE_EXISTING_KEY_COMPLETED;
public static int GET_COMPLETED;

// Blackboard counters for max time spent for an operation
public static int MAX_TIME_INVALIDATE;
public static int MAX_TIME_LOCAL_INVALIDATE;
public static int MAX_TIME_DESTROY;
public static int MAX_TIME_LOCAL_DESTROY;
public static int MAX_TIME_NEW_KEY;
public static int MAX_TIME_UPDATE_EXISTING_KEY;
public static int MAX_TIME_GET;

// Blackboard counters for cache loader test
public static int NEW_KEY_GETS_RETURNED_NULL;
public static int NEW_KEY_GETS_RETURNED_LOADER_VALUE;

// Blackboard counters for cache writer test
public static int CACHE_WRITER_INVOKED_DURING_GII;
public static int CACHE_WRITER_INVOKED_NO_GII;

public static InitImageBB bbInstance = null;

/**
 *  Get the BB instance
 */
public static InitImageBB getBB() {
   if (bbInstance == null) {
      synchronized ( InitImageBB.class ) {
         if (bbInstance == null) 
            bbInstance = new InitImageBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public InitImageBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public InitImageBB(String name, String type) {
   super(name, type, InitImageBB.class);
}
   
}
