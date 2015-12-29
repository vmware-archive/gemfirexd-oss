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
package mirror;

//import util.*;
import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;

public class MirrorBB extends Blackboard {
   
// Blackboard variables
static String MIRROR_BB_NAME = "Mirror_Blackboard";
static String MIRROR_BB_TYPE = "RMI";

// singleton instance of blackboard
private static MirrorBB bbInstance = null;

// Key in the blackboard sharedMap for the hydra parameter hydra.Prms-serialExecution
static String SERIAL_EXECUTION = "SerialExecution";

// Counter for the serial execution number for each task
public static int EXECUTION_NUMBER;

// The number of the next event listener to use 
public static int NEXT_END_TASK_LISTENER;

// Event counters

// afterCreate events
   public static int numAfterCreateEvents_isDist;
   public static int numAfterCreateEvents_isNotDist;
   public static int numAfterCreateEvents_isExp;
   public static int numAfterCreateEvents_isNotExp;
   public static int numAfterCreateEvents_isRemote;
   public static int numAfterCreateEvents_isNotRemote;
// afterDestroy events
   public static int numAfterDestroyEvents_isDist;
   public static int numAfterDestroyEvents_isNotDist;
   public static int numAfterDestroyEvents_isExp;
   public static int numAfterDestroyEvents_isNotExp;
   public static int numAfterDestroyEvents_isRemote;
   public static int numAfterDestroyEvents_isNotRemote;
// afterInvalidate events
   public static int numAfterInvalidateEvents_isNotDist;
   public static int numAfterInvalidateEvents_isNotExp;
   public static int numAfterInvalidateEvents_isRemote;
   public static int numAfterInvalidateEvents_isNotRemote;
   
// afterUpdate events
public static int numAfterUpdateEvents_isDist;
public static int numAfterUpdateEvents_isNotDist;
public static int numAfterUpdateEvents_isExp;
public static int numAfterUpdateEvents_isNotExp;
public static int numAfterUpdateEvents_isRemote;
public static int numAfterUpdateEvents_isNotRemote;

// afterRegionDestroy events
public static int numAfterRegionDestroyEvents_isDist;
public static int numAfterRegionDestroyEvents_isNotDist;
public static int numAfterRegionDestroyEvents_isExp;
public static int numAfterRegionDestroyEvents_isNotExp;
public static int numAfterRegionDestroyEvents_isRemote;
public static int numAfterRegionDestroyEvents_isNotRemote;

// afterRegionInvalidate events
public static int numAfterRegionInvalidateEvents_isDist;
public static int numAfterRegionInvalidateEvents_isNotDist;
public static int numAfterRegionInvalidateEvents_isExp;
public static int numAfterRegionInvalidateEvents_isNotExp;
public static int numAfterRegionInvalidateEvents_isRemote;
public static int numAfterRegionInvalidateEvents_isNotRemote;

   
/**
 *  Get the MirrorBB
 */
public static MirrorBB getMirrorBB() {
   if (bbInstance == null) {
      synchronized ( MirrorBB.class ) {
         if (bbInstance == null)
            bbInstance = new MirrorBB(MIRROR_BB_NAME, MIRROR_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Initialize the MirrorBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
   getMirrorBB().getSharedMap().put(SERIAL_EXECUTION, new Boolean(TestConfig.tab().booleanAt(hydra.Prms.serialExecution)));
}

public static boolean isSerialExecution() {
   return (((Boolean)(MirrorBB.getMirrorBB().getSharedMap().get(MirrorBB.SERIAL_EXECUTION))).booleanValue());
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public MirrorBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public MirrorBB(String name, String type) {
   super(name, type, MirrorBB.class);
}
   
}
