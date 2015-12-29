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
package asyncMsg;

import hydra.*;
import hydra.blackboard.*;
import tx.OpList;
import util.TestException;

public class AsyncMsgBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "AsyncMsg_Blackboard";
static String BB_TYPE = "RMI";

private static AsyncMsgBB bbInstance = null;

// Counter for the serial execution number for each task
public static int RoundRobinNumber;   // the current number of "rounds"
public static int ExecutionNumber;    // the current task number

public static int OPLIST_READY_FOR_VALIDATION;   // indicates putter done
public static String OpListKey = "OpList_";

public static String PrimeQDir = "PrimeQDir_";
public static String PrimeQCount = "PrimeQCount_";
public static String DestroyedRegionsKey = "DestroyedRegions";
public static String RegDefForPIDKey = "RegionDefForPID_" + ProcessMgr.getProcessId();

public static String conflationEnabled = "conflationEnabled";

public static final String ErrorKey = "Error";
public static final String SlowReceiverAlertKey = "SlowReceiverKey";
public static final String SlowReceiverDetectedKey = "SlowReceiverDetectedKey";

// sharedCounters: used by concurrent tests; concurrent threads run in a concurrent
//                 "round"; all threads begin, pause, verify, end, then start again
public static int ConcurrentLeader; // used to choose one thread to do once-per-round duties
public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
public static int Pausing;          // used to sync all threads when pausing for verification
public static int FinishedVerify;   // used to sync all threads when verification is done
public static int SnapshotWritten;  // used to sync threads when one thread has written its state
public static int NoMoreEvents;     // used to sync threads when no more events are being invoked

// sharedMap keys used by concurrent tests
public static String RegionSnapshot = "RegionSnapshot";

/** Convenience method for writing a list of operations for the current
 *  client thread.
 */
public static void putOpList(OpList opList) {
   Object key = OpListKey + Thread.currentThread().getName();
   AsyncMsgBB.getBB().getSharedMap().put(key, opList);
   Log.getLogWriter().info("AsyncBB put into shared map key " + key + ", value " + opList);
}
                                                                                
/** Convenience method for reading the list of operations for the current
 *  client thread.
 */
public static OpList getOpList() {
   Object key = OpListKey + Thread.currentThread().getName();
   OpList opList = (OpList)(AsyncMsgBB.getBB().getSharedMap().get(key));
   Log.getLogWriter().info("AsyncBB read from shared map key " + key + ", value " + opList);
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
                                                                                
/**
 *  Get the instance of AsyncMsgBB
 */
public static Blackboard getBB() {
   if (bbInstance == null) {
      synchronized( AsyncMsgBB.class ) {
         if ( bbInstance == null )  
            bbInstance = new AsyncMsgBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/** Check if an error has been recorded in the blackboard.
 */
public static void checkForError() {
   Object error = AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.ErrorKey);
   if (error != null) {
      throw new TestException(error.toString());
   }
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public AsyncMsgBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public AsyncMsgBB(String name, String type) {
   super(name, type, AsyncMsgBB.class);
}
   
}
