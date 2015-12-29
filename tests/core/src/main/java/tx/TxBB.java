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
package tx;

import util.*;
import hydra.*;
import hydra.blackboard.*;
//import com.gemstone.gemfire.cache.*;
//import java.util.TreeSet;

public class TxBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "Tx_Blackboard";
static String BB_TYPE = "RMI";

private static TxBB bbInstance = null;

// Counter for the serial execution number for each task
public static int RoundRobinNumber;   // the current number of "rounds"
public static int ExecutionNumber;    // the current task number

// SharedCounters
public static int TX_NUMBER;         // tracks current tx from begin/complete
public static int PROCESS_EVENTS;    // controls CacheListener event processing

public static int TX_IN_PROGRESS;            // in begin() block
public static int TX_READY_FOR_VALIDATION;   // indicates TX has begun
public static int TX_COMPLETED;              // indicates TX completed

// shared counters to count commit success/failure
public static int TX_SUCCESS;
public static int TX_FAILURE;
public static int TX_ROLLBACK;
public static int NOT_IN_TRANS;

// shared counters to count expected number of failed commits and rollbacks
public static int NUM_EXPECTED_FAILED_COMMIT;
public static int NUM_EXPECTED_ROLLBACK;

// counter to divide VMs into using a unique key set
public static int UniqueKeyCounter;

public static int nextRepeatableRead;

// SharedMap keys 
public static String TxWriterAction = "TxWriterAction"; // commit/abort
public static String COMPLETION_ACTION = "CompletionAction";  // commit/rollback
public static String TXACTION_COMMIT = "commit";
public static String TXACTION_ROLLBACK = "rollback";
public static String TXACTION_ABORT = "abort";
public static String TXACTION_NONE = "none"; 
public static String RoundRobinStartThread = "RoundRobinStartThread";
public static String OpListKey = "OpList_";
public static String OpListKey2 = "OpList2_";
public static String CommitStatusKey = "CommitStatus_";
public static String DestroyedRegionsKey = "DestroyedRegions";
public static String FirstInRoundCommitStatus = "FirstInRoundCommitStatus";
public static String SecondInRoundCommitStatus = "SecondInRoundCommitStatus";
public static String TX_OPLIST_KEY = "txOpListKey";
public static String TX_VM_PID = "txVmPid";
public static String TX_VM_CLIENTNAME = "txVmClientName";
public static String NON_TX_OPLIST = "nonTxOpList";
public static String LocalListenerOpListPrefix = "LocalListenerOpList_";
public static String RemoteListenerOpListPrefix = "RemoteListenerOpList_";
public static String UpdateStrategy = "UpdateStrategy";
public static String CommitStateTrigger = "CommitStateTrigger";
public static String RegConfigForPIDBaseKey = "RegionsForPID_";
public static String RegConfigForPIDKey = RegConfigForPIDBaseKey + ProcessMgr.getProcessId();
public static String RepeatableRead = "RepeatableRead";
// Key for DataPolicies for View Tests
// Complete key has 'clientX' appended.
public static String DataPolicyPrefix = "DATAPOLICY_";

// SharedMap keys for PR TX (colocated) View tests
public static final String keySet = "keySet_";  // for thread specific keySetResult

// SharedMap key for killTarget (in client/server distIntegrity tests)
// ClientVmInfo (to use with dynamic stop)
public static final String delegate = "DelegateVmInfo";
public static final String nonDelegateServer = "NonDelegateServerVmInfo";

// SharedMap key for parRegIntegrityRemote
// Signal to waitForDist method to signal local killSyncObj to give up waiting (we have killCommittor threads on both non-tx VMs and only one will be killed by the test callbacks
public static final String timeToStop = "timeToStop";

// SharedMap key for ParRegViewTests (to know if the TxListener.afterCommit() was invoked in any VM
public static final String afterCommitProcessed = "afterCommitProcessed";

public static final String VMOTION_TRIGGERED = "vMotionTriggerred";
public static final String VMOTION_TRIGGERED_TIME = "vMotionTriggerredTime";

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

// Counter for TxEvent seen only in TX VM (when create/destroy conflated to destroy)
public static int CONFLATED_CREATE_DESTROY;
// Counter for TxEvent seen only in TX VM (CommitConflict/FailedCommit)
// seen when an entry-create is followed by a region-destroy on the host region
public static int CREATE_IN_DESTROYED_REGION;
public static int CREATE_IN_DESTROYED_REGION_ISLOAD;

/** Get the update strategy.
 */
public static String getUpdateStrategy() {
   String strategy = (String)(TxBB.getBB().getSharedMap().get(UpdateStrategy));
   if (strategy == null) {
      strategy = TxPrms.getUpdateStrategy();
      TxBB.getBB().getSharedMap().put(UpdateStrategy, strategy);
      Log.getLogWriter().info("Update strategy is " + strategy);
   }
   return strategy;
}

/** Convenience method for writing a list of operations for the current 
 *  client thread.
 */
public static void putOpList(OpList opList) {
   Object key = OpListKey + Thread.currentThread().getName();
   TxBB.getBB().getSharedMap().put(key, opList);
   Log.getLogWriter().info("TxBB put into shared map key " + key + ", value " + opList); 
}

/** Convenience method for reading the list of operations for the current 
 *  client thread.
 */
public static OpList getOpList() {
   Object key = OpListKey + Thread.currentThread().getName();
   OpList opList = (OpList)(TxBB.getBB().getSharedMap().get(key));
   Log.getLogWriter().info("TxBB read from shared map key " + key + ", value " + opList); 
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

/** Write to the blackboard the expected commit success/failure
 *  for the current thread.
 *
 *  @param expectCommitStatus True if we expect the current thread
 *         to successfully commit, false otherwise.
 */
public static void recordCommitStatus(boolean expectCommitStatus) {
   Object key = TxBB.CommitStatusKey + Thread.currentThread().getName();
   TxBB.getBB().getSharedMap().put(key, new Boolean(expectCommitStatus));
   Log.getLogWriter().info("Written to TxBB: " + key + ", " + expectCommitStatus);
}

/** Return the expected commit success/failure for the current thread.
 *
 *  @returns true if the current thread should succeed in its commit,
 *           false otherwise.
 */
public static boolean getCommitStatus() {
   Object key = TxBB.CommitStatusKey + Thread.currentThread().getName();
   Object result = TxBB.getBB().getSharedMap().get(key);
   if (result instanceof Boolean)
      return ((Boolean)result).booleanValue();
   else
      throw new TestException("Unknown value " + result + " for TxBB sharedMap key " + key);
}

/** Increment the given counter */
public static void inc(int whichCounter) {
   TxBB.getBB().getSharedCounters().increment(whichCounter);
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

   sc.zero(CONFLATED_CREATE_DESTROY);
   sc.zero(CREATE_IN_DESTROYED_REGION);
   sc.zero(CREATE_IN_DESTROYED_REGION_ISLOAD);

   sc.zero(REMOTE_CREATE);
   sc.zero(REMOTE_CREATE_ISLOAD);
   sc.zero(REMOTE_UPDATE);
   sc.zero(REMOTE_UPDATE_ISLOAD);
   sc.zero(REMOTE_DESTROY);
   sc.zero(REMOTE_INVALIDATE);
   sc.zero(REMOTE_LOCAL_DESTROY);
   sc.zero(REMOTE_LOCAL_INVALIDATE);

   // TransactionListener callback counters
   sc.zero(TX_SUCCESS);
   sc.zero(TX_FAILURE);
   sc.zero(TX_ROLLBACK);
   sc.zero(NUM_EXPECTED_FAILED_COMMIT);
   sc.zero(NUM_EXPECTED_ROLLBACK);
}

/**
 *  Get the instance of TxBB
 */
public static TxBB getBB() {
   if (bbInstance == null) {
      synchronized ( TxBB.class ) {
         if (bbInstance == null) 
            bbInstance = new TxBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public TxBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public TxBB(String name, String type) {
   super(name, type, TxBB.class);
}
   
}
