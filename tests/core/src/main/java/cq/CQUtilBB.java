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
package cq;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import pdx.PdxTest;
import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard for use with cq.CQUtil.
 *
 * @author lhughes
 * @since 5.1
 */
public class CQUtilBB extends Blackboard {

// Blackboard creation variables
static String CQUTIL_BB_NAME = "CQUtil_Blackboard";
static String CQUTIL_BB_TYPE = "RMI";

// singleton instance of this BB
private static CQUtilBB blackboard = new CQUtilBB( CQUTIL_BB_NAME, CQUTIL_BB_TYPE );  

// Counters for number of times CQListener processed various events
public static int NUM_CREATE;
public static int NUM_UPDATE;
public static int NUM_DESTROY;
public static int NUM_INVALIDATE;
public static int NUM_REGION_DESTROY;
public static int NUM_REGION_INVALIDATE;
public static int NUM_REGION_CLEAR;
public static int NUM_REGION_CREATE;
public static int NUM_CLOSE;
public static int NUM_CLEARREGION;
public static int NUM_INVALIDATEREGION;

public static int NUM_ERRORS;

// Counters for cq.CQSequentialValuesListener
public static int MISSING_UPDATES;
public static int LATE_UPDATES;

// Counters for cq.hct.BridgeEventListener
public static int NUM_LOCAL_LOAD_CREATE;
public static int NUM_EVICT_DESTROY;

// Counters for cq/ha listeners (counters to track expected number of various CQ Events
public static int expectedCQCreates;
public static int expectedCQDestroys;
public static int expectedCQUpdates;

// Count of clients that have registeredInterest
public static int NUM_RI_VMS;

// Count of clients that have registered as having CQs 
public static int NUM_CQS;

// sharedCounters: used by serial round robin tests
public static int ExecutionNumber;  // the serial execution number
public static int RoundPosition;    // the position (order) of a thread in a round
public static int RoundNumber;      // the number of rounds that have been executed

// sharedCounters: used by concurrent tests; concurrent threads run in a concurrent 
//                 "round"; all threads begin, pause, verify, end, then start again 
public static int ConcurrentLeader; // used to choose one thread to do once-per-round duties
public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
public static int Pausing;          // used to sync all threads when pausing for verification
public static int FinishedVerify;   // used to sync all threads when verification is done
public static int SnapshotWritten;  // used to sync threads when one thread has written its state
public static int ExceptionCounter; // used to sync threads when another thread has closed the cache
public static int Reinitialized;    // used to sync threads when a region has been recreated after a cache close
public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException
public static int SyncUp;           // used to coordinate HAController and HADoEntryOps invocations 

public static int QueryNumber;      // used to uniquely name a query

// sharedCounters: used by concCQ for entry operation counts
public static int EntryPerformed;
public static int Verified;
public static int NumCounterToDestroy;

// sharedMap: used by serial tests
public static final String RegionSnapshot = "RegionSnapshot";
public static final String DestroyedKeys  = "DestroyedKeys";

// sharedMap: used for conc verifying
public static final String VerifyFlag = "VerifyFlag";
public static final String EntryDone = "EntryDone";
public static final String VerifyDone = "VerifyDone";

//sharedMap: used for conc region ops
public static final String PerformedDestroyRegion = "PerformedDestroyRegion";
public static final String PerformedInvalidateRegion = "PerformedInvalidateRegion";

//sharedMap: used for determine region scope
public static final String Scope = "Scope";

// sharedMap: used for known keys tests
public static String KeyIntervals = "KeyIntervals";

// SharedMap key for failed tx ops (due to TransactionDataNodeHasDeparted, TransactionDataRebalancedExceptions)
public static String FAILED_TXOPS = "FailedTxOps";
// SharedMap key for failed tx ops (due to TransactionInDoubt)
public static String INDOUBT_TXOPS = "InDoubtTxOps";

// sharedCounters: used for known keys tests
public static int LASTKEY_INVALIDATE;
public static int LASTKEY_LOCAL_INVALIDATE;
public static int LASTKEY_DESTROY;
public static int LASTKEY_LOCAL_DESTROY;
public static int LASTKEY_UPDATE_EXISTING_KEY;
public static int LASTKEY_GET;
public static int SHOULD_ADD_COUNT;
public static int NUM_NEW_KEYS_CREATED;
public static int NUM_ORIGINAL_KEYS_CREATED;

// used to check for cq event silence
public static int lastEventTime;

/**
 *  Creates a singleton instance of the BB
 */
public static CQUtilBB getBB() {
    if ( blackboard == null )
      synchronized( CQUtilBB.class ) {
        if ( blackboard == null )
          blackboard = new CQUtilBB( CQUTIL_BB_NAME, CQUTIL_BB_TYPE );
      }
    return blackboard;
}
   
/**
 *  Display the CQUtilBB Counters
 */
public static void printBB() {
   if (blackboard != null) {
      blackboard.printSharedCounters();
   }
}

/** Put a region snapshot into the blackboard. If the values in the snapshot
 *  are PdxSerializables they cannot be put to the blackboard since hydra
 *  MasterController does not have them on the classPath. In this case 
 *  make an alternate map with values being Maps of field/field values.
 *  Note: either all values are PdxSerializables or all values are not.
 * @param snapshot The snapshot to write to the blackboard; this might contain
 *        PdxSerializables. 
 */
public static void putSnapshot(Map snapshot) {
  CQTestVersionHelper.putSnapshot(snapshot);
}

/** Get a region snapshot from the blackboard. If the values in the snapshot
 *  are Maps, then they represent PdxSerializables and should be converted
 *  to such before returning. 
 * @returns A region snapshot map. 
 */
public static Map getSnapshot() {
  Map snapshot = (Map)(CQUtilBB.getBB().getSharedMap().get(CQUtilBB.RegionSnapshot));
  for (Object key: snapshot.keySet()) {
    Object value = snapshot.get(key);
    if (value instanceof Map) { // this is a field map created by putSnapshot()
      Map fieldMap = (Map)value;
      Object restoredObject = PdxTest.restoreFromFieldMap(fieldMap);
      snapshot.put(key, restoredObject);
    }
  }
  return snapshot;
}

/** Add the key to the failedTxOps or inDoubtTxOps Sets in the shared map
 *
 */
public synchronized void addFailedOp(Object sharedMapKey, Object keyToAdd) {
   Log.getLogWriter().info("Adding " + keyToAdd + " to " + sharedMapKey);

   // coordinate access among VMs
   blackboard.getSharedLock().lock();

   Set sharedMapKeys = (Set)blackboard.getSharedMap().getMap().keySet();
   if (!sharedMapKeys.contains(sharedMapKey)) {
      blackboard.getSharedMap().put(sharedMapKey, new HashSet());
   }
   Set ops = (Set)blackboard.getSharedMap().get(sharedMapKey);
   ops.add(keyToAdd);
   blackboard.getSharedMap().put(sharedMapKey, ops);
   blackboard.getSharedLock().unlock();

   Log.getLogWriter().info("Added " + keyToAdd + " to set: " + ops);
}

/** return the failedTxOps or inDoubtTxOps
 *
 */
public Set getFailedOps(Object sharedMapKey) {
   Set sharedMapKeys = (Set)blackboard.getSharedMap().getMap().keySet();
   if (!sharedMapKeys.contains(sharedMapKey)) {
      blackboard.getSharedMap().put(sharedMapKey, new HashSet());
   }
   Set ops = (Set)blackboard.getSharedMap().get(sharedMapKey);
   return ops;
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public CQUtilBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public CQUtilBB(String name, String type) {
   super(name, type, CQUtilBB.class);
}
   
  /**
   * Increments the counter with the given name
   */
public static long incrementCounter(String counterName, int counter) {
   long counterValue = CQUtilBB.getBB().getSharedCounters().incrementAndRead(counter);
   Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
   return counterValue;
}
   
}
