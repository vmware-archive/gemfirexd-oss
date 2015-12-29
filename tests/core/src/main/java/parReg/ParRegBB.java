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
package parReg;

import hydra.Log;
import hydra.blackboard.Blackboard;
import java.util.*;

public class ParRegBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "ParReg_Blackboard";
static String BB_TYPE = "RMI";

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
public static int BridgeServerTally; // the number of servers created during a test, includes restarted servers
public static int BridgeServerCounter; // the number of servers in a test (excludes restarted server)
public static int choice;           // used to cycle through random choices, to ensure we eventually hit them
                                    // all rather than waiting for true randomness to hit them all
public static int rebalance;        // used to coordinate a single rebalance in a task
public static int rebalanceCompleted; // used to signal rebalancing has completed
public static int BeginVerify; // used to signal time to begin verification
public static int operationsCoordinator;
public static int numEmptyClients; // used to track the number of empty bridge clients
public static int numThinClients; // used to track the number of thin bridge clients
                                  // (clients with eviction to keep them small)
public static int genericCounter; // used to help track concurrentMap ops that are noops
public static int onlineBackup; // used to coordinate which thread should do an online backup
public static int onlineBackupNumber; // used to determine if online backup should be done
                                        // with the command line or through the admin api
public static int verifyBackups; // used to coordinate which thread should verify backups
public static int backupRestored;
public static int backupsDone;
 
// counters for persistence tests
public static int DiskDirsCounter;
public static int DiskRecoveryCounter;
public static int RecoverySnapshotLeader;
public static int MaxRC; // the maximum redundantCopy setting in this test

// sharedCounters: used by fill tests
public static int LocalMaxMemoryCounter;   // used to determine a value for local max memory
public static int ErrorRecorded;           // used to ensure only the first error gets recorded in the shared map
public static int AlertForLocalMaxMemory1; // counter for alerts for LOCAL_MAX_MEMORY = 1 MB  
public static int AlertForLocalMaxMemory2; // counter for alerts for LOCAL_MAX_MEMORY = 2 MB1  
public static int AlertForLocalMaxMemory3; // etc.  
public static int AlertForLocalMaxMemory4;   
public static int AlertForLocalMaxMemory5;   
public static int AlertForLocalMaxMemory6;   
public static int AlertForLocalMaxMemory7;   
public static int AlertForLocalMaxMemory8;   

// SharedMap key for failed tx ops (due to TransactionDataNodeHasDeparted, TransactionDataRebalancedExceptions)
public static String FAILED_TXOPS = "FailedTxOps";
// SharedMap key for failed tx ops (due to TransactionInDoubt)
public static String INDOUBT_TXOPS = "InDoubtTxOps";

// sharedMap: used by fill tests
public static final String ErrorKey = "Error";

// sharedMap: used by serial tests
public static final String RegionSnapshot = "RegionSnapshot";
public static final String DestroyedKeys  = "DestroyedKeys";
public static int resultSenderCounter;

//shared counters used by Function Routing tests
public static int numOfAccessors; 
public static int numOfDataStores;

//shared counters used by PR eviction test
public static int numOfPutOperations;

public static int indexNameCounter;

public static ParRegBB bbInstance = null;

/**
 *  Get the BB
 */
public static ParRegBB getBB() {
   if (bbInstance == null) {
      synchronized ( ParRegBB.class ) {
         if (bbInstance == null) 
            bbInstance = new ParRegBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public ParRegBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ParRegBB(String name, String type) {
   super(name, type, ParRegBB.class);
}

/** Add the key to the failedTxOps or inDoubtTxOps Sets in the shared map
 *
 */
public synchronized void addFailedOp(Object sharedMapKey, Object keyToAdd) {
   Log.getLogWriter().info("Adding " + keyToAdd + " to " + sharedMapKey);

   // coordinate access among VMs
   bbInstance.getSharedLock().lock();
   
   if (ParRegBB.getBB().getSharedMap().get(sharedMapKey) == null) {
      bbInstance.getSharedMap().put(sharedMapKey, new HashSet());
   }
   Set ops = (Set)bbInstance.getSharedMap().get(sharedMapKey);
   ops.add(keyToAdd);
   bbInstance.getSharedMap().put(sharedMapKey, ops);
   bbInstance.getSharedLock().unlock();

   Log.getLogWriter().info("Added " + keyToAdd + " to set: " + ops);
}

/** return the failedTxOps or inDoubtTxOps
 *
 */
public Set getFailedOps(Object sharedMapKey) {
   if (ParRegBB.getBB().getSharedMap().get(sharedMapKey) == null) {
      bbInstance.getSharedMap().put(sharedMapKey, new HashSet());
   }
   Set ops = (Set)bbInstance.getSharedMap().get(sharedMapKey);
   return ops;
}

}
