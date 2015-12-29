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
package delta;

import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task threads in an
 * {@link Feeder} do.
 * @author aingle
 * @since 6.1
 */
public class DeltaPropagationBB extends Blackboard {

  // Blackboard creation variables
  static String BRIDGE_BB_NAME = "DeltaClientQueue_Blackboard";

  static String BRIDGE_BB_TYPE = "RMI";

  // Counters for number of times test did certain operations
  public static int NUM_CREATE;

  public static int NUM_UPDATE;

  // counter for delta purpose
  public static int NUM_DELTA_CREATE;

  public static int NUM_DELTA_UPDATE;

  public static int NUM_NON_DELTA_CREATE;
  
  public static int NUM_NON_DELTA_UPDATE;
  
  public static int NUM_INVALIDATE;

  public static int NUM_DESTROY;

  public static int NUM_EXCEPTION;

  public static int NUM_CLIENTS_KILL;

  //1 if OverFlow happens in HA
  public static int HA_OVERFLOW_STATUS;

  //Counter to check for the total no. of conflation happened across all the VMs
  public static int NUM_GLOBAL_CONFLATE;

  // Counters used for synchronization
  public static int feedSignal;

  public static int stableSignal;

// sharedMap: used by serial tests
public static final String RegionSnapshot = "RegionSnapshot";
public static final String DestroyedKeys  = "DestroyedKeys";
public static final String ErrorKey  = "Error";

// sharedMap: used for conc verifying
public static final String VerifyFlag = "VerifyFlag";
public static final String EntryDone = "EntryDone";
public static final String VerifyDone = "VerifyDone";

// sharedCounters: used by serial round robin tests
public static int ExecutionNumber;  // the serial execution number
public static int RoundPosition;    // the position (order) of a thread in a round
public static int RoundNumber;      // the number of rounds that have been executed
public static int toDeltaIdNumber;  // a unique id number for each toDelta call

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


  private static DeltaPropagationBB blackboard;

  /**
   * initialize HAClientQueueBB
   */
  public static void initialize() {
    getBB().printSharedCounters();
  }

  /**
   * Get the DeltaClientQueueBB
   */
  public static DeltaPropagationBB getBB() {
    if (blackboard == null)
      synchronized (DeltaPropagationBB.class) {
        if (blackboard == null)
          blackboard = new DeltaPropagationBB(BRIDGE_BB_NAME, BRIDGE_BB_TYPE);
      }
    return blackboard;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public DeltaPropagationBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public DeltaPropagationBB(String name, String type) {
    super(name, type, DeltaPropagationBB.class);
  }

}
