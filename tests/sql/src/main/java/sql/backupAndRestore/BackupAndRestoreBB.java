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
package sql.backupAndRestore;

import hydra.blackboard.Blackboard;

/**
 * The BackupAndRestoreBB class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 1.3
 */
public class BackupAndRestoreBB extends Blackboard {

  // Blackboard creation variables
  static String BB_NAME = "BackupAndRestoreBB";
  static String BB_TYPE = "rmi";
  public static BackupAndRestoreBB bbInstance = null;

  // Shared Counters
  public static int BackupInProgress;      // Used to coordinate which thread should do a backup
  public static int BackupCtr;             // Used to keep track of the backup count and directory name
  public static int ExecutionNumber;       // The serial execution number
  public static int LeaderForFinalBackup;  // Used to make sure that ony one thread is performing the final backup
  public static int NbrOfClientThreads;    // Used to keep track of the number of client threads
  public static int NbrOfVMsStopped;       // Used to keep track of the number of VMs that are stopped
  public static int PauseOps;              // Used to signal all Ops to pause
  public static int RestoreThreadCnt;      // Used to coordinate which thread should do a restore
  public static int TimeToStop;            // Used to tell all clients it is time to throw StopSchedulingException

  // Shared Map
  public static final String DiskDirSnapShotKey = "diskDirSnapShot"; // Used to store the information about the '_disk' directories
  public static final String StoppedVMsKey      = "stoppedVMs";      // ?

  //BigData Shared Counters
  public static int nbrOfThreadsToPause;  // Used to keep track of the number of running threads that need to be pause to complete the test
  public static int theClientLeader;      // Used to indicate which client is the 'leader'
  public static int totalDataBytes;       // Used to store the total bytes stored an all tables
  public static int opTrackerCnt;         // Used to instantiate the OpTracker class
  public static int largestKey;           // Used to track the number of sql inserts that have been performed
  public static int nbrOfUpdates;         // Used to track the number of sql updates that have been performed
  public static int nbrOfDeletes;         // Used to track the number of sql deletes that have been performed
  public static int validateCnt;          // Used to track the number of validations that have been performed
  public static int validationErrorCnt;   // Used to track the number of validations errors that have been encountered

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public BackupAndRestoreBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public BackupAndRestoreBB(String name, String type) {
    super(name, type, BackupAndRestoreBB.class);
  }

  /**
   * Get the blackboard instance
   */
  public static synchronized BackupAndRestoreBB getBB() {
    if (bbInstance == null) {
      bbInstance = new BackupAndRestoreBB(BB_NAME, BB_TYPE);
    }
    return bbInstance;
  }
}
