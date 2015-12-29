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
package diskRecovery;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedMap;
import hydra.Log;

import java.util.ArrayList;
import java.util.List;

public class RecoveryBB extends Blackboard {

  public static RecoveryBB bbInstance = null;

  // Blackboard creation variables
  static String BB_NAME = "Recovery_Blackboard";
  static String BB_TYPE = "RMI";

  // sharedCounters
  public static int executionNumber;
  public static int doneStartingCounter;
  public static int doneDestroyingCounter; 
  public static int doneCompactingCounter;
  public static int doneVerifyingCounter;
  public static int doneAddingCounter;
  public static int timeToStop;
  public static int snapshotWritten;
  public static int writeSnapshot;
  public static int backupCompleted;
  public static int backupDiskFiles;
  public static int compactionCompleted;
  public static int compactOldVersionFiles;
  public static int pausing;
  public static int leader;
  public static int serversAreBack;
  public static int shutDownAllCompleted;
  public static int currValue;
  public static int verifyBackups; // used to coordinate which thread should verify backups
  public static int backupRestored;
  public static int Reinitialized;    // used to sync threads when a region has been recreated after a cache close
  public static int FinishedVerify;   // used to sync all threads when verification is done
  public static int onlineBackupNumber; // used to determine if online backup should be done
  public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
  public static int backupsDone;
  public static int importSnapshot;
  public static int snapshotImported;
  public static int stoppingVMs;      // used in rvvConvert tests to allow EntryNotFoundExceptions when primary
                                      // (and only copy) of data is lost while shutting down oldVersion VMs
  public static int clears;           // number of clears completed

  // sharedMap keys
  public static final String taskStep = "taskStep";
  public static final String firstKeyIndexKey = "firstKeyIndex";
  public static final String lastKeyIndexKey = "lastKeyIndex";
  public static final String allowPersistedMemberInfo = "allowPersistedMemberInfo";
  public static final String shutDownAllKey = "shutDownAllKey";
  public static final String clearedRegionList = "clearedRegionFullPaths"; 

  /**
   *  Get the blackboard instance
   */
  public static RecoveryBB getBB() {
    if (bbInstance == null) {
      synchronized (RecoveryBB.class) {
        if (bbInstance == null) 
          bbInstance = new RecoveryBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public RecoveryBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public RecoveryBB(String name, String type) {
    super(name, type, RecoveryBB.class);
  }

  protected void addToClearedRegionList(String fullPathName) {
    Blackboard bb = getBB();
    SharedMap aMap = bb.getSharedMap();

    // coordinate access among VMs
    bb.getSharedLock().lock();
    List aList = (List)aMap.get(clearedRegionList);
    if (aList == null) {
      aList = new ArrayList();
    }
    aList.add(fullPathName);
    aMap.put(clearedRegionList, aList);
    bb.getSharedLock().unlock();
    Log.getLogWriter().info("After adding " + fullPathName + ", clearedRegionList = " + aList);
  }
}
