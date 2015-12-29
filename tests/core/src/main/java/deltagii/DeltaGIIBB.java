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
package deltagii;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedMap;
import hydra.Log;

import java.util.HashSet;
import java.util.Set;

public class DeltaGIIBB extends Blackboard {

  public static DeltaGIIBB bbInstance = null;

  // Blackboard creation variables
  static String BB_NAME = "DeltaGII_Blackboard";
  static String BB_TYPE = "RMI";

  // sharedCounters
  public static int executionNumber;
  public static int timeToStop;
  public static int snapshotWritten;
  public static int writeSnapshot;
  public static int pausing;
  public static int leader;
  public static int doneVerifying;    // used to sync all threads when verification is done
  public static int stoppingVMs;
  public static int clears;           // number of clears completed
  public static int giiCallbackInvoked;  // set to 1 when getInitialImageCallback invoked
  public static int executeKill;      // controls callback so we only kill during TASKS (we can't count on RemoteTestModule, since callback invoked from async product threads)

  // sharedMap keys
  public static final String clearedRegions = "clearedRegionFullPaths";
  public static final String uniqueKeyIndex = "uniqueKeyIndexForThread_";
  public static final String regionSnapshotKey = "regionSnapshotKey";
  public static final String tombstoneCount = "tombstoneCount_thr";
  public static final String giiStatePrm = "giiStatePrm";

  /**
   *  Get the blackboard instance
   */
  public static DeltaGIIBB getBB() {
    if (bbInstance == null) {
      synchronized (DeltaGIIBB.class) {
        if (bbInstance == null) 
          bbInstance = new DeltaGIIBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public DeltaGIIBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public DeltaGIIBB(String name, String type) {
    super(name, type, DeltaGIIBB.class);
  }

  protected void addToClearedRegionList(String fullPathName) {
    Blackboard bb = getBB();
    SharedMap aMap = bb.getSharedMap();

    // coordinate access among VMs
    bb.getSharedLock().lock();
    Set aSet = (Set)aMap.get(clearedRegions);
    if (aSet == null) {
      aSet = new HashSet();
    }
    aSet.add(fullPathName);
    aMap.put(clearedRegions, aSet);
    bb.getSharedLock().unlock();
    Log.getLogWriter().info("After adding " + fullPathName + ", clearedRegions = " + aSet);
  }
}
