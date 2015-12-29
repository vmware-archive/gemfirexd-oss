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

package dlock;

import hydra.*;
import hydra.blackboard.Blackboard;
//import hydra.blackboard.SharedCounters;
//import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for tests in this package.
* Holds a singleton instance per VM.
*
*/

public class DLockBlackboard extends Blackboard {

  public static int hasLock;
  public static int hasNoLock;

  public static int TotalTimeMs;
  public static int MinLoopTimeMs;
  public static int MaxLoopTimeMs;

  public static int IntegerReads;
  public static int StringReads;
  public static int ArrayOfPrimLongReads;
  public static int ArrayOfObjectReads;
  public static int ArrayListReads;
  public static int HashMapReads;
  public static int HashSetReads;
  public static int HashtableReads;
  public static int VectorReads;
  public static int LinkedListReads;
  public static int SyncArrayListReads;
  public static int SyncHashMapReads;

  public static int IntegerUpdates;
  public static int StringUpdates;
  public static int ArrayOfPrimLongUpdates;
  public static int ArrayOfObjectUpdates;
  public static int ArrayListUpdates;
  public static int HashMapUpdates;
  public static int HashSetUpdates;
  public static int HashtableUpdates;
  public static int VectorUpdates;
  public static int LinkedListUpdates;
  public static int SyncArrayListUpdates;
  public static int SyncHashMapUpdates;

  // counters for lease time tests
  public static int LockOrder;
  public static int ReadyToLock;
  public static int DoneWithLock;
  public static int DoneWithTask;

  // counters for crash lock holder and GrantorTest tests
  public static int NumCrashes;
  public static int NumCurrentThreads;
  public static int NumGrantors;

  // for DLSCreateDestroy tests
  public static int LockNameIndex;

  public static DLockBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public DLockBlackboard() {
  }
  public DLockBlackboard( String name, String type ) {
    super( name, type, DLockBlackboard.class );
  }
  /**
   *  Creates a singleton dlock blackboard using {@link DLockPrms#blackboardName}
   *  and {@link DLockPrms#blackboardType}.
   */
  public static DLockBlackboard getInstance() {
    if ( blackboard == null )
      initialize();
    return blackboard;
  }
  private static synchronized void initialize() {
    if ( blackboard == null )
    blackboard = new DLockBlackboard
                 (
                     TestConfig.tab().stringAt( DLockPrms.blackboardName ),
                     TestConfig.tab().stringAt( DLockPrms.blackboardType )
                 );
  }
  /**
   *  printBlackboard: ENDTASK to print contents of dlock blackboard
   */
  public static void printBlackboard() {
    getInstance().print();
  }
}
