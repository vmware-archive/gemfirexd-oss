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

package versioning;

import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for ConflictedOps tests in this package.
*
*/

public class ConflictedOpsBB extends Blackboard {

public static int ExecutionNumber;  // track and log the number of rounds of execution
public static int processEvents;    // we are not maintaining a list of events fired during the loadRegion step.  This is incremented when the leader executes conflictedOpsTest

// sharedCounters: used by concurrent tests; concurrent threads run in a concurrent 
//                 "round"; all threads begin, pause, verify, end, then start again 
public static int ConcurrentLeader; // used to choose one thread to do once-per-round duties
public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
public static int WaitForSelectedKey;  // waiting for leader to write SelectedKey to BB
public static int Pausing;          // used to sync all threads when pausing for verification
public static int ReadyToVerify;    // used to sync all threads when verification is to begin
public static int FinishedVerify;   // used to sync all threads when verification is done
public static int SnapshotWritten;  // used to sync threads when one thread has written its state
public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException

// sharedMap
public static final String SelectedKey = "SelectedKey";
public static final String ExpectedValue = "ExpectedValue";

  // Blackboard creation variables
  public static String CONFLICTED_OPS_BB_NAME = "ConflictedOps_Blackboard";
  public static String CONFLICTED_OPS_BB_TYPE = "RMI";

  private static ConflictedOpsBB blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public ConflictedOpsBB() {
  }
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public ConflictedOpsBB( String name, String type ) {
    super( name, type, ConflictedOpsBB.class );
  }
  /**
   *  Creates a sample blackboard using {@link ConflictedOpsPrms#blackboardName} and
   *  {@link ConflictedOpsPrms#blackboardType}.
   */
  public static ConflictedOpsBB getInstance() {
    if ( blackboard == null )
      synchronized( ConflictedOpsBB.class ) {
        if ( blackboard == null )
          blackboard = new ConflictedOpsBB(CONFLICTED_OPS_BB_NAME, CONFLICTED_OPS_BB_TYPE);
      }
    return blackboard;
  }
}
