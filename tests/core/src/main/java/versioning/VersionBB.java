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
//import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for tests in this package.
*
*/

public class VersionBB extends Blackboard {

// sharedCounters: used by serial round robin tests
public static int ExecutionNumber;  // the serial execution number
public static int RoundPosition;    // the position (order) of a thread in a round
public static int RoundNumber;      // the number of rounds that have been executed

// sharedCounters: used by concurrent tests; concurrent threads run in a concurrent 
//                 "round"; all threads begin, pause, verify, end, then start again 
public static int ConcurrentLeader; // used to choose one thread to do once-per-round duties
public static int ReadyToBegin;     // used to sync all threads at the beginning of a task
public static int Pausing;          // used to sync all threads when pausing for verification
public static int ReadyToVerify;    // used to sync all threads when verification is to begin
public static int FinishedVerify;   // used to sync all threads when verification is done
public static int SnapshotWritten;  // used to sync threads when one thread has written its state
public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException

// sharedMap: used by serial tests
public static final String RegionSnapshot = "RegionSnapshot";
public static final String DestroyedKeys  = "DestroyedKeys";

  // Blackboard creation variables
  public static String VERSIONBB_NAME = "Version_Blackboard";
  public static String VERSIONBB_TYPE = "RMI";

  private static VersionBB blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public VersionBB() {
  }
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public VersionBB( String name, String type ) {
    super( name, type, VersionBB.class );
  }
  /**
   *  Creates a sample blackboard using {@link VersionPrms#blackboardName} and
   *  {@link VersionPrms#blackboardType}.
   */
  public static VersionBB getInstance() {
    if ( blackboard == null )
      synchronized( VersionBB.class ) {
        if ( blackboard == null )
          blackboard = new VersionBB(VERSIONBB_NAME, VERSIONBB_TYPE);
      }
    return blackboard;
  }
}
