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

package hct;

import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;
//import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for tests in this package.
*
*/

public class BBoard extends Blackboard {

  public static int Adds;
  public static int Removes;
  public static int NameCounter;

  // Counters required for BridgeMembershipListener tests
  // Servers
  public static int expectedServerDepartedEvents;
  public static int actualServerDepartedEvents;
  public static int expectedServerCrashedEvents;
  public static int actualServerCrashedEvents;
  public static int expectedServerJoinedEvents;
  public static int actualServerJoinedEvents;

  // Clients
  public static int expectedClientDepartedEvents;
  public static int actualClientDepartedEvents;
  public static int expectedClientJoinedEvents;
  public static int actualClientJoinedEvents;
  public static int expectedClientCrashedEvents;
  public static int actualClientCrashedEvents;

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
public static int DoneRegisterInterest; // used to sync all threads after registering interest
public static int FinishedVerify;   // used to sync all threads when verification is done
public static int SnapshotWritten;  // used to sync threads when one thread has written its state
public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException

// sharedMap: used by serial tests
public static final String RegionSnapshot = "RegionSnapshot";
public static final String DestroyedKeys  = "DestroyedKeys";

  // this is used to tell the tasks that the pure java error case (which
  // is an expected error) has occurred and to not try to execute tasks. 
  // (StopSchedulingOrder is thrown from init tasks when the pure java 
  // error occurs, but the regular tasks still get scheduled; this tells 
  // the regular tasks to just exit). The counter will be non-zero if
  // we correctly detected the pureJava error case
  public static int PureJavaErrorCase;

  // interest policy tests
  public static int testCase;

  private static BBoard blackboard;

  /** Zero all counters in this blackboard
   */
  public void zeroAllCounters() {
    SharedCounters sc = getSharedCounters();
    sc.zero(expectedServerDepartedEvents);
    sc.zero(expectedServerCrashedEvents);
    sc.zero(expectedServerJoinedEvents);
    sc.zero(actualServerDepartedEvents);
    sc.zero(actualServerCrashedEvents);
    sc.zero(actualServerJoinedEvents);

    sc.zero(expectedClientDepartedEvents);
    sc.zero(expectedClientCrashedEvents);
    sc.zero(expectedClientJoinedEvents);
    sc.zero(actualClientDepartedEvents);
    sc.zero(actualClientCrashedEvents);
    sc.zero(actualClientJoinedEvents);
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public BBoard() {
  }
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public BBoard( String name, String type ) {
    super( name, type, BBoard.class );
  }
  /**
   *  Creates a sample blackboard using {@link HctPrms#blackboardName} and
   *  {@link HctPrms#blackboardType}.
   */
  public static BBoard getInstance() {
    if ( blackboard == null )
      synchronized( BBoard.class ) {
        if ( blackboard == null )
          blackboard = new BBoard(
            TestConfig.tab().stringAt( HctPrms.blackboardName ),
            TestConfig.tab().stringAt( HctPrms.blackboardType )
          );
      }
    return blackboard;
  }
}
