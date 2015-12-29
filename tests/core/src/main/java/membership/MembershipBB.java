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
package membership; 

import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard for the MemberTest
 *
 * @author Jean Farris
 * @since 4.5
 */
public class MembershipBB extends Blackboard {
   
// Blackboard creation variables
static String MEMBERSHIP_BB_NAME = "Membership_Blackboard";
static String MEMBERSHIP_BB_TYPE = "RMI";

private static MembershipBB bbInstance = null;

static String DEPARTED = "departed";


// Counters for syncing membership change and verification tasks
public static int verifyCompleted;
public static int taskCompletedToHere;
public static int membershipChangeInProgress;
public static int numTotalTaskCompletions;
// no longer needed?
public static int taskReadyToStart;

// Counter to keep track of test start time
public static int startTime;

/**
 * Returns the blackboard
 */
public static MembershipBB getBB() {
   if (bbInstance == null) {
      synchronized ( MembershipBB.class ) {
         if (bbInstance == null) 
            bbInstance = new MembershipBB(MEMBERSHIP_BB_NAME, MEMBERSHIP_BB_TYPE);
      }
   }
   return bbInstance;
}
   

/**
 *  Zero-arg constructor for remote method invocations.
 */
public MembershipBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public MembershipBB(String name, String type) {
   super(name, type, MembershipBB.class);
}
  /**
   * Increments and reads the counter with the given name
   */
public static long incrementAndReadCounter(int counter) {
   long counterValue = MembershipBB.getBB().getSharedCounters().incrementAndRead(counter);
   return counterValue;
}

  /**
   * Decrements and reads the counter with the given name
   *
   */
public static long decrementAndReadCounter(int counter) {
  long counterValue = MembershipBB.getBB().getSharedCounters().decrementAndRead(counter);
  return counterValue;
}
   
  /**
   * Increments the counter with the given name
   */
public static void incrementCounter(int counter) {
   MembershipBB.getBB().getSharedCounters().increment(counter);
}

  /**
   * Decrements the counter with the given name
   *
   */
public static void decrementCounter(int counter) {
  MembershipBB.getBB().getSharedCounters().decrement(counter);
}
  /**
   * Read the counter with the given name
   *
   */
public static long readCounter(int counter) {
  long counterValue = MembershipBB.getBB().getSharedCounters().read(counter);  
  return counterValue;
}
  /**
   * Set the counter with the given name to zero
   *
   */
public static void zeroCounter(int counter) {
  MembershipBB.getBB().getSharedCounters().zero(counter);
}

}
