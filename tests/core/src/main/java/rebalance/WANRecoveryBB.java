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
package rebalance;

import hydra.blackboard.Blackboard;

public class WANRecoveryBB extends Blackboard {

// Blackboard creation variables
static String BB_NAME = "WANRecovery_Blackboard";
static String BB_TYPE = "RMI";

// Single instance of the BB 
public static WANRecoveryBB bbInstance = null;

// WAN Recovery tests are workload based (number of recyclePrimary/validate cycles)
public static int executionNumber; 

// status flags for recyclePrimary coordination with putSequentialKeys clients
public static int numPublishers;
public static int publishing;
public static int donePublishing;
public static int validationComplete;

// Listeners use these counters to flag problems
public static int missedUpdates;
public static int duplicateUpdates;

// recyclePrimary uses a SharedLock SharedCondition in order to
// notify the publishers that it has completed the current round of
// validation.
public static final String recyclePrimaryComplete = "recyclePrimaryComplete";

// BB SharedMap Keys
public static String ErrorKey = "ERROR_KEY";

/**
 *  Get the BB
 */
public static WANRecoveryBB getBB() {
   if (bbInstance == null) {
      synchronized ( WANRecoveryBB.class ) {
         if (bbInstance == null) 
            bbInstance = new WANRecoveryBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public WANRecoveryBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public WANRecoveryBB(String name, String type) {
   super(name, type, WANRecoveryBB.class);
}
   
}
