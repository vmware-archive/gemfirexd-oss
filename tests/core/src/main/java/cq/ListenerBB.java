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
package cq;

import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task
 * threads in an {@link ListenerTest} do.  For instance, the test
 * writes an ordered list of the listeners to the BB (ExpectedListeners), 
 * It then perforoms entry or region operations.  The MultiListeners add
 * themselves to the invokedListeners list on the BB.  When control returns
 * to the application thread, the two lists are compared to insure the 
 * correct listeners were invoked in the order provided.
 *
 * @see ListenerTest
 * @see ListenerPrms
 *
 * @author lhughes
 * @since 5.2
 */
public class ListenerBB extends Blackboard {
   
// Blackboard creation variables
static String MY_BB_NAME = "CQListener_Blackboard";
static String MY_BB_TYPE = "RMI";

// Key in the blackboard sharedMap for expected listeners
// Because the test supports multiple clients & multiple regions, the full
// key is ListenerBB.ExpectedListeners + "_" + <cqName>
static String ExpectedListeners = "ExpectedListeners_";
// Key in the blackboard sharedMap for invoked Listeners
// Because the test supports multiple clients & multiple regions, the full
// key is ListenerBB.InvokedListeners + "_" + <cqName>
static String InvokedListeners = "InvokedListeners_";

// Counters for number of times CQListener invoked (this round)
// for use with serialMultiListener test only
public static int NUM_LISTENER_INVOCATIONS;

// singleton instance of the Blackboard
static public ListenerBB bbInstance = null;

/**
 *  Get the ListenerBB
 */
public static ListenerBB getBB() {
   if (bbInstance == null) {
      synchronized ( ListenerBB.class ) {
         if (bbInstance == null) 
            bbInstance = new ListenerBB(MY_BB_NAME, MY_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public ListenerBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ListenerBB(String name, String type) {
   super(name, type, ListenerBB.class);
}
   
}
