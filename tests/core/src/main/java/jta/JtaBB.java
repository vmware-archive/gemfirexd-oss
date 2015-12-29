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
package jta;

import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task
 * threads do.  
 */
public class JtaBB extends Blackboard {
   
// Blackboard creation variables
static String JTA_BB_NAME = "Jta_Blackboard";
static String JTA_BB_TYPE = "RMI";

// Counters for number of times test did certain operations
public static int NUM_CREATE;
public static int NUM_UPDATE;
public static int NUM_GET;

// Counter to generate keys/values for cache callback tests
public static int COUNTER;

// singleton instance of this Blackboard
public static JtaBB bbInstance = null;

// sharedMap holds the name of the oracle database table
// to be shared from start -> task -> end tasks
public static String dbTableName = "dbTableName";

/**
 *  Get the JtaBB
 */
public static JtaBB getBB() {
   if (bbInstance == null) {
      synchronized ( JtaBB.class ) {
         if (bbInstance == null) 
            bbInstance = new JtaBB(JTA_BB_NAME, JTA_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public JtaBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public JtaBB(String name, String type) {
   super(name, type, JtaBB.class);
}
   
  /**
   * Increments the counter with the given name
   */
public static long incrementCounter(String counterName, int counter) {
   long counterValue = JtaBB.getBB().getSharedCounters().incrementAndRead(counter);
   Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
   return counterValue;
}
   
}
