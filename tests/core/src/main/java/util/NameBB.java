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
package util;

//import util.*;
//import hydra.*;
import mapregion.MapBB;
import hydra.Log;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;

public class NameBB extends Blackboard {
   
// Blackboard variables
private static String BB_NAME = "Name_Blackboard";
private static String BB_TYPE = "RMI";

// singleton instance of blackboard
private static NameBB bbInstance = null;

// Counters to create unique names
public static int POSITIVE_NAME_COUNTER;
public static int NEGATIVE_NAME_COUNTER;
public static int REGION_NAME_COUNTER;
public static int LISTENER_NAME_COUNTER;

public long incrementAndRead(int whichCounter) {
   hydra.blackboard.SharedCounters sharedCounters = this.getSharedCounters();
   long nextCounter = sharedCounters.incrementAndRead(whichCounter);
   return nextCounter;
}
   
public long decrementAndRead(int whichCounter) {
   hydra.blackboard.SharedCounters sharedCounters = this.getSharedCounters();
   long nextCounter = sharedCounters.decrementAndRead(whichCounter);
   return nextCounter;
}
   
  /**
   * Sets the value count with the given name to zero.
   */
  public void zero(String counterName, int whichCounter)
  {
    hydra.blackboard.SharedCounters sharedCounters = this.getSharedCounters();
    sharedCounters.zero(whichCounter);
    Log.getLogWriter().info(counterName + " has been zeroed");
  }
   
public long read(int whichCounter) {
   hydra.blackboard.SharedCounters sharedCounters = this.getSharedCounters();
   long counter = sharedCounters.read(whichCounter);
   return counter;
}
   
/**
 *  Get the NameBB
 */
public static NameBB getBB() {
   if (bbInstance == null) {
      synchronized ( NameBB.class ) {
         if (bbInstance == null) 
            bbInstance = new NameBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public NameBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public NameBB(String name, String type) {
   super(name, type, NameBB.class);
}
   
}
