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
package diskReg.oplogs;

import hydra.Log;
import hydra.blackboard.Blackboard;

public class DiskBB extends Blackboard {

//Blackboard creation variables
  static String DISK_BB_NAME = "Disk_Blackboard";
  static String DISK_BB_TYPE = "RMI";
  // Counters for number of times test did certain operations
  public static int NUM_PUT;
  public static int NUM_GET;
  public static int VmRestartedCounter; // used for VMs to signal when they are done restarted
  public static int VmReinitializedCounter; // used for VMs to signal when they are done reinitialization

  private static DiskBB bbInstance;

  /** Zero-arg constructor for remote method invocations. */

  public DiskBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public DiskBB(String name, String type) {
      super(name, type, DiskBB.class);
  }

  /**
   *  Get the DiskBB
   */
  public static DiskBB getBB() {
      if (bbInstance == null) {
         synchronized ( DiskBB.class ) {
            if (bbInstance == null) 
               bbInstance = new DiskBB(DISK_BB_NAME, DISK_BB_TYPE);
         }
      }
      return bbInstance;
  }

  /**
   * Sets the value count with the given name to zero.
   */
  public static void zero(String counterName, int counter) {
      DiskBB.getBB().getSharedCounters().zero(counter);
      Log.getLogWriter().info(counterName + " has been zeroed");
  }

  /**
   * Increments the counter with the given name
   */
  public static long incrementCounter(String counterName, int counter) {
      long counterValue = DiskBB.getBB().getSharedCounters().incrementAndRead(counter);
      if(Log.getLogWriter().fineEnabled()){
        Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
      }
      return counterValue;
  }

  /**
   * Adds a given amount to the value of a given counter
   */
  public static long add(String counterName, int counter, int amountToAdd) {
      long counterValue = DiskBB.getBB().getSharedCounters().add(counter, amountToAdd);
      Log.getLogWriter().info("After adding " + amountToAdd + ", " + counterName + " is " + counterValue);
      return counterValue;
  }

}//end of DiskBB
