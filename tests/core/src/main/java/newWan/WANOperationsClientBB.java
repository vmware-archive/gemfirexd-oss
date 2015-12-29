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
package newWan;

import util.OperationCountersBB;
import hydra.blackboard.Blackboard;

public class WANOperationsClientBB extends Blackboard {

  // shared counter for HA
  public static int NumStartedDoingOps;   // number of operations in process
  public static int NumFinishedDoingOps;   // number of operations in process
  public static int NumDoneValidation; // number of validation operations in process
  public static int NumCycle;      // Cycle counter
  public static int IsReady;        // used to provide coordination between cycles
  public static int InvokeValidation; // to invoke the validation 
  public static int InvokeEntryOperation; // to invoke the validation
  
  //sharedCounter: 
  public static int WanEventResolved; // used to count the resolved events
  public static int NumKeys;          // Counter for num keys in use
  
  // shared variable keys
  public static String IS_TASK_SCHEDULING_STOPPED = "StopSchedulingOrderCalled"; // is StopSchedulingOrder called
  
  // Blackboard variables
  static String BB_NAME = "WANOperationsClient_Blackboard";
  static String BB_TYPE = "RMI";

  // singleton instance of blackboard
  private static WANOperationsClientBB bbInstance = null;
  
  
  /**
   *  Get the WANOperationsClientBB
   */
  public static WANOperationsClientBB getBB() {
      if (bbInstance == null) {
         synchronized ( WANOperationsClientBB.class ) {
            if (bbInstance == null)
               bbInstance = new WANOperationsClientBB(BB_NAME, BB_TYPE);
         }
      }
      return bbInstance;
  }
  
  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public WANOperationsClientBB() {}
  
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public WANOperationsClientBB(String name, String type) {
      super(name, type, WANOperationsClientBB.class);
  }  
}

