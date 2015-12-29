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
package management.test.cli;

import hydra.blackboard.Blackboard;

public class CommandBB extends Blackboard {

  // shared counters
  public static int executionNumber;  // the test execution number 
  public static int managerCount;  // count the number of managers
  public static int configurableInitCounter;
  public static int dataRateDoOps;
  public static int dataRatePutCntr;

  // Blackboard creation variables
  static String BB_NAME = "Command_Blackboard";
  static String BB_TYPE = "RMI";

  public static CommandBB bbInstance = null;
  
  public static void HydraTask_logBB() {
    getBB().printSharedMap();
    getBB().printSharedCounters();
  }

  /**
   *  Get the BB
   */
  public static CommandBB getBB() {
    if (bbInstance == null) {
      synchronized (CommandBB.class) {
        if (bbInstance == null) 
          bbInstance = new CommandBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public CommandBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public CommandBB(String name, String type) {
    super(name, type, CommandBB.class);
  }

}
