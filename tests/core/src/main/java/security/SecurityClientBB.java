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
package security;

import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task threads in an
 * {@link EntryOperations} do.
 * 
 * @author Rajesh Kumar
 */
public class SecurityClientBB extends Blackboard {

  // Blackboard creation variables
  static String BRIDGE_BB_NAME = "SecurityClient_Blackboard";

  static String BRIDGE_BB_TYPE = "RMI";

  // Counters used for synchronization
  public static int feedSignal;

  public static int stableSignal;

  public static int threadCount;
  
  private static SecurityClientBB blackboard;

  /**
   * initialize SecurityClientBB
   */
  public static void initialize() {
    getBB().printSharedCounters();
  }

  /**
   * Get the SecurityClientBB
   */
  public static SecurityClientBB getBB() {
    if (blackboard == null)
      synchronized (SecurityClientBB.class) {
        if (blackboard == null)
          blackboard = new SecurityClientBB(BRIDGE_BB_NAME, BRIDGE_BB_TYPE);
      }
    return blackboard;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public SecurityClientBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public SecurityClientBB(String name, String type) {
    super(name, type, SecurityClientBB.class);
  }

}
