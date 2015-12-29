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
package dunit.impl;

import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of which tests need to be dispatched
 * 
 * @author kbanks 
 * @since 5.5
 * 
 */
public class DUnitBB extends Blackboard {

  // Blackboard creation variables
  static String BB_NAME = "DUnit_Blackboard";

  static String BB_TYPE = "RMI";
  
  private static DUnitBB blackboard;
  
  
  /**
   * initialize DUnitBB
   */
  public static void initialize() {
    getBB().printSharedCounters();
  }

  /**
   * Get the DUnitBB
   */
  public static DUnitBB getBB() {
    if (blackboard == null)
      synchronized (DUnitBB.class) {
        if (blackboard == null)
          blackboard = new DUnitBB(BB_NAME, BB_TYPE);
      }
    return blackboard;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public DUnitBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public DUnitBB(String name, String type) {
    super(name, type, DUnitBB.class);
  }

}
