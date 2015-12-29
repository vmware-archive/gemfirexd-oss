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
package resman;


import hydra.blackboard.Blackboard;

public class ResourceManBB extends Blackboard {

  // Blackboard creation variables
  static String BB_NAME = "HeapMan_Blackboard";
  static String BB_TYPE = "RMI";

  // BB SharedMap Keys
  public static int lowMemoryFlag; // used for waitForCounter
  public static int firstLowMemoryExceptionTime;
  public static int testStartTime;
  public static int verifyController1; // coordinates one thread does validation
  public static int verifyController2; // coordinates one thread does validation
  public static int verifyController3; // coordinates one thread does validation
  public static volatile int lowCounter;
  public static volatile int criticalMembers;
  public static int criticalStateVersion;

  // Single instance of the BB
  private static final ResourceManBB bbInstance = new ResourceManBB(BB_NAME, BB_TYPE);

  /**
   *  Get the BB
   */
  public static ResourceManBB getBB() {
    return bbInstance;
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public ResourceManBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  private ResourceManBB(final String name, final String type) {
    super(name, type, ResourceManBB.class);
  }
}
