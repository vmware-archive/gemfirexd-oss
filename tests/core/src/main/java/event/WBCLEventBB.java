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

package event;

import hydra.blackboard.*;

/**
 * Manages the blackboard for tests in this package.
 */

public class WBCLEventBB extends Blackboard {

  private static WBCLEventBB blackboard;

  public static int NUM_CREATES;
  public static int NUM_UPDATES;
  public static int NUM_INVALIDATES;
  public static int NUM_DESTROYS;
  
  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public WBCLEventBB() {
  }
  /**
   *  Creates a blackboard using the specified name and transport type.
   */
  public WBCLEventBB( String name, String type ) {
    super( name, type, WBCLEventBB.class );
  }
  /**
   *  Creates a blackboard named "WBCLEvent" using RMI for transport.
   */
  public static synchronized WBCLEventBB getBB() {
    if (blackboard == null) {
      blackboard = new WBCLEventBB("WBCLEvent", "RMI");
    }
    return blackboard;
  }
}
