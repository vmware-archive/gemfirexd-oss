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

package org.jgroups.simple;

import hydra.TestConfig;
import hydra.blackboard.Blackboard;
//import hydra.blackboard.SharedCounters;
//import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for distributed tests in this package.
*
*/

public class SimpleBlackboard extends Blackboard {

  public static int NumEvents;
  public static int EventsElapsed;
  public static int NumTimeOuts;
  
  private static SimpleBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public SimpleBlackboard() {
  }
  /**
   *  Creates a blackboard using the specified name and transport type.
   */
  public SimpleBlackboard( String name, String type ) {
    super( name, type, SimpleBlackboard.class );
  }
  /**
   *  Creates a blackboard using {@link SimpleParms#blackboardName} and
   *  {@link SimpleParms#blackboardType}.
   */
  public static SimpleBlackboard getInstance() {
    if ( blackboard == null )
      synchronized( SimpleBlackboard.class ) {
        if ( blackboard == null )
          blackboard = new SimpleBlackboard(
            TestConfig.tab().stringAt( SimpleParms.blackboardName ),
            TestConfig.tab().stringAt( SimpleParms.blackboardType )
          );
      }
    return blackboard;
  }
}
