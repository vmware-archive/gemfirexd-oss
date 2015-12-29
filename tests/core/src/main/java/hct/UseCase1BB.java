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

package hct;

import hydra.TestConfig;
import hydra.blackboard.Blackboard;

/**
*
* Manages the blackboard for tests in this package.
*
*/

public class UseCase1BB extends Blackboard {

  public static int numEntries;

  private static UseCase1BB blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public UseCase1BB() {
  }
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public UseCase1BB( String name, String type ) {
    super( name, type, UseCase1BB.class );
  }
  /**
   *  Creates a sample blackboard using {@link HctPrms#blackboardName} and
   *  {@link HctPrms#blackboardType}.
   */
  public static UseCase1BB getInstance() {
    if ( blackboard == null )
      synchronized( UseCase1BB.class ) {
        if ( blackboard == null )
          blackboard = new UseCase1BB(
            TestConfig.tab().stringAt( HctPrms.blackboardName ),
            TestConfig.tab().stringAt( HctPrms.blackboardType )
          );
      }
    return blackboard;
  }
}
