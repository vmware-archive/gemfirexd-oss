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

package hydra.samples;

import hydra.TestConfig;
import hydra.blackboard.Blackboard;
//import hydra.blackboard.SharedCounters;
//import hydra.blackboard.SharedMap;

/**
*
* Manages the blackboard for distributed tests in this package.
*
*/

public class DistBlackboard extends Blackboard {

  public static int NumWrites;
  public static int NumReads;

  private static DistBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public DistBlackboard() {
  }
  /**
   *  Creates a blackboard using the specified name and transport type.
   */
  public DistBlackboard( String name, String type ) {
    super( name, type, DistBlackboard.class );
  }
  /**
   *  Creates a blackboard using {@link DistPrms#blackboardName} and
   *  {@link DistPrms#blackboardType}.
   */
  public static synchronized DistBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new DistBlackboard(
        TestConfig.tab().stringAt(DistPrms.blackboardName),
        TestConfig.tab().stringAt(DistPrms.blackboardType)
      );
    }
    return blackboard;
  }
}
