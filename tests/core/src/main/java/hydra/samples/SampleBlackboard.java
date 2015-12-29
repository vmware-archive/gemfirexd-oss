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
* Manages the blackboard for tests in this package.
*
*/

public class SampleBlackboard extends Blackboard {

  public static int NumConnectionsOpened;
  public static int NumConnectionsClosed;
  public static int MaxSample1;
  public static int NextUniqueID;
  public static int MiscInt;

  private static SampleBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public SampleBlackboard() {
  }
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public SampleBlackboard( String name, String type ) {
    super( name, type, SampleBlackboard.class );
  }
  /**
   *  Creates a sample blackboard using {@link SamplePrms#blackboardName} and
   *  {@link SamplePrms#blackboardType}.
   */
  public static synchronized SampleBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new SampleBlackboard(
        TestConfig.tab().stringAt(SamplePrms.blackboardName),
        TestConfig.tab().stringAt(SamplePrms.blackboardType)
      );
    }
    return blackboard;
  }
}
