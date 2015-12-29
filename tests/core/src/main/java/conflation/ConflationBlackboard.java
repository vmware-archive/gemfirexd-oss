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

package conflation;

import hydra.blackboard.Blackboard;

/**
* Holds a singleton instance per VM.
*/

public class ConflationBlackboard extends Blackboard {

  public static ConflationBlackboard blackboard;

  public static int Puts;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public ConflationBlackboard() {
  }
  public ConflationBlackboard( String name, String type ) {
    super( name, type, ConflationBlackboard.class );
  }
  /**
   *  Creates a singleton blackboard.
   */
  public static ConflationBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new ConflationBlackboard("ConflationBlackboard", "RMI");
    }
    return blackboard;
  }
  /**
   *  printBlackboard: ENDTASK to print contents of blackboard
   */
  public static void printBlackboard() {
    getInstance().print();
  }
}
