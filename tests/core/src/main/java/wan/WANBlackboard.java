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

package wan;

import hydra.blackboard.*;

/**
 * Manages the blackboard for tests in this package.
 */

public class WANBlackboard extends Blackboard {

  private static WANBlackboard blackboard;
  public static int MaxKeys;
  public static int currentEntry;
  public static int currentEntry1;
  public static int currentEntry2;
  public static int currentEntry3;
  public static int currentEntry4;
  public static int currentEntry5;
  public static int currentEntry6;
  public static int currentEntry_valid; // for the security wan test for the
                                        // valid credentials

  public static int currentEntry_invalid; // for the security wan test for the
                                          // invalid credentials
  public static int currentEntry_reader; // for the security wan test

  public static int currentEntry_writer; // for the security wan test
                                         
  public static int bridgeCloserNumber;
  public static int keyListReady;
  public static int farEndReady;  // used as synchronization
  public static int nearEndReady; // used as synchronization
  
 // SharedMap entries for validating server region entries
  public static String KEY_LIST = "keyList";
  public static String REGION_SIZE = "regionSize";

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public WANBlackboard() {
  }
  /**
   *  Creates a WAN blackboard using the specified name and transport type.
   */
  public WANBlackboard( String name, String type ) {
    super( name, type, WANBlackboard.class );
  }
  /**
   *  Creates a WAN blackboard named "WAN" using RMI for transport.
   */
  public static synchronized WANBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new WANBlackboard("WAN", "RMI");
    }
    return blackboard;
  }
  /**
   *  printBlackboard: ENDTASK to print contents of blackboard
   */
  public static void printBlackboard() {
    hydra.Log.getLogWriter().info("Printing WAN Blackboard contents");
    getInstance().print();
  }
}
