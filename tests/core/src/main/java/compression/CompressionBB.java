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
package compression;

import hydra.blackboard.Blackboard;

/**
 * The CompressionBB class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class CompressionBB extends Blackboard {

  // Blackboard creation variables
  static String BB_NAME = "Compression_Blackboard";
  static String BB_TYPE = "RMI";
  public static CompressionBB bbInstance = null;

  // sharedCounters
  public static int CntOfEmptyAccessors;    // the current count of empty regions that've been created
  public static int CntOfCompressedRegions; // the current count of compressed regions that've been created
  public static int RoundCounter;           // the number of rounds (in the round robin) that have been executed
  public static int RoundPosition;          // the position of a thread in the round
  public static int ObjectsAddedCnt;        // the count of objects that have been added to the region

  // ...
  public static String stringData; // ...

  /**
   *  Get the BB
   */
  public static CompressionBB getBB() {
    if (bbInstance == null) {
      synchronized ( CompressionBB.class ) {
        if (bbInstance == null)
          bbInstance = new CompressionBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public CompressionBB() {
  }

  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public CompressionBB(String name, String type) {
    super(name, type, CompressionBB.class);
  }
}
