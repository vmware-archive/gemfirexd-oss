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
package sql.dataextractor;

import hydra.blackboard.Blackboard;

public class DataExtractorBB extends Blackboard {
  public static DataExtractorBB bbInstance = null;

  // Blackboard creation variables
  static String BB_NAME = "DataSalvager_Blackboard";
  static String BB_TYPE = "RMI";

  // sharedCounters
  public static int recycledAllVMs;
  public static int recyclingStarted;

  /**
   * Get the blackboard instance
   */
  public static DataExtractorBB getBB() {
    if (bbInstance == null) {
      synchronized (DataExtractorBB.class) {
        if (bbInstance == null)
          bbInstance = new DataExtractorBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public DataExtractorBB() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public DataExtractorBB(String name, String type) {
    super(name, type, DataExtractorBB.class);
  }

}
