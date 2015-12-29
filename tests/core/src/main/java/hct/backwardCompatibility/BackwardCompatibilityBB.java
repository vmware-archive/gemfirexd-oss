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
package hct.backwardCompatibility;

import java.util.*;
import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard for BackwardCompatibility tests.
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 */
 public class BackwardCompatibilityBB extends Blackboard {
   
  // Blackboard creation variables
  public static String BB_NAME = "BackwardCompatibility_Blackboard";
  public static String BB_TYPE = "RMI";

  // singleton instance of blackboard
  private static BackwardCompatibilityBB bbInstance = null;

  /**
   *  Get the BackwardCompatibilityBB
   */
  public static BackwardCompatibilityBB getBB() {
     if (bbInstance == null) {
        synchronized ( BackwardCompatibilityBB.class ) {
           if (bbInstance == null) 
              bbInstance = new BackwardCompatibilityBB(BB_NAME, BB_TYPE);
        }
     }
     return bbInstance;
  }
   
  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public BackwardCompatibilityBB() {
  }
   
  /**
   *  Creates a sample blackboard using the specified name and transport type.
   */
  public BackwardCompatibilityBB(String name, String type) {
     super(name, type, BackwardCompatibilityBB.class);
}
   
}
