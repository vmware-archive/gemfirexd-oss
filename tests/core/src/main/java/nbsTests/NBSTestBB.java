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
package nbsTests;

import java.util.*;
import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard with counters to track of failed tx ops in nbsTest.conf
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class NBSTestBB extends Blackboard {
   
// Blackboard creation variables
public static String NBS_BB_NAME = "NBS_Blackboard";
public static String NBS_BB_TYPE = "RMI";

// singleton instance of blackboard
private static NBSTestBB bbInstance = null;

// Counters for failed tx ops
public static int FAILED_CREATES;
public static int FAILED_UPDATES;
public static int FAILED_DESTROYS;
public static int FAILED_INVALIDATES;

/**
 *  Get the NBSTestBB
 */
public static NBSTestBB getBB() {
   if (bbInstance == null) {
      synchronized ( NBSTestBB.class ) {
         if (bbInstance == null) 
            bbInstance = new NBSTestBB(NBS_BB_NAME, NBS_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public NBSTestBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public NBSTestBB(String name, String type) {
   super(name, type, NBSTestBB.class);
}
   
}
