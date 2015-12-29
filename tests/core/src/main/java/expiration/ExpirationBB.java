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
package expiration;

//import util.*;
//import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;

public class ExpirationBB extends Blackboard {
   
// Blackboard creation variables
static String EXPIR_BB_NAME = "Expiration_Blackboard";
static String EXPIR_BB_TYPE = "RMI";

// singleton instance of blackboard
private static ExpirationBB bbInstance = null;

// Shared counters
public static int DoneWithTask;
public static int DoneLoggingRegion;

/**
 *  Get the ExpirationBB
 */
public static ExpirationBB getBB() {
   if (bbInstance == null) {
      synchronized ( ExpirationBB.class ) {
         if (bbInstance == null) 
            bbInstance = new ExpirationBB(EXPIR_BB_NAME, EXPIR_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public ExpirationBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ExpirationBB(String name, String type) {
   super(name, type, ExpirationBB.class);
}
   
}
