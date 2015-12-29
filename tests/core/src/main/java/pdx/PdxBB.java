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
package pdx;

import hydra.blackboard.Blackboard;

public class PdxBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "Pdx_Blackboard";
static String BB_TYPE = "RMI";

public static PdxBB bbInstance = null;

//sharedCounters
public static int pdxClassPathIndex; 
public static int loadCoordinator;
public static int shutDownAllInProgress;
public static int restartingAfterShutDownAll;

/**
 *  Get the BB
 */
public static PdxBB getBB() {
   if (bbInstance == null) {
      synchronized (PdxBB.class) {
         if (bbInstance == null) 
            bbInstance = new PdxBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public PdxBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public PdxBB(String name, String type) {
   super(name, type, PdxBB.class);
}

}
