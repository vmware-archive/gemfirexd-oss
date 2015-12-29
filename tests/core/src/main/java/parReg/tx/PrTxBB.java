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
package parReg.tx;

import hydra.Log;
import hydra.blackboard.Blackboard;

public class PrTxBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "PrTx_Blackboard";
static String BB_TYPE = "RMI";

// sharedMap: 
public static final String pidList = "Pids";    
public static final String keySet = "keySet_";  // for thread specific keySetResult
public static final String targetDM = "targetDM";  
public static final String alwaysFireLocalListeners = "alwaysFireLocalListeners";
public static final String redundantCopies = "redundantCopies";

public static PrTxBB bbInstance = null;

/**
 *  Get the BB
 */
public static PrTxBB getBB() {
   if (bbInstance == null) {
      synchronized ( PrTxBB.class ) {
         if (bbInstance == null) 
            bbInstance = new PrTxBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public PrTxBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public PrTxBB(String name, String type) {
   super(name, type, PrTxBB.class);
}

}
