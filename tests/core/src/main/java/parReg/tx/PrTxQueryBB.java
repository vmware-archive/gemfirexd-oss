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

public class PrTxQueryBB extends Blackboard {
   
// Blackboard creation variables
static String BB_NAME = "PrTxQuery_Blackboard";
static String BB_TYPE = "RMI";

public static int ReadyForTx;  
public static int PositiveCommit;
public static int NegativeCommit;

public static PrTxQueryBB bbInstance = null;

/**
 *  Get the BB
 */
public static PrTxQueryBB getBB() {
   if (bbInstance == null) {
      synchronized ( PrTxQueryBB.class ) {
         if (bbInstance == null) 
            bbInstance = new PrTxQueryBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public PrTxQueryBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public PrTxQueryBB(String name, String type) {
   super(name, type, PrTxQueryBB.class);
}

}
