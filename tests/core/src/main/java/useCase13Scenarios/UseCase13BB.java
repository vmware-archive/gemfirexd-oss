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
package useCase13Scenarios;

//import util.*;
//import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;

public class UseCase13BB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "UseCase13_Blackboard";
static String BB_TYPE = "RMI";

public static UseCase13BB bbInstance = null;

// Counter to allow 'killer' thread to know how much work we've done
public static int numOperationsCompleted;

/**
 *  Get the BB instance
 */
public static UseCase13BB getBB() {
   if (bbInstance == null) {
      synchronized ( UseCase13BB.class ) {
         if (bbInstance == null) 
            bbInstance = new UseCase13BB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public UseCase13BB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public UseCase13BB(String name, String type) {
   super(name, type, UseCase13BB.class);
}
   
}
