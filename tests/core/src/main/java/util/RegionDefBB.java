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
package util;

//import util.*;
//import hydra.*;
//import java.io.*;
import hydra.blackboard.Blackboard;

public class RegionDefBB extends Blackboard {
   
// Blackboard variables
static String REG_DEF_BB_NAME = "RegDef_Blackboard";
static String REG_DEF_BB_TYPE = "RMI";

// singleton instance of blackboard
private static RegionDefBB bbInstance;

// Blackboard counters
public static int DISK_DIR_NUMBER;  // used for creating unique disk dir names for diskRegions

/**
 *  Get the RegionDefBB
 */
public static RegionDefBB getBB() {
   if (bbInstance == null) {
      synchronized ( RegionDefBB.class ) {
         if (bbInstance == null)
            bbInstance = new RegionDefBB(REG_DEF_BB_NAME, REG_DEF_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public RegionDefBB() {
}
   
public static void printBB() {
   getBB().printSharedMap();
}

/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public RegionDefBB(String name, String type) {
   super(name, type, RegionDefBB.class);
}
   
}
