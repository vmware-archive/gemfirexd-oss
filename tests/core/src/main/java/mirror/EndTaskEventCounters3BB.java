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
package mirror;

//import util.*;
//import hydra.*;
import hydra.blackboard.*;
//import com.gemstone.gemfire.cache.*;

public class EndTaskEventCounters3BB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "EndTaskEventCounters3_Blackboard";
static String BB_TYPE = "RMI";

// singleton instance of blackboard
private static EndTaskEventCounters3BB bbInstance = null;

// Event counters
// afterCreate events
public static int numAfterCreateEvents_isDist;
public static int numAfterCreateEvents_isNotDist;
public static int numAfterCreateEvents_isExp;
public static int numAfterCreateEvents_isNotExp;
public static int numAfterCreateEvents_isRemote;
public static int numAfterCreateEvents_isNotRemote;

// afterDestroy events
public static int numAfterDestroyEvents_isDist;
public static int numAfterDestroyEvents_isNotDist;
public static int numAfterDestroyEvents_isExp;
public static int numAfterDestroyEvents_isNotExp;
public static int numAfterDestroyEvents_isRemote;
public static int numAfterDestroyEvents_isNotRemote;

// afterInvalidate events
public static int numAfterInvalidateEvents_isDist;
public static int numAfterInvalidateEvents_isNotDist;
public static int numAfterInvalidateEvents_isExp;
public static int numAfterInvalidateEvents_isNotExp;
public static int numAfterInvalidateEvents_isRemote;
public static int numAfterInvalidateEvents_isNotRemote;

// afterUpdate events
public static int numAfterUpdateEvents_isDist;
public static int numAfterUpdateEvents_isNotDist;
public static int numAfterUpdateEvents_isExp;
public static int numAfterUpdateEvents_isNotExp;
public static int numAfterUpdateEvents_isRemote;
public static int numAfterUpdateEvents_isNotRemote;

// afterRegionDestroy events
public static int numAfterRegionDestroyEvents_isDist;
public static int numAfterRegionDestroyEvents_isNotDist;
public static int numAfterRegionDestroyEvents_isExp;
public static int numAfterRegionDestroyEvents_isNotExp;
public static int numAfterRegionDestroyEvents_isRemote;
public static int numAfterRegionDestroyEvents_isNotRemote;

// afterRegionInvalidate events
public static int numAfterRegionInvalidateEvents_isDist;
public static int numAfterRegionInvalidateEvents_isNotDist;
public static int numAfterRegionInvalidateEvents_isExp;
public static int numAfterRegionInvalidateEvents_isNotExp;
public static int numAfterRegionInvalidateEvents_isRemote;
public static int numAfterRegionInvalidateEvents_isNotRemote;

/**
 *  Get the EndTaskEventCounters3BB
 */
public static EndTaskEventCounters3BB getBB() {
   if (bbInstance == null) {
      synchronized ( EndTaskEventCounters3BB.class ) {
         if (bbInstance == null) 
            bbInstance = new EndTaskEventCounters3BB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public EndTaskEventCounters3BB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public EndTaskEventCounters3BB(String name, String type) {
   super(name, type, EndTaskEventCounters3BB.class);
}
   
}
