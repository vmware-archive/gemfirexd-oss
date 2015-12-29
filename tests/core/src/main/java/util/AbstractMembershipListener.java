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


import hydra.*;
import hydra.blackboard.*;

import util.TestHelper;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.query.*;

import java.util.*;

/** An abstract listener class that provides logging methods for
 *  BridgeMembershipEvents.
 */
public abstract class AbstractMembershipListener {

/** The process ID of the VM that created this listener */
public int whereIWasRegistered;

/** Constructor */
public AbstractMembershipListener() {
   whereIWasRegistered = ProcessMgr.getProcessId();
}

/** Log that an event occurred.
 *
 *  @param methodName The name of the BridgeMembershipEvent method that was invoked.
 *  @param event The event object that was passed to the event.
 */
public String logCall(String methodName, BridgeMembershipEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}

/** Log a string description of the event.
 *
 *  @param methodName The name of the Event callback method that was invoked.
 *  @param event The BridgeMembership Event object that was passed to the event.
 *  
 *  @returns A String representing the event
 */ 
public String toString(String methodName, BridgeMembershipEvent event) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + getMyAppName() + "\n");

   if (event == null)
      return aStr.toString();

   aStr.append("   event.getMemberId(): " + event.getMemberId() + "\n");
   aStr.append("   event.isClient(): " + event.isClient() + "\n");

   return aStr.toString();
}

/** Returns the hydra client name as a String */
public String getMyAppName() {
  return System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
}

}
