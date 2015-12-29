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

package admin;

import java.util.*;

import hydra.*;
import util.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.*;

/**
 * Convenience class for various admin listeners: SystemMembershipListener,
 * SystemMemberCacheListener.
 *
 * Provides log/display utility for SystemMembershipEvents.
 *
 */
public class AbstractListener {

  /** Log that an event occurred.
   *
   *  @param methodName The name of the SystemMembershipEvent callback method 
   *                    that was invoked.
   *  @param event The event object that was passed to the event.
   */
  public String logCall(String methodName, SystemMembershipEvent event) {
     String aStr = toString(methodName, event);
     Log.getLogWriter().info(aStr);    
     return aStr;
  }

  /** Return a string description of the event. 
   *
   *  @param methodName The name of the Event callback method that was invoked.
   *  @param event The SystemMembership Event object that was passed to the event.
   *
   *  @return A String description of the invoked event.
   */
  public String toString(String methodName, SystemMembershipEvent event) {
     StringBuffer aStr = new StringBuffer();
     aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName() + "\n");
  
     if (event == null)
        return aStr.toString();
  
     aStr.append("   event.getDistributedMember(): " + event.getDistributedMember().toString() + "\n");
     aStr.append("   event.getMemberId(): " + event.getMemberId() + "\n");

     if (event instanceof SystemMemberCacheEvent) {
       Operation op = ((SystemMemberCacheEvent)event).getOperation();
       aStr.append("   Operation: " + op.toString() + "\n");
     }

     if (event instanceof SystemMemberRegionEvent) {
       aStr.append("   RegionPath: " + ((SystemMemberRegionEvent)event).getRegionPath() + "\n");
     }
     return aStr.toString();
  }

  /** verify that the member reporting the event is part of the
   *  active members list.
   *
   *  @see AdminBB.activeMembers
   *
   *  @throws TestException if received for a member not in the list
   */
  public static void verifyMemberJoined(DistributedMember dm) {
    List activeMembers = (List)AdminBB.getInstance().getSharedMap().get(AdminBB.activeMembers);
    if (!activeMembers.contains(dm)) {
      throwException("Event received from member " + dm.toString() + " but member not found in activeMembers list: " + activeMembers);
    }
  }

/**
 * Utility method to write an Exception string to the Admin Blackboard and
 * to also throw an exception containing the same string.
 *  
 * @param errStr String to log, post to EventBB and throw
 *
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 *
 */ 
 public static void throwException(String errStr) {
   hydra.blackboard.SharedMap aMap = AdminBB.getInstance().getSharedMap();
   aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());       
   Log.getLogWriter().info(errStr);
   throw new TestException(errStr);
}

}
