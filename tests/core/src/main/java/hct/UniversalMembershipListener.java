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

package hct;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.admin.*;

public class UniversalMembershipListener extends UniversalMembershipListenerAdapter {

  /** Override memberJoined method
   *
   *  @param SystemMembershipEvent memberJoined event
   */
  public void memberJoined(SystemMembershipEvent event) {

    logCall("memberJoined", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();

    if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
      if (((AdaptedMembershipEvent)event).isClient()) {
        long count = sc.incrementAndRead( BBoard.actualClientJoinedEvents );
        Log.getLogWriter().info("After incrementing counter, actualClientJoinedEvents =  " + count);
      } else {
        long count = sc.incrementAndRead( BBoard.actualServerJoinedEvents );
        Log.getLogWriter().info("After incrementing counter, actualServerJoinedEvents =  " + count);
      }
    }
  }

  /** Override UniversalMembershipListenerAdapter, memberLeft method
   *
   *  @param SystemMembershipEvent memberLeft event
   */
  public void memberLeft(SystemMembershipEvent event) {
 
    logCall("memberLeft", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();

    if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
      if (((AdaptedMembershipEvent)event).isClient()) {
        long count = sc.incrementAndRead( BBoard.actualClientDepartedEvents);
        Log.getLogWriter().info("After incrementing counter, actualClientDepartedEvents =  " + count);
      } else {
        long count = sc.incrementAndRead( BBoard.actualServerDepartedEvents);
        Log.getLogWriter().info("After incrementing counter, actualServerDepartedEvents =  " + count);
      }
    }
  }

  /** Override UniversalMembershipListenerAdapter, memberCrashed method
   *
   *  @param SystemMembershipEvent memberCrashed event
   */
  public void memberCrashed(SystemMembershipEvent event) {
    logCall("memberCrashed", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();

    if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
      if (((AdaptedMembershipEvent)event).isClient()) {
        long count = sc.incrementAndRead( BBoard.actualClientCrashedEvents );
        Log.getLogWriter().info("After incrementing counter, actualClientCrashedEvents =  " + count);
      } else {
        long count = sc.incrementAndRead( BBoard.actualServerCrashedEvents );
        Log.getLogWriter().info("After incrementing counter, actualServerCrashedEvents =  " + count);
      }
    }
  }

  /** log event receipt 
   *
   *  @param String - callback name (memberLeft, memberJoined, memberCrashed)
   *  @param SystemMembershipEvent - AdaptedMembershipEvent
   *
   *
   */
  private void logCall(String methodName, SystemMembershipEvent event) {
     String aStr = toString(methodName, event);
     Log.getLogWriter().info(aStr);
  }

  private String toString(String methodName, SystemMembershipEvent event) {
    StringBuffer aStr = new StringBuffer();    
    aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + getMyAppName() + "\n");    

    if (event == null)       
      return aStr.toString();

    if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
      AdaptedMembershipEvent uEvent = (AdaptedMembershipEvent)event;
      aStr.append(  "event.getMemberId(): " + uEvent.getMemberId() + "\n");
      aStr.append(  "event.isClient(): " + uEvent.isClient() + "\n");
    } else {
      aStr.append( event.toString() );
    }
    return aStr.toString();
  }

  /** Returns the hydra bridgeServer name as a String */
  public String getMyAppName() {
     return System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
  }
}
