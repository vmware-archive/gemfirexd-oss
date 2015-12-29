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

package splitBrain;

import java.util.*;

import hydra.*;
import hydra.blackboard.*;
import util.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.*;

public class SBSystemMembershipListener implements SystemMembershipListener {

  /** Implementation of SystemMembershipListener, memberJoined interface
   *
   *  @param SystemMembershipEvent memberJoined event
   */
  public void memberJoined(SystemMembershipEvent event) {
    logCall("memberJoined", event);
  }

  /** Implementation of SystemMembershipListener, memberLeft interface
   *
   *  @param SystemMembershipEvent memberLeft event
   */
  public void memberLeft(SystemMembershipEvent event) {
    logCall("memberLeft", event);
  }

  /** Implementation of SystemMembershipListener, memberCrashed interface
   *
   *  @param SystemMembershipEvent memberCrashed event
   */
  public void memberCrashed(SystemMembershipEvent event) {
    logCall("memberCrashed", event);

    boolean survivingPartition = !RemoteTestModule.getMyHost().equals((String)SplitBrainBB.getLosingSideHost());
    DistributedMember dm = event.getDistributedMember();
    SplitBrainBB.updateMember(dm, survivingPartition);
  }

  /** Implementation of SystemMembershipListener, memberInfo interface
   *
   *  @param SystemMembershipEvent memberInfo event
   */
  public void memberInfo(SystemMembershipEvent event) {
    logCall("memberInfo", event);
  }

  /** Log that an event occurred.
   *
   *  @param methodName The name of the SystemMembershipEvent callback method
   *                    that was invoked.
   *  @param event The event object that was passed to the event.
   */
  private String logCall(String methodName, SystemMembershipEvent event) {
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
  private String toString(String methodName, SystemMembershipEvent event) {
     StringBuffer aStr = new StringBuffer();
     aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName() + "\n");

     if (event == null)
        return aStr.toString();

     aStr.append("   event.getDistributedMember(): " + event.getDistributedMember().toString() + "\n");
     aStr.append("   event.getMemberId(): " + event.getMemberId() + "\n");
     return(aStr.toString());
  }

  /**
   * Utility method to write an Exception string to the SplitBrain Blackboard and
   * to also throw an exception containing the same string.
   *
   * @param errStr String to log, post to SplitBrainBB and throw
   * @throws TestException containing the passed in String
   *
   * @see util.TestHelper.checkForEventError
   */
  protected void throwException(String errStr) {
        hydra.blackboard.SharedMap aMap = SplitBrainBB.getBB().getSharedMap();
        aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
        throw new TestException(errStr);
  }

}
