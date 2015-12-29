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

import hydra.blackboard.*;

import com.gemstone.gemfire.admin.*;

public class AdminSystemMembershipListener extends AbstractListener implements SystemMembershipListener {

  /** Implementation of SystemMembershipListener, memberJoined interface
   *
   *  @param SystemMembershipEvent memberJoined event
   */
  public void memberJoined(SystemMembershipEvent event) {

    logCall("memberJoined", event);

    // update counters (joinedNotifications)
    SharedCounters sc = AdminBB.getInstance().getSharedCounters();
    // Don't count startup 'join' messages
    if (sc.read( AdminBB.recycleRequests ) > 0) {
      sc.increment( AdminBB.joinedNotifications );
    }
    AdminBB.getInstance().printSharedCounters();
  }

  /** Implementation of SystemMembershipListener, memberLeft interface
   *
   *  @param SystemMembershipEvent memberLeft event
   */
  public void memberLeft(SystemMembershipEvent event) {
 
    logCall("memberLeft", event);

    // update counters (departedNotifications)
    SharedCounters sc = AdminBB.getInstance().getSharedCounters();
    sc.increment( AdminBB.departedNotifications );
    AdminBB.getInstance().printSharedCounters();
  }

  /** Implementation of SystemMembershipListener, memberCrashed interface
   *
   *  @param SystemMembershipEvent memberCrashed event
   */
  public void memberCrashed(SystemMembershipEvent event) {
    logCall("memberCrashed", event);
  }

  /** Implementation of SystemMembershipListener, memberInfo interface
   *
   *  @param SystemMembershipEvent memberInfo event
   */
  public void memberInfo(SystemMembershipEvent event) {
    logCall("memberInfo", event);
  }
 
}
