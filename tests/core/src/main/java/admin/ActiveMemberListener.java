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
import hydra.blackboard.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.*;

public class ActiveMemberListener extends AbstractListener implements SystemMembershipListener {

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

    // update sharedMap (active members)
    List activeMembers = (List)AdminBB.getInstance().getSharedMap().get(AdminBB.activeMembers);
    DistributedMember dm = event.getDistributedMember();
    if (activeMembers.contains(dm)) {
      throwException("BUG 34517?  Received memberJoin event, but member " + dm.toString() + " is already in active members list: " + activeMembers);
    }

    activeMembers.add(dm);
    Log.getLogWriter().info("Putting updated list of active members to BB: " + activeMembers);
    AdminBB.getInstance().getSharedMap().put(AdminBB.activeMembers, activeMembers);
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

    // update sharedMap (active Members)
    List activeMembers = (List)AdminBB.getInstance().getSharedMap().get(AdminBB.activeMembers);
    DistributedMember dm = event.getDistributedMember();
    if (!activeMembers.contains(dm)) {
      throwException("Received memberLeft event, but member " + dm.toString() + " was no longer in active members list: " + activeMembers);
    }

    activeMembers.remove(dm);
    Log.getLogWriter().info("Putting updated list of active members to BB: " + activeMembers);
    AdminBB.getInstance().getSharedMap().put(AdminBB.activeMembers, activeMembers);
  }

  /** Implementation of SystemMembershipListener, memberCrashed interface
   *
   *  @param SystemMembershipEvent memberCrashed event
   */
  public void memberCrashed(SystemMembershipEvent event) {
    logCall("memberCrashed", event);

    // update sharedMap (active Members)
    List activeMembers = (List)AdminBB.getInstance().getSharedMap().get(AdminBB.activeMembers);
    DistributedMember dm = event.getDistributedMember();
    if (!activeMembers.contains(dm)) {
      throwException("Received memberCrashed event, but member " + dm.toString() + " was no longer in active members list: " + activeMembers);
    }
    activeMembers.remove(dm);
    Log.getLogWriter().info("Putting updated list of active members to BB: " + activeMembers);
    AdminBB.getInstance().getSharedMap().put(AdminBB.activeMembers, activeMembers);
  }

  /** Implementation of SystemMembershipListener, memberInfo interface
   *
   *  @param SystemMembershipEvent memberInfo event
   */
  public void memberInfo(SystemMembershipEvent event) {
    logCall("memberInfo", event);
  }
 
}
