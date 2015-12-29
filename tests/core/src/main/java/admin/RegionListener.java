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

import com.gemstone.gemfire.cache.*;
import hydra.blackboard.*;

/**
 * RegionRoleListener to handle region reliability membership events : 
 * maintains counts of roleLost/roleGain events.
 *
 * @author lhughes
 * @since 5.0
 *
 * @see AdminBB.roleLostEvents
 * @see AdminBB.roleGainEvents
 */
public class RegionListener extends util.AbstractListener implements RegionRoleListener, Declarable {

  /**
   * Invoked when a required role has returned to the distributed system
   * after being absent.
   *
   * @param event describes the member that fills the required role.
   */
  public void afterRoleGain(RoleEvent event) {
    logCall("afterRoleGain", event);
    SharedCounters counters = AdminBB.getInstance().getSharedCounters();
    counters.increment(AdminBB.roleGainEvents);
  }
  
  /**
   * Invoked when a required role is no longer available in the distributed
   * system.
   *
   * @param event describes the member that last filled the required role.
   */
  public void afterRoleLoss(RoleEvent event) {
    logCall("afterRoleLoss", event);
    SharedCounters counters = AdminBB.getInstance().getSharedCounters();
    counters.increment(AdminBB.roleLossEvents);
  }

  // CacheListener implmentation
  public void afterCreate(EntryEvent event) {
     logCall("afterCreate", event);
  }

  public void afterDestroy(EntryEvent event) {
     logCall("afterDestroy", event);
  }

  public void afterInvalidate(EntryEvent event) {
     logCall("afterInvalidate", event);
  }

  public void afterRegionDestroy(RegionEvent event) {
     logCall("afterRegionDestroy", event);
  }

  public void afterRegionInvalidate(RegionEvent event) {
     logCall("afterRegionInvalidate", event);
  }

  public void afterUpdate(EntryEvent event) {
     logCall("afterUpdate", event);
  }

  public void close() {
     logCall("close", null);
  }

  public void afterRegionClear(RegionEvent event) {
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate(RegionEvent event) {
    logCall("afterRegionCreate", event);
  }

  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
  }
  
public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}


