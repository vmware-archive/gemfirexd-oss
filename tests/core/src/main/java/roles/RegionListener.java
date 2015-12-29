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
package roles;

import com.gemstone.gemfire.cache.*;

import hydra.*;

/**
 * RegionRoleListener to process region reliability membership events : 
 * (currently it simply logs the events)
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

    // maintain expected counts for ENDTASK validation
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.actualRoleGainEvents);
    Log.getLogWriter().info("After incrementing counter, actualRoleGainEvents = " + count);

    // update the BB SharedMap (RegionAvailable_clientX) entry to show that
    // this region is currently available
    String key = RolesBB.RegionAvailablePrefix + System.getProperty( "clientName" );
    RolesBB.getBB().getSharedMap().put(key, new Boolean(true));
    RolesBB.getBB().printSharedMap();
  }
  
  /**
   * Invoked when a required role is no longer available in the distributed
   * system.
   *
   * @param event describes the member that last filled the required role.
   */
  public void afterRoleLoss(RoleEvent event) {
    logCall("afterRoleLoss", event);

    // update expected counts for ENDTASK validation
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.actualRoleLossEvents);
    Log.getLogWriter().info("After incrementing counter, actualRoleLossEvents = " + count);

    // update the BB SharedMap (RegionAvailable_clientX) entry to show that
    // this region is not available due to role loss
    String key = RolesBB.RegionAvailablePrefix + System.getProperty( "clientName" );
    RolesBB.getBB().getSharedMap().put(key, new Boolean(false));
    RolesBB.getBB().printSharedMap();
  }

  // CacheListener implmentation
  public void afterCreate(EntryEvent event) {
     logCall("afterCreate", event);
     countQueuedEvents(event);
  }

  public void afterDestroy(EntryEvent event) {
     logCall("afterDestroy", event);
     countQueuedEvents(event);
  }

  public void afterInvalidate(EntryEvent event) {
     logCall("afterInvalidate", event);
     countQueuedEvents(event);
  }

  public void afterRegionDestroy(RegionEvent event) {
     logCall("afterRegionDestroy", event);
     countQueuedEvents(event);
  }

  public void afterRegionInvalidate(RegionEvent event) {
     logCall("afterRegionInvalidate", event);
     countQueuedEvents(event);
  }

  public void afterUpdate(EntryEvent event) {
     logCall("afterUpdate", event);
     countQueuedEvents(event);
  }

  public void close() {
     logCall("close", null);
  }

  public void afterRegionClear(RegionEvent event) {
     logCall("afterRegionClear", event);
     countQueuedEvents(event);
  }

  public void afterRegionCreate(RegionEvent event) {
     logCall("afterRegionCreate", event);
     countQueuedEvents(event);
  }

  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
    countQueuedEvents(event);
  }
  
  private void countQueuedEvents(CacheEvent event) {
  }

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}


