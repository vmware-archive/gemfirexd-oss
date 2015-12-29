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

package com.pivotal.gemfirexd.callbacks.impl;

/**
 * GatewayConflictResolver is a cluster-wide plugin that is called upon to decide what to do
 * with events that originate in other systems and arrive through the WAN Gateway.  A
 * GatewayConflictResolver is invoked if the current row was established by a
 * different distributed system (with a different distributed-system-id) than an event
 * that is attempting to modify the row.  It is not invoked if the event has the same
 * distributed system ID as the event that last changed the row.
 * 
 * @author sjigyasu
 *
 */
public interface GatewayConflictResolver {

  /**
   * Initializes the resolver
   * @param params
   */
  public void init(String params);

  /**
   * This method is invoked when a change is received from another distributed system and
   * the last modification to the affected row did not also come from the same system.
   * <p>
   * The given GatewayConflictHelper can be used to allow the change to be made to the site,
   * disallow the modification or make a change to the value to be stored.
   * <p>For any two events, all GatewayConflictResolvers must make the same decision
   * on the resolution of the conflict in order to maintain consistency.  They must
   * do so regardless of the order of the events.</p>
   * @param event the event that is in conflict with the current row
   * @param helper an object to be used in modifying the course of action for this event
   */
  public void onEvent(GatewayEvent event, GatewayConflictHelper helper);

}
