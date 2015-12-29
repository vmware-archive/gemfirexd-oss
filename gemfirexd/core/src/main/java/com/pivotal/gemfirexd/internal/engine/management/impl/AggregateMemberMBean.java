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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import com.pivotal.gemfirexd.internal.engine.management.AggregateMemberMXBean;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerNestedConnectionStats;

/**
 * 
 * @author Ajay Pande
 * @since gfxd 1.0
 */

public class AggregateMemberMBean implements AggregateMemberMXBean {

  private GfxdDistributedSystemBridge bridge = null;

  public AggregateMemberMBean(
      GfxdDistributedSystemBridge gfxdDistributedSystemBridge) {
    this.bridge = gfxdDistributedSystemBridge;
  }

  @Override
  public String[] getMembers() {
    return bridge.getMembers();
  }

  @Override
  public void enableStatementStats(boolean enableOrDisableStats,
      boolean enableOrDisableTimeStats) {
    bridge.enableStatementStats(enableOrDisableStats, enableOrDisableTimeStats);
  }

  @Override
  public int getProcedureCallsCompleted() {
    return bridge.getProcedureCallsCompleted();
  }

  @Override
  public NetworkServerConnectionStats getNetworkServerClientConnectionStats() {
    return bridge.getNetworkServerClientConnectionStats();
  }
  
  @Override
  public NetworkServerConnectionStats getNetworkServerPeerConnectionStats() {
    return bridge.getNetworkServerPeerConnectionStats();
  }
  
  @Override
  public NetworkServerNestedConnectionStats getNetworkServerNestedConnectionStats(){
    return bridge.getNetworkServerNestedConnectionStats();
  }
  
  @Override
  public NetworkServerNestedConnectionStats getNetworkServerInternalConnectionStats(){
    return bridge.getNetworkServerInternalConnectionStats();
  }

  @Override
  public int getProcedureCallsInProgress() {
    return bridge.getProcedureCallsInProgress();
  }

}
