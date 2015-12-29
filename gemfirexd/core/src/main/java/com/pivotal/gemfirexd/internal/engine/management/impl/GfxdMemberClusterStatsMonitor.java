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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerNestedConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.GfxdMemberMXBean;

/**
*
* @author Ajay Pande
* @since gfxd 1.0
*/

public class GfxdMemberClusterStatsMonitor {

  private long NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN = 0L;
  private long NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE = 0L;

  private long NETWORK_SERVER_PEER_CONNECTIONS_OPENED = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_CLOSED = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_FAILED = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_OPEN = 0L;
  private long NETWORK_SERVER_PEER_CONNECTIONS_IDLE = 0L;
  
  private long NETWORK_SERVER_NESTED_CONNECTIONS_OPEN = 0L;
  private long NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED = 0L;
  private long NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE = 0L;
  
  private long NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN = 0L;
  private long NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED = 0L;
  private long NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE = 0L;
  
  private int PROCEDURE_CALLS_COMPLETED = 0;
  private int PROCEDURE_CALLS_IN_PROGRESS = 0;
  
  private NetworkServerConnectionStats networkServerClientConnectionStats = null;
  private NetworkServerConnectionStats networkServerPeerConnectionStats = null;
  private NetworkServerNestedConnectionStats networkServerNestedConnectionStats = null;
  private NetworkServerNestedConnectionStats networkServerInternalConnectionStats = null;

  
  protected LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();

  public GfxdMemberClusterStatsMonitor() {
    this.networkServerClientConnectionStats = new NetworkServerConnectionStats(
        "Client", this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE);

    this.networkServerPeerConnectionStats = new NetworkServerConnectionStats(
        "Peer", this.NETWORK_SERVER_PEER_CONNECTIONS_OPENED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_FAILED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME,
        this.NETWORK_SERVER_PEER_CONNECTIONS_OPEN,
        0);

    this.networkServerNestedConnectionStats = new NetworkServerNestedConnectionStats(
        "Nested", this.NETWORK_SERVER_NESTED_CONNECTIONS_OPEN,
        this.NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE);

    this.networkServerInternalConnectionStats = new NetworkServerNestedConnectionStats(
        "Internal", this.NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN,
        this.NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE);

  }

  
  public synchronized int getProcedureCallsCompleted() {
    update();
    return this.PROCEDURE_CALLS_COMPLETED;
  }

  public synchronized int getProcedureCallsInProgress() {
    update();
    return this.PROCEDURE_CALLS_IN_PROGRESS;
  }

  public synchronized NetworkServerConnectionStats getNetworkServerClientConnectionStats() {
    update();
    this.networkServerClientConnectionStats.updateNetworkServerConnectionStats(
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN,
        this.NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE);
    return this.networkServerClientConnectionStats;
  }

  public synchronized NetworkServerConnectionStats getNetworkServerPeerConnectionStats() {
    update();
    this.networkServerPeerConnectionStats.updateNetworkServerConnectionStats(
        this.NETWORK_SERVER_PEER_CONNECTIONS_OPENED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_FAILED,
        this.NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME,
        this.NETWORK_SERVER_PEER_CONNECTIONS_OPEN,
        0);
    return this.networkServerPeerConnectionStats;

  }

  public synchronized NetworkServerNestedConnectionStats getNetworkServerNestedConnectionStats() {
    update();
    this.networkServerNestedConnectionStats.updateNetworkServerConnectionStats(
        this.NETWORK_SERVER_NESTED_CONNECTIONS_OPEN,
        this.NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED,
        this.NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE);
    return this.networkServerNestedConnectionStats;
  }

  public synchronized NetworkServerNestedConnectionStats getNetworkServerInternalConnectionStats() {
    update();    
    this.networkServerInternalConnectionStats
        .updateNetworkServerConnectionStats(
            this.NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN,
            this.NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED,
            this.NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE);
    return this.networkServerInternalConnectionStats;
  }

  private void update() {
    assert Thread.holdsLock(this);

    Cache cache = Misc.getGemFireCacheNoThrow();
    if (cache != null) {
      Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);
      Iterator<DistributedMember> it = dsMembers.iterator();
      reset();
      while (it.hasNext()) {
        try {
          DistributedMember dsMember = it.next();
          ObjectName memberMBeanName = ManagementUtils.getMemberMBeanName( MBeanJMXAdapter.getMemberNameOrId(dsMember), "DEFAULT");
          GfxdMemberMXBean mbean = (GfxdMemberMXBean) (InternalManagementService.getAnyInstance().getMBeanInstance(memberMBeanName, GfxdMemberMXBean.class));
          if (mbean != null) {
            this.PROCEDURE_CALLS_COMPLETED += mbean.getProcedureCallsCompleted();
            this.PROCEDURE_CALLS_IN_PROGRESS += mbean.getProcedureCallsInProgress();
            NetworkServerConnectionStats clientStats = mbean.getNetworkServerClientConnectionStats();
            NetworkServerNestedConnectionStats internalStats = mbean.getNetworkServerInternalConnectionStats();
            NetworkServerNestedConnectionStats nestedStats = mbean.getNetworkServerNestedConnectionStats();
            NetworkServerConnectionStats peerStats = mbean.getNetworkServerPeerConnectionStats();

            // aggregate client stats
            if (clientStats != null) {
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED += clientStats.getConnectionsOpened();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED += clientStats.getConnectionsClosed();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED += clientStats.getConnectionsAttempted();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED += clientStats.getConnectionsFailed();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME += clientStats.getConnectionLifeTime();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN += clientStats.getConnectionsOpen();
              this.NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE += clientStats.getConnectionsIdle();
            }

            // aggregate peer stats
            if (peerStats != null) {
              this.NETWORK_SERVER_PEER_CONNECTIONS_OPENED += peerStats.getConnectionsOpened();
              this.NETWORK_SERVER_PEER_CONNECTIONS_CLOSED += peerStats.getConnectionsClosed();
              this.NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED += peerStats.getConnectionsAttempted();
              this.NETWORK_SERVER_PEER_CONNECTIONS_FAILED += peerStats.getConnectionsFailed();
              this.NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME += peerStats.getConnectionLifeTime();
              this.NETWORK_SERVER_PEER_CONNECTIONS_OPEN += peerStats.getConnectionsOpen();
              this.NETWORK_SERVER_PEER_CONNECTIONS_IDLE += peerStats.getConnectionsIdle();
            }

            // aggregate nested stats
            if (nestedStats != null) {
              this.NETWORK_SERVER_NESTED_CONNECTIONS_OPEN += nestedStats.getConnectionsOpened();
              this.NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED += nestedStats.getConnectionsClosed();
              this.NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE += nestedStats.getConnectionsActive();
            }

            // aggregate internal stats
            if (internalStats != null) {
              this.NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN += internalStats.getConnectionsOpened();
              this.NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED += internalStats.getConnectionsClosed();
              this.NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE += internalStats.getConnectionsActive();
            }
          }
        } catch (Exception ex) {
          if (logger.fineEnabled()) {
            logger.fine("Exception while aggregating member : " + ex.getMessage());
            logger.fine("Cause : " + ex.getCause());
          }
          
          //continue aggregation for other members
          continue;
        }
      }
    }
  }
  void reset(){
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED = 0;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED = 0;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED = 0;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED = 0;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME = 0;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN = 0 ;
    this.NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE = 0;

    this.NETWORK_SERVER_PEER_CONNECTIONS_OPENED = 0;
    this.NETWORK_SERVER_PEER_CONNECTIONS_CLOSED = 0;
    this.NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED = 0 ;
    this.NETWORK_SERVER_PEER_CONNECTIONS_FAILED = 0 ;
    this.NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME = 0 ;
    this.NETWORK_SERVER_PEER_CONNECTIONS_OPEN = 0 ;    
    this.NETWORK_SERVER_PEER_CONNECTIONS_IDLE = 0 ;
    
    this.NETWORK_SERVER_NESTED_CONNECTIONS_OPEN = 0;
    this.NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED = 0;
    this.NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE = 0;
    
    this.NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN = 0;
    this.NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED = 0;
    this.NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE = 0;
    
    this.PROCEDURE_CALLS_COMPLETED = 0;
    this.PROCEDURE_CALLS_IN_PROGRESS = 0;
    
  }

}
