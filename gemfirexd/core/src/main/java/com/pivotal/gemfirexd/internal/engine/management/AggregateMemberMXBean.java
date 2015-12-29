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
package com.pivotal.gemfirexd.internal.engine.management;

/**
 * 
 * @author Ajay Pande
 * @since gfxd 1.0
 */

public interface AggregateMemberMXBean {
  /**
   * member ids / names
   * 
   */
  String[] getMembers();

  /**
   * Enables or disables stats. The first parameter enables/disables stats and
   * second one enables/disables Time stats.
   */
  void enableStatementStats(boolean enableOrDisableStats,
      boolean enableOrDisableTimeStats);

  /**
   * Aggregated procedures calls completed
   */
  int getProcedureCallsCompleted();

  /**
   * Aggregated procedures calls in progress
   */
  int getProcedureCallsInProgress();

  /**
   * Represents aggregation of network server client connection stats. It has
   * attributes e.g. connection type (connectionStatsType), opened connections
   * (connectionsOpened), closed connections (connectionsClosed), connections
   * attempted (connectionsAttempted ), Failed connections (connectionsFailed ),
   * connections life time (connectionLifeTime), open connections
   * (connectionsOpen) and idle connections (connectionsIdle)
   */
  NetworkServerConnectionStats getNetworkServerClientConnectionStats();

  /**
   * Represents aggregation of network server peer connection stats. It has
   * attributes e.g. connection type (connectionStatsType), open connections
   * (connectionsOpened), closed connections (connectionsClosed), connections
   * attempted (connectionsAttempted ), Failed connections (connectionsFailed ),
   * connections life time (connectionLifeTime) and active connections
   * (connectionsActive)
   */
  NetworkServerConnectionStats getNetworkServerPeerConnectionStats();

  /**
   * Represents aggregation of network server internal connection stats. It has
   * attributes e.g. connection type (connectionStatsType), open connections
   * (connectionsOpened), closed connections (connectionsClosed), active
   * connections (connectionsActive)
   */
  NetworkServerNestedConnectionStats getNetworkServerInternalConnectionStats();

  /**
   * Represents aggregation of network server nested connection stats. It has
   * attributes e.g. connection type (connectionStatsType), open connections
   * (connectionsOpened), closed connections (connectionsClosed), active
   * connections (connectionsActive)
   */
  NetworkServerNestedConnectionStats getNetworkServerNestedConnectionStats();
}
