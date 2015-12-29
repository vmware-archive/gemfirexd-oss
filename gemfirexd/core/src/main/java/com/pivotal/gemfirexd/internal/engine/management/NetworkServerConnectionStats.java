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

import java.beans.ConstructorProperties;
import java.io.Serializable;

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
public final class NetworkServerConnectionStats implements Serializable { // TODO - Abhishek Remove Serializable
  private static final long serialVersionUID = 109393891063677277L;

  private String connectionStatsType; // shouldn't change
  private long connectionsOpened;
  private long connectionsClosed;
  private long connectionsAttempted;
  private long connectionsFailed;
  private long connectionLifeTime; // is this total TTL??
  private long connectionsOpen;
  private long connectionsIdle;

  @ConstructorProperties(value = { "connectionStatsType",
      "connectionsOpened", "connectionsClosed",
      "connectionsAttempted", "connectionsFailed",
      "connectionLifeTime", "connectionsOpen", "connectionsIdle" })
  public NetworkServerConnectionStats(String connectionStatsType,
      long connectionsOpened, long connectionsClosed,
      long connectionsAttempted, long connectionsFailed,
      long connectionLifeTime, long connectionsOpen, long connectionsIdle) {
    this.connectionStatsType  = connectionStatsType;
    this.connectionsOpened    = connectionsOpened;
    this.connectionsClosed    = connectionsClosed;
    this.connectionsAttempted = connectionsAttempted;
    this.connectionsFailed    = connectionsFailed;
    this.connectionLifeTime   = connectionLifeTime;
    this.connectionsOpen = connectionsOpen;
    this.connectionsIdle    = connectionsIdle;
  }

  /**
   * @return the connectionStatsType
   */
  public String getConnectionStatsType() {
    return connectionStatsType;
  }

  /**
   * @return the connectionsOpened
   */
  public long getConnectionsOpened() {
    return connectionsOpened;
  }

  /**
   * @return the connectionsClosed
   */
  public long getConnectionsClosed() {
    return connectionsClosed;
  }

  /**
   * @return the connectionsAttempted
   */
  public long getConnectionsAttempted() {
    return connectionsAttempted;
  }

  /**
   * @return the connectionsFailed
   */
  public long getConnectionsFailed() {
    return connectionsFailed;
  }

  /**
   * @return the connectionLifeTime
   */
  public long getConnectionLifeTime() {
    return connectionLifeTime;
  }

  /**
   * @return the connectionsOpen
   */
  public long getConnectionsOpen() {
    return connectionsOpen;
  }

  /**
   * @return the connectionsIdle
   */
  public long getConnectionsIdle() {
    return connectionsIdle;
  }

  public void updateNetworkServerConnectionStats(
      long connectionsOpened, long connectionsClosed,
      long connectionsAttempts, long connectionsFailures,
      long connectionLifeTime, long connectionsOpen, long connectionsIdle) {
    this.connectionsOpened    = connectionsOpened;
    this.connectionsClosed    = connectionsClosed;
    this.connectionsAttempted = connectionsAttempts;
    this.connectionsFailed    = connectionsFailures;
    this.connectionLifeTime   = connectionLifeTime;
    this.connectionsOpen      = connectionsOpen;
    this.connectionsIdle    = connectionsIdle;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(NetworkServerConnectionStats.class.getSimpleName());
    builder.append("[connectionStatsType=").append(connectionStatsType);
    builder.append(", connectionsOpened=").append(connectionsOpened);
    builder.append(", connectionsClosed=").append(connectionsClosed);
    builder.append(", connectionsAttempted=").append(connectionsAttempted);
    builder.append(", connectionsFailed=").append(connectionsFailed);
    builder.append(", connectionsTTL=").append(connectionLifeTime);
    builder.append(", connectionsOpen=").append(connectionsOpen);
    builder.append(", connectionsIdle=").append(connectionsIdle);
    builder.append("]");
    return builder.toString();
  }
}