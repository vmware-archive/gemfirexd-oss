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
public final class NetworkServerNestedConnectionStats implements Serializable { // TODO - Abhishek Remove Serializable
  private static final long serialVersionUID = -788587782999937336L;

  private final String connectionStatsType; // shouldn't change
  private long connectionsOpened;
  private long connectionsClosed;
  private long connectionsActive;

  @ConstructorProperties(value = { "connectionStatsType",
      "connectionsOpened", "connectionsClosed",
      "connectionsActive"})
  public NetworkServerNestedConnectionStats(String connectionStatsType,
      long connectionsOpen, long connectionsClosed,
      long connectionsActive) {
    this.connectionStatsType = connectionStatsType;
    this.connectionsOpened   = connectionsOpen;
    this.connectionsClosed   = connectionsClosed;
    this.connectionsActive   = connectionsActive;
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
  public long getConnectionsActive() {
    return connectionsActive;
  }

  public void updateNetworkServerConnectionStats(
      long connectionsOpen, long connectionsClosed,
      long connectionsActive) {
    this.connectionsOpened = connectionsOpen;
    this.connectionsClosed = connectionsClosed;
    this.connectionsActive = connectionsActive;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(NetworkServerNestedConnectionStats.class.getSimpleName());
    builder.append("[connectionStatsType=").append(connectionStatsType);
    builder.append(", connectionsOpened=").append(connectionsOpened);
    builder.append(", connectionsClosed=").append(connectionsClosed);
    builder.append(", connectionsActive=").append(connectionsActive);
    builder.append("]");
    return builder.toString();
  }
}