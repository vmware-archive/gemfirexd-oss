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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Class <code>GatewayHubStatus</code> provides information about
 * <code>GatewayHub</code>s. This class is used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 4.3
 */
public class GatewayHubStatus implements Serializable {
  private static final long serialVersionUID = -4925249622141335103L;
  protected String _id;
  protected int _port;
  protected boolean _isPrimary;
  protected GatewayStatus[] _gatewayStatuses;

  public GatewayHubStatus(GatewayHub hub) {
    setId(hub.getId());
    setPort(hub.getPort());
    setIsPrimary(hub.isPrimary());
    initializeGatewayStatuses(hub);
  }

  public String getId() {
    return this._id;
  }

  protected void setId(String id) {
    this._id = id;
  }

  public int getPort() {
    return this._port;
  }

  protected void setPort(int port) {
    this._port = port;
  }

  public boolean getIsPrimary() {
    return this._isPrimary;
  }

  protected void setIsPrimary(boolean isPrimary) {
    this._isPrimary = isPrimary;
  }

  public GatewayStatus[] getGatewayStatuses() {
    return this._gatewayStatuses;
  }

  protected void initializeGatewayStatuses(GatewayHub hub) {
    List gateways = hub.getGateways();
    this._gatewayStatuses = new GatewayStatus[gateways.size()];
    int i = 0;
    for (Iterator gatewaysIterator = gateways.iterator(); gatewaysIterator.hasNext(); i++) {
      Gateway gateway = (Gateway) gatewaysIterator.next();
      this._gatewayStatuses[i] = new GatewayStatus(gateway);
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("GatewayHubStatus[")
      .append("id=")
      .append(this._id)
      .append("; port=")
      .append(this._port)
      .append("; isPrimary=")
      .append(this._isPrimary);
      if (this._gatewayStatuses == null) {
        buffer.append("; gatewayStatus = null"); 
      }
      else {
        buffer.append("; gatewayStatuses (" + this._gatewayStatuses.length + ") = [");
        for (int i = 0; i < this._gatewayStatuses.length; i ++) {
          buffer.append(this._gatewayStatuses[i].toString());
          if (i != this._gatewayStatuses.length - 1) {
            buffer.append(", ");
          }
        }
        buffer.append("]");
      }
      buffer.append("]");
    return buffer.toString();
  }
}
