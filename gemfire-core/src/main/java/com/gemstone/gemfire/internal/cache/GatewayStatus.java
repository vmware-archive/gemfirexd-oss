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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Class <code>GatewayStatus</code> provides information about
 * <code>Gateway</code>s. This class is used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 4.3
 */
public class GatewayStatus implements Serializable {
  private static final long serialVersionUID = -6151097585068547412L;
  protected String _id;
  protected boolean _isConnected;
  protected int _queueSize;
  protected GatewayEndpointStatus[] _endpointStatuses;

  public GatewayStatus(Gateway gateway) {
    setId(gateway.getId());
    setIsConnected(gateway.isConnected());
    setQueueSize(gateway.getQueueSize());
    initializeEndpointStatuses(gateway);
  }

  public String getId() {
    return this._id;
  }

  protected void setId(String id) {
    this._id = id;
  }

  public boolean getIsConnected() {
    return this._isConnected;
  }

  protected void setIsConnected(boolean isConnected) {
    this._isConnected = isConnected;
  }

  public int getQueueSize() {
    return this._queueSize;
  }

  protected void setQueueSize(int queueSize) {
    this._queueSize = queueSize;
  }

  public GatewayEndpointStatus[] getEndpointStatuses() {
    return this._endpointStatuses;
  }

  protected void initializeEndpointStatuses(Gateway gateway) {
    List endpoints = gateway.getEndpoints();
    this._endpointStatuses = new GatewayEndpointStatus[endpoints.size()];
    int i = 0;
    for (Iterator endpointsIterator = endpoints.iterator(); endpointsIterator.hasNext(); i++) {
      Gateway.Endpoint endpoint = (Gateway.Endpoint) endpointsIterator.next();
      this._endpointStatuses[i] = new GatewayEndpointStatus(endpoint);
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("GatewayStatus[")
      .append("id=")
      .append(this._id)
      .append("; isConnected=")
      .append(this._isConnected)
      .append("; queueSize=")
      .append(this._queueSize);
      if (this._endpointStatuses == null) {
        buffer.append("; endpoints = null");
      }
      else {
        buffer.append("; endpoints (" + this._endpointStatuses.length +") = [");
        for (int i = 0; i < this._endpointStatuses.length; i ++) {
          buffer.append(this._endpointStatuses[i].toString());
          if (i != this._endpointStatuses.length - 1) {
            buffer.append(", ");
          }
        }
        buffer.append("]");
      }
      buffer.append("]");
    return buffer.toString();
  }
}
