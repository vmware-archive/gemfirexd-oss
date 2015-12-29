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

import com.gemstone.gemfire.cache.util.Gateway.Endpoint;

import java.io.Serializable;

/**
 * Class <code>GatewayEndpointStatus</code> provides information about
 * <code>GatewayEndpoint</code>s. This class is used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 4.3
 */
public class GatewayEndpointStatus implements Serializable {
  private static final long serialVersionUID = 4123544515359769808L;
  protected String _id;
  protected String _host;
  protected int _port;

  public GatewayEndpointStatus(Endpoint endpoint) {
    setId(endpoint.getId());
    setHost(endpoint.getHost());
    setPort(endpoint.getPort());
  }

  public String getId() {
    return this._id;
  }

  protected void setId(String id) {
    this._id = id;
  }

  public String getHost() {
    return this._host;
  }

  protected void setHost(String host) {
    this._host = host;
  }

  public int getPort() {
    return this._port;
  }

  protected void setPort(int port) {
    this._port = port;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("GatewayEndpointStatus[")
      .append("id=")
      .append(this._id)
      .append("host=")
      .append(this._host)
      .append("port=")
      .append(this._port)
      .append("]");
    return buffer.toString();
  }
}
