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
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class TransportFilterServerSocket extends ServerSocket {
  
  private List<GatewayTransportFilter> gatewayTransportFilters;
  
  public TransportFilterServerSocket(List<GatewayTransportFilter> transportFilters) throws IOException {
    super();
    this.gatewayTransportFilters = transportFilters;
  }

  public Socket accept() throws IOException {
    Socket s = new TransportFilterSocket(this.gatewayTransportFilters);
    implAccept(s);
    return s;
  }
}
