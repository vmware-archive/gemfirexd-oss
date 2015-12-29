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
package com.gemstone.gemfire.distributed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Interface <code>ClientSocketFactory</code> is used to create non-default
 * client sockets.  Set the system property gemfire.clientSocketFactory to the
 * full name of your factory implementation, and GemFire will use your
 * factory to manufacture sockets when it connects to server caches.
 * 
 * @author Barry Oglesby
 * 
 * @since 6.5
 * 
 */
public interface ClientSocketFactory {

  /**
   * Creates a <code>Socket</code> for the input address and port
   * 
   * @param address
   *          The <code>InetAddress</code> of the server
   * @param port
   *          The port of the server
   * 
   * @return a <code>Socket</code> for the input address and port
   */
  public Socket createSocket(InetAddress address, int port) throws IOException;
}
