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
package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.IOException;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * A handler which responds to messages for the {@link TcpServer}
 * @author dsmith
 * @since 5.7
 */
public interface TcpHandler {
  /**
   * Process a request and return a response
   * @param request
   * @return the response, or null if there is no reponse
   * @throws IOException
   */
  Object processRequest(Object request) throws IOException;
  
  void endRequest(Object request,long startTime);
  
  void endResponse(Object request,long startTime);
  
  /**
   * Perform any shutdown code in the handler after the TCP server
   * has closed the socket.
   */
  void shutDown();
  
  /**
   * Informs the handler that TcpServer is restarting with the given
   * distributed system and cache
   */
  void restarting(DistributedSystem ds, GemFireCache cache);
  
  /**
   * Initialize the handler with the TcpServer. Called before the TcpServer
   * starts accepting connections.
   */
  void init(TcpServer tcpServer);
}
