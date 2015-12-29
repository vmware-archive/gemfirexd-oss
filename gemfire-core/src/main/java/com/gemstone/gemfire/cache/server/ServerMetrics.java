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
package com.gemstone.gemfire.cache.server;


/**
 * Metrics about the resource usage for a bridge server.
 * These metrics are provided to the {@link ServerLoadProbe} for
 * use in calculating the load on the server.
 * @author dsmith
 * @since 5.7
 *
 */
public interface ServerMetrics {
  /**
   * Get the number of open connections
   * for this bridge server.
   */
  int getConnectionCount();
  
  /** Get the number of clients connected to this
   * bridge server.
   */ 
  int getClientCount();
  
  /**
   * Get the number of client subscription connections hosted on this
   * bridge server.
   */
  int getSubscriptionConnectionCount();
  
  /**
   * Get the max connections for this bridge server.
   */
  int getMaxConnections();
  
  //TODO grid - Queue sizes, server group counts,
  //CPU Usage, stats, etc.
}
