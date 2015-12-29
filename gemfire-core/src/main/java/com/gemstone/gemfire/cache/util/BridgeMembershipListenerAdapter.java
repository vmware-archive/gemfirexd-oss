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

package com.gemstone.gemfire.cache.util;

/**
 * Utility class that implements all methods in 
 * <code>BridgeMembershipListener</code> with empty implementations.
 * Applications can subclass this class and only override the methods for
 * the events of interest.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public abstract class BridgeMembershipListenerAdapter 
implements BridgeMembershipListener {
    
  /**
   * Invoked when a client has connected to this process or when this
   * process has connected to a BridgeServer.
   */
  public void memberJoined(BridgeMembershipEvent event) {}

  /**
   * Invoked when a client has gracefully disconnected from this process
   * or when this process has gracefully disconnected from a BridgeServer.
   */
  public void memberLeft(BridgeMembershipEvent event) {}

  /**
   * Invoked when a client has unexpectedly disconnected from this process
   * or when this process has unexpectedly disconnected from a BridgeServer.
   */
   public void memberCrashed(BridgeMembershipEvent event) {}
  
}

