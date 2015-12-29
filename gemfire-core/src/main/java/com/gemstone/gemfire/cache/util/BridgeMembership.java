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

import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;

/**
 * Provides utility methods for registering and unregistering
 * BridgeMembershipListeners in this process.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public final class BridgeMembership {

  private BridgeMembership() {}

  /**
   * Registers a {@link BridgeMembershipListener} for notification of
   * connection changes for BridgeServers and bridge clients.
   * @param listener a BridgeMembershipListener to be registered
   */
  public static void registerBridgeMembershipListener(BridgeMembershipListener listener) {
    InternalBridgeMembership.registerBridgeMembershipListener(listener);
  }

  /**
   * Removes registration of a previously registered {@link
   * BridgeMembershipListener}.
   * @param listener a BridgeMembershipListener to be unregistered
   */
  public static void unregisterBridgeMembershipListener(BridgeMembershipListener listener) {
    InternalBridgeMembership.unregisterBridgeMembershipListener(listener);
  }

  /**
   * Returns an array of all the currently registered
   * <code>BridgeMembershipListener</code>s. Modifications to the returned
   * array will not effect the registration of these listeners.
   * @return the registered <code>BridgeMembershipListener</code>s; an empty
   * array if no listeners
   */
  public static BridgeMembershipListener[] getBridgeMembershipListeners() {
    return InternalBridgeMembership.getBridgeMembershipListeners();
  }


}

