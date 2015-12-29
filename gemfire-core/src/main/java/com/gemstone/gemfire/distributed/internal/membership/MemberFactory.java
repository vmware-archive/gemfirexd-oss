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
package com.gemstone.gemfire.distributed.internal.membership;

import java.net.InetAddress;


import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;

/**
 * Create a new Member based on the given inputs.
 * TODO: need to implement a real factory implementation based on gemfire.properties
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 * @author D. Jason Penney
 */
public class MemberFactory {
  
  private static final MemberServices services = new JGroupMemberFactory();

  /**
   * Return a blank NetMember (used by externalization)
   * @return the new NetMember
   */
  static public NetMember newNetMember() {
    return services.newNetMember();
  }
  
  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully)
   * if there are any problems.
   * @param p the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param payload the payload for this member
   * @return the new NetMember
   */
  static public NetMember newNetMember(InetAddress i, int p,
      boolean splitBrainEnabled, boolean canBeCoordinator, MemberAttributes payload) {
    return services.newNetMember(i, p, splitBrainEnabled, canBeCoordinator, payload);
  }

  /**
   * Return a new NetMember representing current host
   * @param i an InetAddress referring to the current host
   * @param p the membership port being used
   * @return the new NetMember
   */
  static public NetMember newNetMember(InetAddress i, int p) {
    NetMember result = services.newNetMember(i, p);
    return result;
  }

  /**
   * Return a new NetMember representing current host
   * 
   * @param s a String referring to the current host
   * @param p the membership port being used
   * @return the new member
   */
  static public NetMember newNetMember(String s, int p) {
    return services.newNetMember(s, p);
  }
  
  /**
   * Create a new MembershipManager.  Be sure to send the manager a postConnect() message
   * before you start using it.
   * 
   * @param logger the logger to use
   * @param securityLogger the security logger to use
   * @param listener the listener to notify for callbacks
   * @param config the configuration of connection to distributed system
   * @param transport holds configuration information that can be used by the manager to configure itself
   * @param stats are used for recording statistical communications information
   * @return a MembershipManager
   */
  static public MembershipManager newMembershipManager(LogWriterI18n logger, 
          LogWriterI18n securityLogger,
          DistributedMembershipListener listener,
          DistributionConfig config,
          RemoteTransportConfig transport,
          DMStats stats)
  {
    return services.newMembershipManager(logger, securityLogger, listener,
        config, transport, stats);
  }
}
