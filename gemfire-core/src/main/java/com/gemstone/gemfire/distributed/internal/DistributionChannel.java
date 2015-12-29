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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.io.NotSerializableException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public final class DistributionChannel {

  private MembershipManager membershipManager;
  /** Set to true when this manager is being shutdown */
//  private transient volatile boolean shuttingDown = false;
//  private  Set sendJGroupsSet = null;
//  private  Set sendDirectChannelSet = null;

  private final LogWriterI18n logger;
  
  /**
   * Constructor DistributionChannel for JGroups.
   * @param channel jgroups channel
   */
  public DistributionChannel(MembershipManager channel, LogWriterI18n theLogger) {
    logger = theLogger;
    membershipManager = channel;
  }


  public InternalDistributedMember getLocalAddress() {
    return membershipManager.getLocalMember();
  }


  /**
   * @return the MembershipManager
   */
  public MembershipManager getMembershipManager() {
    return membershipManager;
  }



  /**
   * @return list of recipients who did not receive the message because
   * they left the view (null if all received it or it was sent to
   * {@link DistributionMessage#ALL_RECIPIENTS}).
   * @throws NotSerializableException
   *         If content cannot be serialized
   */
  public Set send(InternalDistributedMember[] destinations,
                  DistributionMessage content,
                  DistributionManager dm, DistributionStats stats)
  throws NotSerializableException {
    if (membershipManager == null) {
      logger.warning(LocalizedStrings.DistributionChannel_ATTEMPTING_A_SEND_TO_A_DISCONNECTED_DISTRIBUTIONMANAGER);
      if (destinations.length == 1 
          && destinations[0] == DistributionMessage.ALL_RECIPIENTS)
        return null;
      HashSet result = new HashSet();
      for (int i = 0; i < destinations.length; i ++)
        result.add(destinations[i]);
      return result;
      }
    return membershipManager.send(destinations, content, stats);
  }

  public void disconnect(boolean duringStartup)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("Disconnected from distribution channel ");

    long start = System.currentTimeMillis();

    if (membershipManager != null) {
      sb.append(membershipManager.getLocalMember());
      sb.append(" (took ");
      long begin = System.currentTimeMillis();
      if (duringStartup) {
        membershipManager.uncleanShutdown("Failed to start distribution", null);
      }
      else {
        membershipManager.shutdown();
      }
      long delta = System.currentTimeMillis() - begin;
      sb.append(delta);
      sb.append("/");
    }
    membershipManager = null;

    if (DistributionManager.VERBOSE) {
      long delta = System.currentTimeMillis() - start;
      sb.append(delta);
      sb.append(" ms)");
      logger.fine(sb.toString());
    }
  }

  /**
   * Returns the id of this distribution channel.  If this channel
   * uses JavaGroups and the conduit to communicate with others, then
   * the port of the JavaGroups channel's {@link InternalDistributedMember address} is
   * returned.
   *
   * @since 3.0
   */
  public long getId() {
    InternalDistributedMember moi = membershipManager.getLocalMember();
    if (moi == null) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DistributionChannel_I_NO_LONGER_HAVE_A_MEMBERSHIP_ID.toLocalizedString(), membershipManager.getShutdownCause());
    }
    return moi.getPort();
  }

  public void setShutDown() {
//    this.shuttingDown = shuttingDown;
    if (membershipManager != null)
      membershipManager.setShutdown();
  }

//   private void sendViaJGroups(Serializable[] destinations,Address source,Serializable content,
//                          boolean deliverToSender, int processorType,
//                          DistributionManager dm)
//   throws ChannelNotConnectedException, ChannelClosedException {
//     Message msg = new Message(null, source, content);
//     msg.setDeliverToSender(deliverToSender);
//     msg.setProcessorType(processorType);
//     for (int i=0; i < destinations.length; i++) {
//       Address destination = (Address) destinations[i];
//       msg.setDest(destination);
//       jgroupsChannel.send(msg);
//       if (DistributionManager.VERBOSE) {
//         dm.logger.info("Sending " + content + " to " + destination +
//                        " via java groups");
//       }
//       if (destination == null)
//         break;
//     }
//   }

}
