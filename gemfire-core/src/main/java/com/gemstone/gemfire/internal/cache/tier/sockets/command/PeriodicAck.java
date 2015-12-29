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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import java.io.IOException;

public class PeriodicAck extends BaseCommand {

  private final static PeriodicAck singleton = new PeriodicAck();

  public static Command getCommand() {
    return singleton;
  }

  private PeriodicAck() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received periodic ack request ("
          + msg.getPayloadLength() + " bytes) from "
          + servConn.getSocketString());
    }
    try {
      int numEvents = msg.getNumberOfParts();
      boolean success = false;        
      CacheClientNotifier ccn = servConn.getAcceptor().getCacheClientNotifier();
      CacheClientProxy proxy = ccn.getClientProxy(servConn.getProxyID());
      if (proxy != null) {
        proxy.getHARegionQueue().createAckedEventsMap();
        for (int i = 0; i < numEvents; i++) {
          EventID eid = (EventID)msg.getPart(i).getObject();
          success = ccn.processDispatchedMessage(servConn.getProxyID(), eid);
          if (!success)
            break;
        }
      }
      if (success) {
        proxy.getHARegionQueue().setAckedEvents();
        writeReply(msg, servConn);
        servConn.setAsTrue(RESPONDED);
      }

    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }

    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Sent periodic ack response for"
          + servConn.getSocketString());
    }

  }

}
