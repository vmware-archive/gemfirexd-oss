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


import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import java.io.IOException;


public class MakePrimary extends BaseCommand {

  private final static MakePrimary singleton = new MakePrimary();

  public static Command getCommand() {
    return singleton;
  }

  private MakePrimary() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    Part isClientReadyPart = msg.getPart(0);
    byte[] isClientReadyPartBytes = (byte[])isClientReadyPart.getObject();
    boolean isClientReady = isClientReadyPartBytes[0] == 0x01;
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received make primary request ("
          + msg.getPayloadLength() + " bytes) isClientReady="
          + isClientReady + ": from " + servConn.getSocketString());
    }
    try {
      servConn.getAcceptor().getCacheClientNotifier()
      .makePrimary(servConn.getProxyID(), isClientReady);
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);

      if (logger.fineEnabled()) {
        logger.fine(servConn.getName() + ": Sent make primary response for"
            + servConn.getSocketString());
      }
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
