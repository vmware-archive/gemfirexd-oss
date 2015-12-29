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

import java.io.IOException;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;


public class GetPDXIdForEnum extends BaseCommand {

  private final static GetPDXIdForEnum singleton = new GetPDXIdForEnum();

  public static Command getCommand() {
    return singleton;
  }

  private GetPDXIdForEnum() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName()
          + ": Received get pdx id for enum request ("
          + msg.getNumberOfParts() + " parts) from "
          + servConn.getSocketString());
    }

    EnumInfo enumInfo = (EnumInfo) msg.getPart(0).getObject();
    
    int enumId;
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl) servConn.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      enumId = registry.defineEnum(enumInfo);
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(msg.getTransactionId());
    responseMsg.addIntPart(enumId);
    responseMsg.send(servConn, logger, msg.getTransactionId());
    servConn.setAsTrue(RESPONDED);
  }
}
