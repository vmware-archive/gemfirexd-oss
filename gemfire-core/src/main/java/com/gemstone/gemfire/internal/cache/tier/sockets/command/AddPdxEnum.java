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
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;


public class AddPdxEnum extends BaseCommand {

  private final static AddPdxEnum singleton = new AddPdxEnum();

  public static Command getCommand() {
    return singleton;
  }

  private AddPdxEnum() {
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
    int noOfParts = msg.getNumberOfParts();

    EnumInfo enumInfo = (EnumInfo) msg.getPart(0).getObject();
    int enumId = msg.getPart(1).getInt();
    
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl) servConn.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      registry.addRemoteEnum(enumId, enumInfo);
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
  }
}
