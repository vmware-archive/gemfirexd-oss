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
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;

public class Invalid extends BaseCommand {

  private final static Invalid singleton = new Invalid();

  public static Command getCommand() {
    return singleton;
  }

  private Invalid() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    if (logger.errorEnabled()) {
      logger.error(
        LocalizedStrings.Invalid_0_INVALID_MESSAGE_TYPE_WITH_TX_1_FROM_2,
        new Object[] {servConn.getName(), Integer.valueOf(msg.getTransactionId()), servConn.getSocketString()});
    }
    writeErrorResponse(msg, MessageType.INVALID, servConn);

  }

}
