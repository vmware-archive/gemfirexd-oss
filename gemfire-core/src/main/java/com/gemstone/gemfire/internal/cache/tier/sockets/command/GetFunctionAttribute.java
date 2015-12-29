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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class GetFunctionAttribute extends BaseCommand {

  private final static GetFunctionAttribute singleton = new GetFunctionAttribute();

  public static Command getCommand() {
    return singleton;
  }

  private GetFunctionAttribute() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    String functionId = msg.getPart(0).getString();
    if (functionId == null) {
      String message = LocalizedStrings.GetFunctionAttribute_THE_INPUT_0_FOR_GET_FUNCTION_ATTRIBUTE_REQUEST_IS_NULL
          .toLocalizedString("functionId");
      if (logger.warningEnabled()) {
        logger.warning(LocalizedStrings.ONE_ARG, servConn.getName() + ": "
            + message);
      }
      sendError(msg, message, servConn);
      return;
    }
    else {
      Function function = FunctionService.getFunction(functionId);
      if (function == null) {
        String message = null;
        message = LocalizedStrings.GetFunctionAttribute_THE_FUNCTION_IS_NOT_REGISTERED_FOR_FUNCTION_ID_0
            .toLocalizedString(functionId);
        if (logger.warningEnabled()) {
          logger.warning(LocalizedStrings.ONE_ARG, servConn.getName() + ": "
              + message);
        }
        sendError(msg, message, servConn);
        return;
      }
      else {
        byte[] functionAttributes = new byte[3];
        functionAttributes[0] = (byte)(function.hasResult() ? 1 : 0);
        functionAttributes[1] = (byte)(function.isHA() ? 1 : 0);
        functionAttributes[2] = (byte)(function.optimizeForWrite() ? 1 : 0);
        writeResponseWithFunctionAttribute(functionAttributes, msg, servConn);
      }
    }
  }

  private void sendError(Message msg, String message, ServerConnection servConn)
      throws IOException {
    synchronized (msg) {
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
