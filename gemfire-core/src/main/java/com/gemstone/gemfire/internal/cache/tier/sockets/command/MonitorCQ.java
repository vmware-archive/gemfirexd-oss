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


import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import java.io.IOException;

public class MonitorCQ extends BaseCommand {

  private final static MonitorCQ singleton = new MonitorCQ();

  public static Command getCommand() {
    return singleton;
  }

  private MonitorCQ() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    int op = msg.getPart(0).getInt();

    if (op < 1) {
      // This should have been taken care at the client - remove?
      String err = LocalizedStrings.MonitorCQ__0_THE_MONITORCQ_OPERATION_IS_INVALID.toLocalizedString(servConn.getName());
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
      return;
    }

    String regionName = null;
    if (msg.getNumberOfParts() == 2) {
      // This will be enable/disable on region.
      regionName = msg.getPart(1).getString();
      if (regionName == null) {
        // This should have been taken care at the client - remove?
        String err = LocalizedStrings.MonitorCQ__0_A_NULL_REGION_NAME_WAS_PASSED_FOR_MONITORCQ_OPERATION.toLocalizedString(servConn.getName());
        sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
            .getTransactionId(), null, servConn);
        return;
      }
    }

    if (logger.fineEnabled()) {
      String rName = (regionName != null) ? " RegionName: " + regionName : "";
      logger.fine(servConn.getName() + ": Received MonitorCq request from "
          + servConn.getSocketString() + " op: " + op + rName);
    }

    try {
      CqService cqService = CqService.getCqService(crHelper.getCache());
      cqService.handleCqMonitorOp(op, regionName);
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    }
    catch (Exception e) {
      String err = LocalizedStrings.MonitorCQ_EXCEPTION_WHILE_HANDLING_THE_MONITOR_REQUEST_OP_IS_0.toLocalizedString(Integer.valueOf(op));
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err,
          msg.getTransactionId(), e, servConn);
      return;
    }

    // Send OK to client
    sendCqResponse(MessageType.REPLY,
        LocalizedStrings.MonitorCQ_MONITOR_CQ_REQUEST_COMPLETED_SUCCESSFULLY.toLocalizedString(), msg.getTransactionId(),
        null, servConn);
    servConn.setAsTrue(RESPONDED);

  }

}
