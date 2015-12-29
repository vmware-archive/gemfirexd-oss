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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import java.io.IOException;


public class ContainsKey extends BaseCommand {

  private final static ContainsKey singleton = new ContainsKey();

  public static Command getCommand() {
    return singleton;
  }

  private ContainsKey() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    Part regionNamePart = null, keyPart = null;
    String regionName = null;
    Object key = null;
    //StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadContainsKeyRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received containsKey request ("
          + msg.getPayloadLength() + " bytes) from "
          + servConn.getSocketString() + " for region " + regionName + " key "
          + key);
    }

    // Process the containsKey request
    if (key == null || regionName == null) {
      String errMessage = "";
      if (key == null) {
        if (logger.warningEnabled()) {
          logger.warning(
            LocalizedStrings.ContainsKey_0_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL,
            servConn.getName());
        }
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      if (regionName == null) {
        if (logger.warningEnabled()) {
          logger.warning(
            LocalizedStrings.ContainsKey_0_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL,
            servConn.getName());
        }
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      writeErrorResponse(msg, MessageType.CONTAINS_KEY_DATA_ERROR, errMessage,
          servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.ContainsKey_WAS_NOT_FOUND_DURING_CONTAINSKEY_REQUEST.toLocalizedString();
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        if (authzRequest != null) {
          try {
            authzRequest.containsKeyAuthorize(regionName, key);
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
        }
        // Execute the containsKey
        boolean containsKey = region.containsKey(key);

        // Update the statistics and write the reply
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessContainsKeyTime(start - oldStart);
        }
        writeContainsKeyResponse(containsKey, msg, servConn);
        servConn.setAsTrue(RESPONDED);
        if (logger.fineEnabled()) {
          logger.fine(servConn.getName()
              + ": Sent containsKey response for region " + regionName
              + " key " + key);
        }
        stats.incWriteContainsKeyResponseTime(DistributionStats.getStatTime()
            - start);
      }
    }

  }

  private static void writeContainsKeyResponse(boolean containsKey, Message origMsg,
      ServerConnection servConn) throws IOException {
    LogWriterI18n logger = servConn.getLogger();
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(containsKey ? Boolean.TRUE : Boolean.FALSE);
    responseMsg.send(servConn, logger, origMsg.getTransactionId());
  }

}
