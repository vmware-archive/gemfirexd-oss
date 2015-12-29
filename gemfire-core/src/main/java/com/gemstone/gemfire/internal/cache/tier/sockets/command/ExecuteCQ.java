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

import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.cache.operations.ExecuteCQOperationContext;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.cache.query.internal.CqQueryImpl;
import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;

import java.io.IOException;
import java.util.Set;


public class ExecuteCQ extends BaseCommand {

  private final static ExecuteCQ singleton = new ExecuteCQ();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteCQ() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    AcceptorImpl acceptor = servConn.getAcceptor();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    ClientProxyMembershipID id = servConn.getProxyID();
    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    
    // Retrieve the data from the message parts
    String cqName = msg.getPart(0).getString();
    String cqQueryString = msg.getPart(1).getString();
    int cqState = msg.getPart(2).getInt();

    Part isDurablePart = msg.getPart(3);
    byte[] isDurableByte = isDurablePart.getSerializedForm();
    boolean isDurable = (isDurableByte == null || isDurableByte[0] == 0) ? false
        : true;
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received "
          + MessageType.getString(msg.getMessageType()) + " request from "
          + servConn.getSocketString() + " CqName : " + cqName
          + " queryString : " + cqQueryString);
    }

    DefaultQueryService qService = null;
    CqService cqServiceForExec = null;
    Query query = null;
    Set cqRegionNames = null;
    ExecuteCQOperationContext executeCQContext = null;
    CqQueryImpl cqQuery = null;
    
    try {
      qService = (DefaultQueryService)((GemFireCacheImpl)crHelper.getCache()).getLocalQueryService();

      // Authorization check
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        query = qService.newQuery(cqQueryString);
        cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
        executeCQContext = authzRequest.executeCQAuthorize(cqName,
            cqQueryString, cqRegionNames);
        String newCqQueryString = executeCQContext.getQuery();
        
        if (!cqQueryString.equals(newCqQueryString)) {
          query = qService.newQuery(newCqQueryString);
          cqQueryString = newCqQueryString;
          cqRegionNames = executeCQContext.getRegionNames();
          if (cqRegionNames == null) {
            cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
          }
        }
      }

      cqServiceForExec = qService.getCqService();
      cqQuery = (CqQueryImpl)cqServiceForExec.executeCq(cqName, cqQueryString, 
        cqState, id, acceptor.getCacheClientNotifier(), isDurable, false, 0, null);
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      return;
    }

    long oldstart = start;
    boolean sendResults = false;
    boolean successQuery = false;
    
    if (msg.getMessageType() == MessageType.EXECUTECQ_WITH_IR_MSG_TYPE) {
      sendResults = true;
    }

    // Execute the query and send the result-set to client.    
    try {
      if (query == null) {
        query = qService.newQuery(cqQueryString);
        cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
      }
      ((DefaultQuery)query).setIsCqQuery(true);
      successQuery = processQuery(msg, query, cqQueryString,
          cqRegionNames, start, cqQuery, executeCQContext, servConn, sendResults);

      // Update the CQ statistics.
      cqQuery.getVsdStats().setCqInitialResultsTime((DistributionStats.getStatTime()) - oldstart);
      stats.incProcessExecuteCqWithIRTime((DistributionStats.getStatTime()) - oldstart);
      //logger.fine("Time spent in execute with initial results :" + DistributionStats.getStatTime() + ", " +  oldstart);
    } finally { // To handle any exception.
      // If failure to execute the query, close the CQ.
      if (!successQuery) {
        try {
          cqServiceForExec.closeCq(cqName, id);
        }
        catch (Exception ex) {
          // Ignore.
        }        
      }
    }

    if (!sendResults && successQuery) {
      // Send OK to client
      sendCqResponse(MessageType.REPLY, LocalizedStrings.ExecuteCQ_CQ_CREATED_SUCCESSFULLY.toLocalizedString(), msg
          .getTransactionId(), null, servConn);

      long start2 = DistributionStats.getStatTime();
      stats.incProcessCreateCqTime(start2 - oldstart);
    }
    servConn.setAsTrue(RESPONDED);
  }

}
