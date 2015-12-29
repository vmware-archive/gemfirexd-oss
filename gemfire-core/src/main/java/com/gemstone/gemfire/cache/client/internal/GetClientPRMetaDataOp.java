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
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.BucketServerLocation66;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Retrieves {@link ClientPartitionAdvisor} for the specified PartitionedRegion from
 * one of the servers
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 6.5
 */
public class GetClientPRMetaDataOp {

  private GetClientPRMetaDataOp() {
    // no instances allowed
  }

  public static void execute(ExecutablePool pool, String regionFullPath,
      ClientMetadataService cms) {
    LogWriterI18n logger = pool.getLoggerI18n();
    AbstractOp op = new GetClientPRMetaDataOpImpl(logger, regionFullPath, cms);
    if (logger.fineEnabled()) {
      logger
          .fine("GetClientPRMetaDataOp#execute : Sending GetClientPRMetaDataOp Message:"
              + op.getMessage() + " to server using pool: " + pool);
    }
    pool.execute(op);
  }

  static class GetClientPRMetaDataOpImpl extends AbstractOp {

    String regionFullPath = null;

    LogWriterI18n logger = null;

    ClientMetadataService cms = null;

    public GetClientPRMetaDataOpImpl(LogWriterI18n lw, String regionFullPath,
        ClientMetadataService cms) {
      super(lw, MessageType.GET_CLIENT_PR_METADATA, 1);
      logger = lw;
      this.regionFullPath = regionFullPath;
      this.cms = cms;
      getMessage().addStringPart(regionFullPath);
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object processResponse(Message msg) throws Exception {
      switch (msg.getMessageType()) {
        case MessageType.GET_CLIENT_PR_METADATA_ERROR:
          String errorMsg = msg.getPart(0).getString();
          if (logger.fineEnabled()) {
            logger.fine(errorMsg);
          }
          throw new ServerOperationException(errorMsg);
        case MessageType.RESPONSE_CLIENT_PR_METADATA:
          if (logger.fineEnabled()) {
            logger
                .fine("GetClientPRMetaDataOpImpl#processResponse: received message of type : "
                    + MessageType.getString(msg.getMessageType()));
          }
          int numParts = msg.getNumberOfParts();
          ClientPartitionAdvisor advisor = cms
              .getClientPartitionAdvisor(regionFullPath);
          for (int i = 0; i < numParts; i++) {
            Object result = msg.getPart(i).getObject();
            List<BucketServerLocation66> locations = (List<BucketServerLocation66>)result;
          if (!locations.isEmpty()) {
            int bucketId = locations.get(0).getBucketId();
            if (logger.fineEnabled()) {
              logger
                  .fine("GetClientPRMetaDataOpImpl#processResponse: for bucketId : "
                      + bucketId + " locations are " + locations);
            }
            advisor.updateBucketServerLocations(bucketId, locations, cms);
            
            Set<ClientPartitionAdvisor> cpas = cms
                .getColocatedClientPartitionAdvisor(regionFullPath);
            if (cpas != null && !cpas.isEmpty()) {
              for (ClientPartitionAdvisor colCPA : cpas) {
                colCPA.updateBucketServerLocations(bucketId, locations, cms);
              }
            }
          }
          }
          if (logger.fineEnabled()) {
            logger.fine("GetClientPRMetaDataOpImpl#processResponse: "
                + "received  ClientPRMetadata from server successfully.");
          }
          return null;
        case MessageType.EXCEPTION:
          if (logger.fineEnabled()) {
            logger
                .fine("GetClientPRMetaDataOpImpl#processResponse: " +
                		"received message of type EXCEPTION");
          }
          Part part = msg.getPart(0);
          Object obj = part.getObject();
          String s = "While performing  GetClientPRMetaDataOp "
              + ((Throwable)obj).getMessage();
          throw new ServerOperationException(s, (Throwable)obj);
        default:
          throw new InternalGemFireError(
              LocalizedStrings.Op_UNKNOWN_MESSAGE_TYPE_0
                  .toLocalizedString(Integer.valueOf(msg.getMessageType())));
      }
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetClientPRMetadata();
    }

    protected String getOpName() {
      return "GetClientPRMetaDataOp";
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPRMetadataSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetClientPRMetadata(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
  }

}
