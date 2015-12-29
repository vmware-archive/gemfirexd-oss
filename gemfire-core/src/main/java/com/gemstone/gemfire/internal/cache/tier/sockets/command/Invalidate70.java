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

import com.gemstone.gemfire.cache.client.internal.DestroyOp;
import com.gemstone.gemfire.cache.client.internal.InvalidateOp;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

import java.io.IOException;

/**
 * @author bruces
 *
 */
public class Invalidate70 extends Invalidate {

  private final static Invalidate70 singleton = new Invalidate70();

  public static Command getCommand() {
    return singleton;
  }

  private Invalidate70() {
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr,
      byte nwHop, VersionTag versionTag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 2;
    if (versionTag != null) {
      flags |= InvalidateOp.HAS_VERSION_TAG;
      numParts++;
    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion().byteValue(), nwHop});
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.send(servConn);
    if (logger.finerEnabled()) {
      logger.finer(servConn.getName() + ": rpl with REFRESH_METADAT tx: "
          + origMsg.getTransactionId());
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn,
      VersionTag versionTag)
  throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 2;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      if (logger.fineEnabled()) {
        logger.fine("wrote version tag in response: " + versionTag);
      }
      replyMsg.addObjPart(versionTag);
    } else {
      if (logger.fineEnabled()) {
        logger.fine("response has no version tag");
      }
    }
    replyMsg.addBytesPart(OK_BYTES); // make old single-hop code happy by putting byte[]{0} here
    replyMsg.send(servConn);
    if (logger.finerEnabled()) {
      logger.finer(servConn.getName() + ": rpl tx: "
          + origMsg.getTransactionId() + " parts=" + replyMsg.getNumberOfParts());
    }
  }
}
