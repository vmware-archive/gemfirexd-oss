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

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * Does a region invalidate on a server
 * @author gregp
 * @since 6.6
 */
public class InvalidateOp {

  public static final int HAS_VERSION_TAG = 0x01;
  
  /**
   * Does a region invalidate on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry keySet on
   */
  public static void execute(ExecutablePool pool,
                            String region, EntryEventImpl event)
  {
    AbstractOp op = new InvalidateOpImpl(pool.getLoggerI18n(), region, event);
    pool.execute(op);
  }
                                                               
  private InvalidateOp() {
    // no instances allowed
  }
  
  private static class InvalidateOpImpl extends AbstractOp {
    private final EntryEventImpl event;
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public InvalidateOpImpl(LogWriterI18n lw,
                         String region,
                         EntryEventImpl event) {
      super(lw, MessageType.INVALIDATE, event.getCallbackArgument() != null ? 4 : 3);
      Object callbackArg = event.getCallbackArgument();
      this.event = event;
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(event.getKey());
      getMessage().addBytesPart(event.getEventId().calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }
    
    
    @Override
    protected Object processResponse(Message msg, Connection con) throws Exception {
       processAck(msg, "invalidate");
       LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
       boolean isReply = (msg.getMessageType() == MessageType.REPLY);
       int partIdx = 0;
       int flags = 0;
       if (isReply) {
         flags = msg.getPart(partIdx++).getInt();
         if ((flags & HAS_VERSION_TAG) != 0) {
           VersionTag tag = (VersionTag)msg.getPart(partIdx++).getObject();
           // we use the client's ID since we apparently don't track the server's ID in connections
           tag.replaceNullIDs((InternalDistributedMember)  con.getEndpoint().getMemberId());
           this.event.setVersionTag(tag);
           if (log.fineEnabled()) {
             log.fine("received Invalidate response with " + tag);
             }
         } else if (log.fineEnabled()) {
           log.fine("received Invalidate response");
         }
       }
       return null;
    }
    @Override  
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.INVALIDATE_ERROR;
    }
    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startInvalidate();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endInvalidateSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endInvalidate(start, hasTimedOut(), hasFailed());
    }
  }
}
