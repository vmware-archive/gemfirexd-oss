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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.NonLocalRegionEntry;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * does getEntry on the server
 * @author sbawaska
 */
public class GetEntryOp {

  /**
   * Does a region.getEntry on the server using the given pool
   * @param pool
   * @param region
   * @param key
   * @return an {@link EntrySnapshot} for the given key
   */
  public static Object execute(ExecutablePool pool, LocalRegion region,
      Object key) {
    AbstractOp op = new GetEntryOpImpl(pool.getLoggerI18n(), region, key);
    return pool.execute(op);
  }
  
  static class GetEntryOpImpl extends AbstractOp {

    private LocalRegion region;
    private Object key;
    public GetEntryOpImpl(LogWriterI18n loggerI18n, LocalRegion region,
        Object key) {
      super(loggerI18n, MessageType.GET_ENTRY, 2);
      this.region = region;
      this.key = key;
      getMessage().addStringPart(region.getFullPath());
      getMessage().addStringOrObjPart(key);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      EntrySnapshot snap = (EntrySnapshot) processObjResponse(msg, "getEntry");
      if (snap != null) {
        snap.region = region;
      }
      return snap;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetEntry();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetEntrySend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetEntry(start, hasTimedOut(), hasFailed());
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
  }
}
