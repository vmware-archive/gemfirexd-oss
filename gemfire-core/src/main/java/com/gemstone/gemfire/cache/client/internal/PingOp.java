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

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Ping a server to see if it is still alive.
 * @author darrel
 * @since 5.7
 */
public class PingOp {
  /**
   * Ping the specified server to see if it is still alive
   * @param pool the pool to use to communicate with the server.
   * @param server the server to do the execution on
   */
  public static void execute(ExecutablePool pool, ServerLocation server)
  {
    AbstractOp op = new PingOpImpl(pool.getLoggerI18n());
    pool.executeOn(server, op, false,false);
  }
                                                               
  private PingOp() {
    // no instances allowed
  }

  static class PingOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public PingOpImpl(LogWriterI18n lw) {
      super(lw, MessageType.PING, 0);
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
      Message.messageType.set(null);
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
      Message.messageType.set(MessageType.PING);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "ping");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPing();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPingSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPing(start, hasTimedOut(), hasFailed());
    }
  }
}
