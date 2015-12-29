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
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.cache.client.internal.CreateCQOp.CreateCQOpImpl;

/**
 * Close a continuous query on the server
 * @author darrel
 * @since 5.7
 */
public class CloseCQOp {
  /**
   * Close a continuous query on the given server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param cqName name of the CQ to close
   */
  public static void execute(ExecutablePool pool, String cqName)
  {
    AbstractOp op = new CloseCQOpImpl(pool.getLoggerI18n(), cqName);
    pool.executeOnAllQueueServers(op);
  }
                                                               
  private CloseCQOp() {
    // no instances allowed
  }
  
  private static class CloseCQOpImpl extends CreateCQOpImpl {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CloseCQOpImpl(LogWriterI18n lw, String cqName) {
      super(lw, MessageType.CLOSECQ_MSG_TYPE, 1);
      getMessage().addStringPart(cqName);
    }
    @Override
    protected String getOpName() {
      return "closeCQ";
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCloseCQ();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCloseCQSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCloseCQ(start, hasTimedOut(), hasFailed());
    }
  }
}
