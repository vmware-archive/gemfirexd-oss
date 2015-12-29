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
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Does a Rollback on the server
 * @since 6.6
 * @author sbawaska
 */
public class RollbackOp {

  /**
   * Does a rollback on the server for given transaction
   * @param pool the pool to use to communicate with the server.
   * @param txId the id of the transaction to rollback
   */
  public static void execute(ExecutablePool pool, int txId) {
    RollbackOpImpl op = new RollbackOpImpl(pool.getLoggerI18n(), txId);
    pool.execute(op);
  }
  
  private RollbackOp() {
    // no instance allowed
  }
  
  private static class RollbackOpImpl extends AbstractOp {
    private int txId;

    protected RollbackOpImpl(LogWriterI18n lw, int txId) {
      super(lw, MessageType.ROLLBACK, 1);
      getMessage().setTransactionId(txId);
      this.txId = txId;
    }
    
    @Override
    public String toString() {
      return "Rollback(txId="+this.txId+")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "rollback");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXCEPTION;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRollback();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRollbackSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRollback(start, hasTimedOut(), hasFailed());
    }
    
  }
}
