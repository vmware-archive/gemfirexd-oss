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
import com.gemstone.gemfire.internal.cache.TXRemoteCommitMessage;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Does a commit on a server
 * @author gregp
 * @since 6.6
 */
public class CommitOp {
  /**
   * Does a commit on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   */
  public static TXRemoteCommitMessage execute(ExecutablePool pool,int txId)
  {
    CommitOpImpl op = new CommitOpImpl(pool.getLoggerI18n(),txId);
    pool.execute(op);
    return op.getTXCommitMessageResponse();
  }

  private CommitOp() {
    // no instances allowed
  }
  
    
  private static class CommitOpImpl extends AbstractOp {
    private final int txId;
    
    private TXRemoteCommitMessage tXCommitMessageResponse = null;
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public CommitOpImpl(LogWriterI18n lw,int txId) {
      super(lw, MessageType.COMMIT, 1);
      getMessage().setTransactionId(txId);
      this.txId = txId;
    }

    public TXRemoteCommitMessage getTXCommitMessageResponse() {
      return tXCommitMessageResponse;
    }
    
    @Override
    public String toString() {
      return "TXCommit(txId="+this.txId+")";
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      TXRemoteCommitMessage rcs = (TXRemoteCommitMessage)processObjResponse(msg, "commit");
      assert rcs != null : "TxCommit response was null";
      this.tXCommitMessageResponse = rcs;
      return rcs;
    }
    
    
    @Override  
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXCEPTION;
    }
    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startCommit();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endCommitSend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endCommit(start, hasTimedOut(), hasFailed());
    }
  }
}
