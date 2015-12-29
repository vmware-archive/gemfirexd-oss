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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.IllegalTransactionStateException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;

/**
 * 
 * @author sbawaska
 *
 */
public final class TXRemoteRollbackMessage extends TXMessage {

  private static final short CALLBACK_ARG_SET = UNRESERVED_FLAGS_START;

  private Object callbackArg;

  public TXRemoteRollbackMessage() {
  }

  private TXRemoteRollbackMessage(final TXStateInterface tx,
      final ReplyProcessor21 processor, final Object callbackArg) {
    super(tx, processor);
    this.callbackArg = callbackArg;
  }

  public static RollbackResponse send(final InternalDistributedSystem system,
      final DM dm, final TXStateProxy tx, final Object callbackArg,
      final Set<?> recipients) {
    final RollbackResponse response = new RollbackResponse(system, recipients);
    final TXRemoteRollbackMessage msg = new TXRemoteRollbackMessage(tx,
        response, callbackArg);
    msg.setRecipients(recipients);
    dm.putOutgoing(msg);
    // if there is any pending commit from this thread then wait for it first
    TXManagerImpl.getOrCreateTXContext().waitForPendingCommit();
    return response;
  }

  @Override
  protected boolean operateOnTX(TXStateProxy txProxy, DistributionManager dm) {
    // remove from hosted TXStates upfront so that there is no scope for a stray
    // message to create anything related to this TX
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    final TXManagerImpl txMgr;
    if (cache != null && (txMgr = cache.getCacheTransactionManager()) != null) {
      txProxy = txMgr.removeHostedTXState(getTXId(), Boolean.FALSE);
    }
    if (txProxy != null) {
      txProxy.rollback(null, this.callbackArg);
    }
    return true;
  }

  @Override
  public boolean containsRegionContentChange() {
    // for TX state flush
    return true;
  }

  /**
   * @see TransactionMessage#useTransactionProxy()
   */
  @Override
  public boolean useTransactionProxy() {
    // if proxy is present then by default the commit/rollback should act on it
    return true;
  }

  @Override
  protected final boolean isTXEnd() {
    return true;
  }

  public void toData(DataOutput out)
          throws IOException {
    super.toData(out);
    if (this.callbackArg != null) {
      DataSerializer.writeObject(this.callbackArg, out);
    }
  }

  @Override
  public void fromData(DataInput in)
          throws IOException, ClassNotFoundException {
    super.fromData(in);
    if ((flags & CALLBACK_ARG_SET) != 0) {
      this.callbackArg = DataSerializer.readObject(in);
    }
  }

  @Override
  protected short computeCompressedShort(short flags) {
    if (this.callbackArg != null) {
      flags |= CALLBACK_ARG_SET;
    }
    return flags;
  }

  /**
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return TX_REMOTE_ROLLBACK_MESSAGE;
  }

  /**
   * @see AbstractOperationMessage#appendFields(StringBuilder)
   */
  @Override
  protected void appendFields(final StringBuilder sb) {
    if (this.callbackArg != null) {
      sb.append("; callbackArg=").append(this.callbackArg);
    }
  }

  public static final class RollbackResponse extends ReplyProcessor21 {
    public RollbackResponse(InternalDistributedSystem system,
        Collection<?> initMembers) {
      super(system, initMembers);
    }

    @Override
    protected synchronized void processException(ReplyException ex) {
      Throwable t = ex.getCause();
      while (t != null) {
        if (t instanceof CancelException) {
          // ignore rollback for node failure
          return;
        }
        if (t instanceof IllegalTransactionStateException) {
          // ignore already rolled back transaction (e.g. via GII)
          return;
        }
      }
      super.processException(ex);
    }
  }
}
