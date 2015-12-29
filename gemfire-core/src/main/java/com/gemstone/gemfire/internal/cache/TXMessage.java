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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Base class for TX meta-operations like commit/rollback.
 * 
 * @author sbawaska, swale
 */
public abstract class TXMessage extends AbstractOperationMessage implements
    MessageWithReply {

  public TXMessage() {
  }

  public TXMessage(final TXStateInterface tx, final ReplyProcessor21 processor) {
    super(tx);
    this.processorId = processor != null ? processor.getProcessorId() : 0;
  }

  @Override
  protected final void basicProcess(final DistributionManager dm) {
    Throwable thr = null;
    final LogWriterI18n logger = dm.getLoggerI18n();
    final boolean fineEnabled = logger.fineEnabled() || TXStateProxy.LOG_FINE;
    boolean sendReply = true;
    TXStateProxy proxy = null;
    try {
      if (fineEnabled) {
        logger.info(LocalizedStrings.DEBUG, "TX: received " + toString()
            + " from " + getSender() + " for " + getTXId());
      }
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache == null || cache.isClosed()) {
        // This cache is closed / shutting down... we can't do much.
        if (fineEnabled) {
          logger.info(LocalizedStrings.DEBUG,
              "TX: returning due to cache closed for " + toString());
        }
        return;
      }
      final TXManagerImpl txMgr = cache.getTxManager();
      assert getTXId() != null;
      final boolean endTX = isTXEnd();
      final boolean useBatching = useBatching();
      final TXManagerImpl.TXContext txContext = txMgr.masqueradeAs(this, endTX,
          useBatching);
      final TXStateInterface tx;
      if (txContext != null) {
        tx = txContext.getTXState();
        proxy = (tx != null ? tx.getProxy() : null);
        if (fineEnabled) {
          logger.fine("TX: got TXContext " + txContext + ", TX " + tx + " for "
              + toString());
        }
        try {
          sendReply = operateOnTX(proxy, dm);
        } finally {
          txMgr.unmasquerade(txContext, useBatching);
          postOperateOnTX(proxy, dm);
        }
      }
      else {
        sendReply = operateOnTX(null, dm);
      }
    } catch (TransactionException te) {
      SystemFailure.checkFailure();
      thr = te;
    } catch (RegionDestroyedException rde) {
      thr = new ForceReattemptException(LocalizedStrings
          .PartitionMessage_REGION_IS_DESTROYED_IN_0.toLocalizedString(dm
              .getDistributionManagerId()), rde);
    } catch (Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // log the exception at fine level if there is no reply to the message
      // Asif: The commented snippet is absent in trunk, which results in
      // RemoteTransactionDUnit passing. If this is enabled it will fail.
      // The code looks logical, why is it absent from trunk?
      // [sumedh] The snippet below should be present. A bug on trunk that
      // masks other bugs.
      if (sendReply) {
        thr = t;
      }
    } finally {
      if (sendReply) {
        ReplyException rex = null;
        if (thr != null) {
          rex = new ReplyException(thr);
        }
        sendReply(getSender(), this.processorId, dm, rex, proxy);
      }
    }
  }

  protected void sendReply(InternalDistributedMember recipient,
      int processorId, DistributionManager dm, ReplyException rex,
      TXStateProxy proxy) {
    if (processorId != 0) {
      ReplyMessage.send(recipient, processorId, rex, getReplySender(dm),
          isTXEnd() ? null : this /* don't update regions */);
    }
  }

  /**
   * Transaction operations override this method to do actual work
   * 
   * @param txProxy
   *          The {@link TXStateProxy} to operate on
   * @param dm
   *          The {@link DistributionManager} being used for the message
   * 
   * @return true if {@link TXMessage} should send a reply false otherwise
   */
  protected abstract boolean operateOnTX(TXStateProxy txProxy,
      DistributionManager dm);

  /**
   * Sub-classes can override this to perform any operations post
   * {@link #operateOnTX} method call. Note that the TXStateInterface will not
   * be set in thread-local at this point.
   */
  protected void postOperateOnTX(TXStateProxy txProxy, DistributionManager dm) {
  }

  /**
   * Return true if this is an operation that may use batching.
   */
  protected boolean useBatching() {
    return false;
  }

  @Override
  public boolean orderedDelivery(boolean threadOwnsResources) {
    // use ordered delivery of TX messages by default, particularly
    // commit/rollback for better behaviour with subsequent ops
    return true;
  }

  protected int getMessageProcessorType() {
    return DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }

  @Override
  public final int getProcessorType() {
    return this.processorType == 0 ? getMessageProcessorType()
        : this.processorType;
  }

  @Override
  public void setProcessorType(boolean isReaderThread) {
    if (isReaderThread) {
      this.processorType = DistributionManager.WAITING_POOL_EXECUTOR;
    }
  }
}
