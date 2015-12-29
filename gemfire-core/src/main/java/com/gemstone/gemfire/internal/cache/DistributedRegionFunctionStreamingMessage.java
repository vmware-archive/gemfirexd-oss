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
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.ArrayUtils;

public final class DistributedRegionFunctionStreamingMessage extends
    AbstractOperationMessage {

  transient int replyMsgNum = 0;

  transient boolean replyLastMsg;

  transient int numObjectsInChunk = 0;

  private Function functionObject;

  private transient String functionName;

  Object args;

  private String regionPath;

  private Set<?> filter;

  private boolean isReExecute;

  private boolean isFnSerializationReqd;

  private static final short IS_REEXECUTE = UNRESERVED_FLAGS_START;

  /** default exception to ensure a false-positive response is never returned */
  static final ForceReattemptException UNHANDLED_EXCEPTION =
    (ForceReattemptException)new ForceReattemptException(
      "Unknown exception").fillInStackTrace();

  public DistributedRegionFunctionStreamingMessage() {
  }

  public DistributedRegionFunctionStreamingMessage(final String regionPath,
      Function function, int procId, final Set<?> filter, Object args,
      boolean isReExecute, boolean isFnSerializationReqd, TXStateInterface tx) {
    super(tx);
    this.functionObject = function;
    this.processorId = procId;
    this.args = args;
    this.regionPath = regionPath;
    this.filter = filter;
    this.isReExecute = isReExecute;
    this.isFnSerializationReqd = isFnSerializationReqd;
    // check for TX match with that in thread-local
    final TXStateInterface currentTX;
    assert tx == (currentTX = TXManagerImpl.getCurrentTXState()):
        "unexpected mismatch of current TX " + currentTX
            + ", and TX passed to message " + tx;
  }

  private TXManagerImpl.TXContext prepForTransaction() {
    if (getTXId() == null) {
      return null;
    }
    else {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        // ignore and return, we are shutting down!
        return null;
      }
      TXManagerImpl mgr = cache.getTxManager();
      return mgr.masqueradeAs(this, false, true);
    }
  }

  private void cleanupTransaction(final TXManagerImpl.TXContext context,
      final DistributionManager dm) {
    if (getTXId() != null) {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        // ignore and return, we are shutting down!
        return;
      }
      TXManagerImpl mgr = cache.getTxManager();
      mgr.unmasquerade(context, true);
    }
  }

  @Override
  protected final void basicProcess(final DistributionManager dm) {

    Throwable thr = null;
    LogWriterI18n logger = null;
    boolean sendReply = true;
    DistributedRegion dr = null;
    logger = dm.getLoggerI18n();
    TXManagerImpl.TXContext txContext = null;

    try {
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        thr = new CacheClosedException(LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0.toLocalizedString(dm.getId()));
        return;
      }
      dr = (DistributedRegion)GemFireCacheImpl.getInstance().getRegion(
          this.regionPath);
      if (dr == null) {
        // if the distributed system is disconnecting, don't send a reply saying
        // the partitioned region can't be found (bug 36585)
        thr = new ForceReattemptException(dm.getDistributionManagerId()
            .toString()
            + ": could not find Distributed region " + regionPath);
        return; // reply sent in finally block below
      }
      thr = UNHANDLED_EXCEPTION;
      txContext = prepForTransaction();
      sendReply = operateOnDistributedRegion(dm, dr); // need to take care of
                                                      // it...
      thr = null;
    }
    catch (CancelException se) {
      // bug 37026: this is too noisy...
      // throw new CacheClosedException("remote system shutting down");
      // thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.fineEnabled()) {
        logger.fine("shutdown caught, abandoning message: " + se);
      }
    }
    catch (RegionDestroyedException rde) {
      // if (logger.fineEnabled()) {
      // logger.fine("Region is Destroyed " + rde);
      // }
      // [bruce] RDE does not always mean that the sender's region is also
      // destroyed, so we must send back an exception. If the sender's
      // region is also destroyed, who cares if we send it an exception
      if (dr != null && dr.isClosed()) {
        thr = new ForceReattemptException("Region is destroyed in "
            + dm.getDistributionManagerId(), rde);
      }
    }
    catch (Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      logger.fine(this + " exception occured while processing message:", t);
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // log the exception at fine level if there is no reply to the message
      thr = null;
      if (sendReply && this.processorId != 0) {
        if (!checkDSClosing(dm)) {
          thr = t;
        }
        else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new ForceReattemptException(
              "Distributed system is disconnecting");
        }
      }
      if (this.processorId == 0) {
        logger.fine(this + " exception while processing message:", t);
      }
      else if (DistributionManager.VERBOSE && (t instanceof RuntimeException)) {
        logger.fine("Exception caught while processing message", t);
      }
    }
    finally {
      cleanupTransaction(txContext, dm);
      if (sendReply && this.processorId != 0) {
        ReplyException rex = null;
        if (thr != null) {
          // don't transmit the exception if this message was to a listener
          // and this listener is shutting down
          boolean excludeException = false;
          if (!this.functionObject.isHA()) {
            excludeException = (thr instanceof CacheClosedException
                || (thr instanceof ForceReattemptException));
          }
          else {
            excludeException = thr instanceof ForceReattemptException;
          }
          if (!excludeException) {
            rex = new ReplyException(thr);
          }
        }
        // Send the reply if the operateOnPartitionedRegion returned true
        // Fix for hang in dunits on gemfirexd after merge.
        //ReplyMessage.send(getSender(), this.processorId, rex, dm);
        sendReply(getSender(), this.processorId, dm, rex, null, 0, true, false,
            true);
      }
    }
  }

  protected final boolean operateOnDistributedRegion(
      final DistributionManager dm, DistributedRegion r)
      throws ForceReattemptException {
    if (this.functionObject == null) {
      ReplyMessage.send(getSender(), this.processorId, new ReplyException(
          new FunctionException(LocalizedStrings
              .ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
                  .toLocalizedString(this.functionName))), dm, this,
          r.isInternalRegion());
      return false;
    }

    if (DistributionManager.VERBOSE) {
      final LogWriter logger = r.getCache().getLogger();
      logger.fine("FunctionMessage operateOnRegion: " + r.getFullPath());
    }
    if (r != null) {
      try {
        r.executeOnRegion(this, this.functionObject, this.args,
            this.processorId, this.filter, this.isReExecute);
        if (!this.replyLastMsg && this.functionObject.hasResult()) {
          ReplyMessage.send(getSender(), this.processorId, new ReplyException(
              new FunctionException(LocalizedStrings
                  .ExecuteFunction_THE_FUNCTION_0_DID_NOT_SENT_LAST_RESULT
                      .toString(functionObject.getId()))), dm, this,
              r.isInternalRegion());
          return false;
        }
      } catch (IOException e) {
        ReplyMessage.send(getSender(), this.processorId, new ReplyException(
            "Operation got interrupted due to "
                + "shutdown in progress on remote VM", e), dm, this);
        return false;
      } catch (CancelException sde) {
        ReplyMessage.send(getSender(), this.processorId, new ReplyException(
            new ForceReattemptException("Operation got interrupted due to "
                + "shutdown in progress on remote VM", sde)), dm, this,
            r.isInternalRegion());
        return false;
      }
    }
    else {
      throw new InternalGemFireError("FunctionMessage sent to wrong member");
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  /**
   * check to see if the cache is closing
   */
  final public boolean checkCacheClosing(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return (cache == null
        || cache.getCancelCriterion().cancelInProgress() != null);
  }

  /**
   * check to see if the distributed system is closing
   * 
   * @return true if the distributed system is closing
   */
  final public boolean checkDSClosing(DistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }

  public int getDSFID() {
    return DR_FUNCTION_STREAMING_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);

    Object object = DataSerializer.readObject(in);
    if (object instanceof String) {
      this.isFnSerializationReqd = false;
      this.functionObject = FunctionService.getFunction((String)object);
      if (this.functionObject == null) {
        this.functionName = (String)object;
      }
    }
    else {
      this.functionObject = (Function)object;
      this.isFnSerializationReqd = true;
    }
    this.args = (Serializable)DataSerializer.readObject(in);
    this.filter = DataSerializer.readHashSet(in);
    this.regionPath = DataSerializer.readString(in);
    this.isReExecute = (flags & IS_REEXECUTE) != 0;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);

    if(this.isFnSerializationReqd){
      DataSerializer.writeObject(this.functionObject, out);
    }
    else {
      DataSerializer.writeObject(functionObject.getId(), out);
    }
    DataSerializer.writeObject(this.args, out);
    DataSerializer.writeHashSet((HashSet<?>)this.filter, out);
    DataSerializer.writeString(this.regionPath, out);
  }

  @Override
  protected final short computeCompressedShort(short flags) {
    if (this.isReExecute) flags |= IS_REEXECUTE;
    return flags;
  }

  @Override
  protected void appendFields(final StringBuilder sb) {
    sb.append("; function=").append(this.functionObject.getId());
    sb.append("; args=");
    ArrayUtils.objectStringNonRecursive(this.args, sb);
    sb.append("; filter=").append(this.filter);
    sb.append("; regionPath=").append(this.regionPath);
  }

  public synchronized boolean sendReplyForOneResult(DM dm, Object oneResult,
      boolean lastResult, boolean sendResultsInOrder, boolean sendTXChanges)
      throws CacheException, ForceReattemptException, InterruptedException {
    if (this.replyLastMsg) {
      return false;
    }
    if (Thread.interrupted())
      throw new InterruptedException();
    LogWriterI18n logger = dm.getLoggerI18n();
    int msgNum = this.replyMsgNum;
    this.replyLastMsg = lastResult;

    sendReply(getSender(), this.processorId, dm, null, oneResult,
        msgNum, lastResult, sendResultsInOrder, sendTXChanges);
    
    if (logger.fineEnabled()) {
      logger.fine("Sending reply message count: " + replyMsgNum
          + " to co-ordinating node");
    }
    this.replyMsgNum++;
    return false;
  }

  protected void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, Object result, int msgNum, boolean lastResult,
      boolean sendResultsInOrder, boolean sendTXChanges) {
    // if there was an exception, then throw out any data
    if (ex != null) {
      this.replyMsgNum = 0;
      this.replyLastMsg = true;
    }
    if (sendResultsInOrder) {
      FunctionStreamingOrderedReplyMessage.send(member, procId, ex, dm, result,
          msgNum, lastResult, this, sendTXChanges);
    }
    else {
      FunctionStreamingReplyMessage.send(member, procId, ex, dm, result,
          msgNum, lastResult, this, sendTXChanges);
    }
  }

  @Override
  public final int getProcessorType() {
    return this.processorType == 0
        ? DistributionManager.REGION_FUNCTION_EXECUTION_EXECUTOR
        : this.processorType;
  }

  @Override
  public void setProcessorType(boolean isReaderThread) {
    if (isReaderThread) {
      this.processorType = DistributionManager.WAITING_POOL_EXECUTOR;
    }
  }

  /* (non-Javadoc)
   * @see TransactionMessage#canStartRemoteTransaction()
   */
  @Override
  public final boolean canStartRemoteTransaction() {
    return true;
  }

  /**
   * @see TransactionMessage#useTransactionProxy()
   */
  @Override
  public final boolean useTransactionProxy() {
    return true;
  }
}
