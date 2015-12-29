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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireCheckedException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.AbstractOperationMessage;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TLongHashSet;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdDDLRegionQueue;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * Base class for GemFireXD distribution messages.
 * 
 * @author swale
 * @since 6.0
 */
public abstract class GfxdMessage extends AbstractOperationMessage implements
    GfxdSerializable {

  protected boolean possibleDuplicate;

  // statistics data
  // ----------------
  /**
   * comparing two consecutive construct_time on originating node can indicate
   * message sending rate by the executor.
   * 
   * comparing message construct_time between origin & dest node minus
   * ser_deser_time on both ends will give request n/w latency.
   * 
   */
  protected transient Timestamp construct_time;

  protected transient long ser_deser_time;
  
  /** captures message processing time.
   * 
   * a. Root request message captures entire distribution time.
   * b. Cloned request messages captures individual send time.
   * c. Receiving node request message captures processMessage time.
   * 
   *  for (c) we have this attribute here, otherwise could have been
   *  part of GfxdFunctionMessage
   */
  protected long process_time;

  /**
   * routing object to bucket id determination time.
   */
  protected transient long member_mapping_time;

  protected transient short mapping_retry_count;

  protected transient boolean lastResultSent;
  // end of statistics data
  // -----------------------
  
  public final int getDSFID() {
    return DataSerializableFixedID.GFXD_TYPE;
  }

  public abstract byte getGfxdID();

  /** for deserialization */
  protected GfxdMessage() {
  }

  protected GfxdMessage(final TXStateInterface tx, boolean timeStatsEnabled) {
    super(tx);
    this.timeStatsEnabled = timeStatsEnabled;
    this.construct_time = this.timeStatsEnabled ? XPLAINUtil.currentTimeStamp() : null;
  }

  /** Copy constructor for child classes. */
  protected GfxdMessage(final GfxdMessage other) {
    super(other);
    this.construct_time = this.timeStatsEnabled ? XPLAINUtil.currentTimeStamp() : null;
    this.setPossibleDuplicate(other.isPossibleDuplicate());
  }

  @Override
  protected final void beforeToData(final DataOutput out) throws IOException {
    // write GFXD specific header first
    //GfxdDataSerializable.writeGfxdHeader(this, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
          ClassNotFoundException {
    super.fromData(in);
    // its a bit late here capturing construct_time but we already have
    // appropriate nanoTime from parent AbtractOperationMessage.
    this.construct_time = this.timeStatsEnabled ? XPLAINUtil.currentTimeStamp()
        : null;
    if ((flags & POS_DUP) != 0) {
      this.possibleDuplicate = true;
    }
  }

  /**
   * @see AbstractOperationMessage#computeCompressedShort(short)
   */
  @Override
  protected short computeCompressedShort(short flags) {
    if (this.possibleDuplicate) flags |= POS_DUP;
    return flags;
  }

  protected final void setProcessorId(int id) {
    this.processorId = id;
  }

  public final boolean isPossibleDuplicate() {
    return this.possibleDuplicate;
  }

  protected final void setPossibleDuplicate(boolean posDup) {
    this.possibleDuplicate = posDup;
  }

  protected int getMessageProcessorType() {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }

  @Override
  public final int getProcessorType() {
    return this.processorType == 0 ? getMessageProcessorType()
        : this.processorType;
  }

  @Override
  public void setProcessorType(boolean isReaderThread) {
    if (isReaderThread && getMessageProcessorType() != DistributionManager
        .WAITING_POOL_EXECUTOR) {
      this.processorType = DistributionManager.WAITING_POOL_EXECUTOR;
    }
  }

  /**
   * Get an {@link GfxdWaitingReplyProcessor} for the given the set of members.
   * If the given set of members is null then it assumes all booted members of
   * the distributed system.
   */
  public static GfxdWaitingReplyProcessor getReplyProcessor(
      InternalDistributedSystem system, Set<DistributedMember> members,
      boolean ignoreNodeDown) {
    DM dm = system.getDistributionManager();
    GfxdWaitingReplyProcessor processor = new GfxdWaitingReplyProcessor(dm,
        members, ignoreNodeDown, false);
    return processor;
  }

  /**
   * Sends an {@link GfxdMessage} to the given members of the distributed system
   * waiting for replies from all. If the provided set of members is null then
   * it sends the message to all booted server members of the distributed system
   * (stores and accessors).
   */
  public final void send(InternalDistributedSystem system,
      Set<DistributedMember> members, boolean ignoreNodeDown)
      throws StandardException, SQLException {
    if (members == null) {
      members = getOtherServers();
    }
    if (members.size() > 0) {
      final GfxdWaitingReplyProcessor processor = getReplyProcessor(system,
          members, ignoreNodeDown);
      setRecipients(members);
      send(system, system.getDistributionManager(), processor, true, false);
    }
  }

  /**
   * Sends an {@link GfxdMessage} to the given members of the distributed system
   * waiting for replies from all. If the provided set of members is null then
   * it sends the message to all booted members of the distributed system.
   */
  public final void send(InternalDistributedSystem system,
      Set<DistributedMember> members) throws StandardException, SQLException {
    send(system, members, false);
  }

  /**
   * Sends an {@link GfxdMessage} to members of the distributed system using the
   * provided {@link GfxdReplyMessageProcessor} waiting for replies or not
   * depending on the "ack" flag.
   * 
   * The recipients should have already been set by a call to
   * {@link #setRecipients(java.util.Collection)} or
   * {@link #setRecipient(InternalDistributedMember)}.
   */
  public final void send(final InternalDistributedSystem system, final DM dm,
      GfxdReplyMessageProcessor processor, boolean ack, boolean toSelf)
      throws StandardException, SQLException {
    setProcessorId(processor.getProcessorId());
    InternalDistributedMember[] members = null;
    if (forAll() || (members = getRecipients()).length > 0) {
      if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, getClass()
            .getSimpleName() + "#send: sending message [" + toString()
            + "] to recipients " + getRecipientsString(members));
      }
      Set<?> failures = dm.putOutgoing(this);
      // for direct reply messages check for failure in send itself since
      // it will not be handled by memberDeparted+ReplyProcessor21 (#48532)
      if (failures != null && !failures.isEmpty()
          && processor.isExpectingDirectReply()) {
        throw new GemFireXDRuntimeException(new ForceReattemptException(
            "Failed sending <" + this + ">"));
      }
    }
    ReplyException re = null;
    try {
      // do waitForReplies even if beforeWaitForReplies throws exception
      // so that no pending ops remain on remote nodes
      try {
        beforeWaitForReplies(processor, toSelf);
      } catch (ReplyException ex) {
        // Throw back an exception from local node also only if ACK is true.
        // Else higher layer will extract all exceptions from ReplyProcessor21.
        if (ack) {
          re = ex;
        }
      }
      if (ack) {
        processor.waitForReplies();
      }
      if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, getClass()
            .getSimpleName() + "#send: done for GemFireXD message ["
            + toString() + "] with toSelf=" + toSelf + " for recipients "
            + getRecipientsString(members));
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      // check for VM going down
      Misc.checkIfCacheClosing(ie);
      re = new ReplyException(ie);
    } catch (ReplyException ex) {
      re = ex;
    } finally {
      if (re != null) {
        handleReplyException(toString(), re, processor);
      }
    }
  }

  protected final String getRecipientsString(
      InternalDistributedMember[] recipients) {
    return (recipients != null ? Arrays.toString(recipients) : "ALL");
  }

  protected void beforeWaitForReplies(GfxdReplyMessageProcessor processor,
      boolean toSelf) throws ReplyException {
    // any processing to be done before waiting for replies should be done here
  }

  @Override
  protected void handleFromDataExceptionCheckCancellation(Throwable t)
      throws CancelException {
    // GemFireXD requires a valid cache unlike the generic parent handling
    // that needs only the DistributedSystem
    Misc.getGemFireCache().getCancelCriterion().checkCancelInProgress(t);
  }

  /**
   * Handle the exceptions received from remote nodes.
   */
  protected void handleReplyException(String exPrefix, ReplyException ex,
      GfxdReplyMessageProcessor processor)
      throws SQLException, StandardException {
    final Map<DistributedMember, ReplyException> exceptions = processor
        .getReplyExceptions();
    // There may be more than one exception, so check all of them
    if (exceptions != null) {
      for (ReplyException replyEx : exceptions.values()) {
        handleProcessorReplyException(exPrefix, replyEx);
      }
    }
    // if nothing was done for any stored exceptions, try for the passed one
    handleProcessorReplyException(exPrefix, ex);
  }

  protected final void handleUnexpectedReplyException(String exPrefix,
      ReplyException replyEx) throws SQLException, StandardException {
    if (GemFireXDUtils.TraceFunctionException) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_FUNCTION_EX, exPrefix
          + ": unexpected exception", replyEx);
    }
    GemFireXDRuntimeException.throwSQLOrRuntimeException(toString()
        + ": unexpected exception", replyEx.getCause());
  }

  protected void handleProcessorReplyException(String exPrefix,
      ReplyException replyEx) throws SQLException, StandardException {
    final Throwable t = replyEx.getCause();
    if (!GemFireXDUtils.retryToBeDone(t)) {
      handleUnexpectedReplyException(exPrefix, replyEx);
    }
  }

  /**
   * Some common processing for all {@link GfxdMessage}s that will in turn
   * invoke {@link #processMessage(DistributionManager)} that should be
   * overridden by child classes. In addition child classes should implement
   * {@link #blockExecutionForLastBatchDDLReplay()} method that will tell whether or not
   * processing should wait for node initialization to complete. Currently this
   * method only causes processing to wait until DDL replay is complete if
   * {@link #blockExecutionForLastBatchDDLReplay()} returns true.
   */
  @Override
  protected final void basicProcess(final DistributionManager dm) {
    final boolean recordStats = this.timeStatsEnabled;
    if (recordStats) {
      SanityManager.ASSERT(construct_time != null);
    }

    final long begintime = recordStats ? XPLAINUtil.recordTiming(-1) : 0;
    /* temporarily carrying the begin time here & statementExecutorMessage 
     * processing will overwrite it before lastResult(...) & let it recorded.
     * 
     * finally block below is too late, but atleast capturing initial processing
     * time.
     * 
     */
    process_time = begintime;

    GemFireStore memStore = null;
    boolean ddlLockAcquired = false;
    if (waitForNodeInitialization()) {
      GemFireXDUtils.waitForNodeInitialization();
    }
    else if (blockExecutionForLastBatchDDLReplay()
        && (memStore = GemFireStore.getBootedInstance()) != null
        && !memStore.initialDDLReplayDone()) {
      memStore.acquireDDLReplayLock(false);
      ddlLockAcquired = true;
    }
    ReplyException replyEx = null;
    Throwable runtimeEx = null;
    GemFireCacheImpl cache = null;
    TXManagerImpl txMgr = null;
    TXManagerImpl.TXContext txContext = null;
    preProcessMessage(dm);
    try {
      // masquerade for remote TX call but not for this VM itself
      if (dm != null && isTransactional()) {
        cache = Misc.getGemFireCache();
        txMgr = cache.getTxManager();
        txContext = txMgr.masqueradeAs(this, false, true);
      }
      processMessage(dm);
    } catch (final GemFireCheckedException ex) {
      replyEx = new ReplyException(ex);
    } catch (final ReplyException ex) {
      replyEx = ex;
    } catch (final RuntimeException ex) {
      replyEx = new ReplyException(ex);
      runtimeEx = ex;
    } catch (final Error err) {
      if (SystemFailure.isJVMFailureError(err)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable.
      SystemFailure.checkFailure();
      // If the above returns then send back error since this may be assertion
      // or some other internal code bug.
      replyEx = new ReplyException(err);
      runtimeEx = err;
    } finally {
      String cancelInProgress = null;
      try {
        if (dm != null) {
          if (txContext != null) {
            txMgr.unmasquerade(txContext, true);
          } else {
            cache = Misc.getGemFireCache();
            txMgr = cache.getTxManager();
            txMgr.unmasquerade(TXManagerImpl.currentTXContext(), true);  
          }
        }
        if (cache == null) {
          cache = Misc.getGemFireCache();
        }
        cancelInProgress = cache.getCancelCriterion().cancelInProgress();
        // also check for FabricService running
        FabricService service;
        if (cancelInProgress == null && (service = FabricServiceManager
            .currentFabricServiceInstance()) != null
            && (service.status() == FabricService.State.STOPPING
                || service.status() == FabricService.State.STOPPED
                || service.status() == FabricService.State.RECONNECTING)) {
          cancelInProgress = "GemFireXD instance not active";
        }
      } catch (CancelException ce) {
        cancelInProgress = ce.getLocalizedMessage();
      }
      if (cancelInProgress != null) {
        runtimeEx = new CacheClosedException(cancelInProgress, runtimeEx);
        replyEx = new ReplyException(runtimeEx);
      }
      endMessage();
      if (!this.lastResultSent) {
        sendReply(replyEx, dm);
      }
      if (ddlLockAcquired) {
        memStore.releaseDDLReplayLock(false);
      }
      /* enable this once we put mechanism to update descriptor record
      if(begintime != 0) {
        process_time = XPLAINUtil.recordTiming(begintime);
      }
      */
    }
  }

  protected void endMessage() {
  }

  /** Any message specific pre-processing required before TX masquerade etc. */
  protected void preProcessMessage(DistributionManager dm) {
  }

  /** Message specific processing. */
  protected abstract void processMessage(DistributionManager dm)
      throws GemFireCheckedException;

  /** Send the result of message processing. */
  protected abstract void sendReply(ReplyException ex, DistributionManager dm);

  /**
   * Return true if the message needs to wait for the last batch of DDLs in
   * initial DDL replay to complete on the node. Note that this should be true
   * for specialized messages like DDL finish message that depend on this..
   */
  protected boolean blockExecutionForLastBatchDDLReplay() {
    return false;
  }

  /**
   * Return true to wait for node to be fully initialized and DDL replay to
   * complete. This should normally be true for any messages that need to read
   * DataDictionary or modify data explicitly since DDL replay itself will not
   * acquire any locks.
   */
  protected abstract boolean waitForNodeInitialization();

  @Override
  public void reset() {
    super.reset();
    this.possibleDuplicate = false;
  }

  /**
   * Returns a modifiable set of all members that are part of GemFireXD including
   * any stand-alone locators but excluding self.
   */
  public static Set<DistributedMember> getOtherMembers() {
    final Set<DistributedMember> members = getAllGfxdMembers();
    final DistributedMember myId = Misc.getMyId();
    members.remove(myId);
    return members;
  }
  
  /**
   * Returns a modifiable set of all dataStores that are part of GemFireXD including
   * any stand-alone locators but excluding self.
   */
  public static Set<DistributedMember> getDataStores() {
    // only send the message to VMs that have the DDL region queue i.e.
    // those that have finished or in process of initial bootup sequence
    return GemFireXDUtils.getGfxdAdvisor().adviseDataStores(null);
  }
  
  /**
   * Returns a modifiable set of all members that are part of GemFireXD
   * excluding self and excluding any stand-alone locators.
   */
  public static Set<DistributedMember> getOtherServers() {
    final Set<DistributedMember> members = getAllGfxdServers();
    final DistributedMember myId = Misc.getMyId();
    members.remove(myId);
    return members;
  }

  /**
   * Returns a modifiable set of all members that are part of GemFireXD
   * including self and any stand-alone locators.
   */
  public static Set<DistributedMember> getAllGfxdMembers() {
    // only send the message to VMs that have the DDL region queue i.e.
    // those that have finished or in process of initial bootup sequence
    return GemFireXDUtils.getGfxdAdvisor().adviseAllNodes(null);
  }

  /**
   * Returns a modifiable set of all members that are part of GemFireXD
   * including self but excluding any stand-alone locators.
   */
  public static Set<DistributedMember> getAllGfxdServers() {
    // only send the message to VMs that have the DDL region queue i.e.
    // those that have finished or in process of initial bootup sequence
    return GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(null);
  }

  /**
   * Returns a modifiable set of all members that are part of Distributed
   * System including self and admin members.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllDSMembers() {
    Set<DistributedMember> members = new THashSet(Misc.getDistributedSystem()
        .getDistributionManager().getDistributionManagerIdsIncludingAdmin());
    final DistributedMember myId = Misc.getMyId();
    members.add(myId);
    return members;
  }
  
  public static Set<DistributedMember> getAllDataStores() {
    return GemFireXDUtils.getGfxdAdvisor().adviseDataStores(null);
  }

  /**
   * Returns true if DDL with given ID has already been processed by the DDL
   * replay thread and should be skipped and false otherwise. Also blocks this
   * thread if the last iteration of the DDL replay thread is in progress by
   * taking the DDL replay read lock to avoid missing anything.
   */
  public static boolean isOKToSkip(long ddlQueueId) {
    final GemFireStore memStore = Misc.getMemStore();
    // Take the lock on DDLs processed set before reading *and release it
    // only after taking the DDL replay read lock*. This is important to
    // ensure that when we read the set then it has all the processed IDs
    // as in the last iteration of the replay thread that will take the
    // DDL replay write lock.

    // In case DDL replay has not started, wait for it to begin first
    final Object sync = memStore.getInitialDDLReplaySync();
    synchronized (sync) {
      while (!memStore.initialDDLReplayInProgress()
          && !memStore.initialDDLReplayDone()) {
        // if queue has not been initialized then don't wait
        final GfxdDDLRegionQueue ddlQ = memStore.getDDLQueueNoThrow();
        if (ddlQ == null || !ddlQ.isInitialized()) {
          break;
        }
        try {
          sync.wait(500L);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          // check for VM going down
          Misc.checkIfCacheClosing(ie);
        }
      }
    }
    // Wait for initial DDL replay to complete if required (last iteration).
    memStore.acquireDDLReplayLock(false);
    try {
      final TLongHashSet processedIds = memStore.getProcessedDDLIDs();
      synchronized (processedIds) {
        // check if message already processed during initial replay
        if (processedIds.contains(ddlQueueId)) {
          return true;
        }
      }
      return !memStore.initialDDLReplayDone();
    } finally {
      memStore.releaseDDLReplayLock(false);
    }
  }

  /** log the warnings, if any, in execution of an SQL statement */
  public static void logWarnings(Statement stmt, String sqlText, String prefix,
      LogWriter logger) throws SQLException {
    SQLWarning warning = stmt.getWarnings();
    if (warning != null) {
      if (logger.warningEnabled()) {
        logger.warning(prefix + sqlText + " "+ warning.getMessage(), null);
      }
      while ((warning = warning.getNextWarning()) != null) {
        if (logger.warningEnabled()) {
          logger.warning(prefix + sqlText + " " + warning.getMessage(), null);
        }
      }
    }
  }

  protected static GfxdConnectionWrapper getExistingWrapper(long connId) {
    return GfxdConnectionHolder.getHolder().getExistingWrapper(connId);
  }

  @Override
  protected void appendFields(final StringBuilder sb) {
    sb.append(";posDup=").append(this.possibleDuplicate);
  }
}
