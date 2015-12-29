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

package com.pivotal.gemfirexd.internal.engine.distributed.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import com.gemstone.gemfire.GemFireCheckedException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.AbstractOperationMessage;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.InternalDataView;
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXEntryState;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.TransactionMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.OffHeapReleaseUtil;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * This is an optimized Region.get() implementation for GemFireXD that will also
 * apply any supplied projection on the store and send back only the required
 * columns.
 * 
 * @author swale
 * @since 7.0
 */
public final class GetExecutorMessage extends RegionSingleKeyExecutorMessage {

  /** the ordered list of fixed width column positions in the projection */
  protected int[] projectionFixedColumns;

  /** the ordered list of variable width column positions in the projection */
  protected int[] projectionVarColumns;

  /** the ordered list of LOB column positions in the projection */
  protected int[] projectionLobColumns;

  /**
   * The ordered list of all column positions in the projection excluding the
   * LOB columns. This list is not transported across but is only used in case
   * of local execution for better efficiency in generating the byte array.
   */
  protected transient int[] projectionAllColumns;

  protected byte targetFormatOffsetBytes;

  protected transient boolean hasProjection;

  protected transient RowFormatter targetFormat;

  // transient arguments used as execution parameters

  /** set to true if the message was requeued in the pr executor pool */
  protected transient boolean forceUseOfPRExecutor;

  /** set to true if there is a loader anywhere in the system for the region */
  private transient boolean hasLoader;

  private boolean queryHDFS;

  /**
   * GET can start a TX at REPEATABLE_READ isolation level or if there is a
   * loader in the region or for SELECT FOR UPDATE.
   */
  private boolean canStartTX;

  /** for SELECT FOR UPDATE */
  private boolean forUpdate;

  /**
   * Token object returned if the result returned by {@link #executeOneKey} is
   * not valid.
   */
  public static final Object INVALID_RESULT = new Object();

  // flags used in serialization are below

  protected static final short HAS_PROJECTION = UNRESERVED_FLAGS_START;
  protected static final short HAS_LOADER = (HAS_PROJECTION << 1);
  protected static final short CAN_START_TX = (HAS_LOADER << 1);
  protected static final short FOR_UPDATE = (CAN_START_TX << 1);

  private static final String ID = "GetExecutorMessage";

  /** Empty constructor for deserialization. Not to be invoked directly. */
  public GetExecutorMessage() {
    super(true);
  }

  public GetExecutorMessage(final LocalRegion region, final Object key,
      final Object callbackArg, final Object routingObject,
      final RowFormatter targetFormat, final int[] projectionFixedColumns,
      final int[] projectionVarColumns, final int[] projectionLobColumns,
      final int[] projectionAllColumns, final TXStateInterface tx,
      final LanguageConnectionContext lcc, final boolean forUpdate,
      final boolean queryHDFS) {
    // acquire lock on all copies for REPEATABLE_READ
    super(region, key, callbackArg, routingObject, tx != null
        && tx.getLockingPolicy().readCanStartTX(), tx,
        getTimeStatsSettings(lcc));
    final int offsetBytes = targetFormat.getNumOffsetBytes();
    assert offsetBytes > 0 && offsetBytes <= 4: offsetBytes;
    this.targetFormat = targetFormat;
    this.projectionFixedColumns = projectionFixedColumns;
    this.projectionVarColumns = projectionVarColumns;
    this.projectionLobColumns = projectionLobColumns;
    this.projectionAllColumns = projectionAllColumns;
    this.targetFormatOffsetBytes = (byte)offsetBytes;
    this.hasProjection = projectionAllColumns != null;
    this.forUpdate = forUpdate;
    this.hasLoader = ((GemFireContainer)region.getUserAttribute())
        .getHasLoaderAnywhere();
    if (isTransactional() && !region.getScope().isLocal()) {
      // check for loader in the region
      this.canStartTX = getLockingPolicy().readCanStartTX() || this.forUpdate
          || this.hasLoader;
    }
    this.queryHDFS = queryHDFS;
  }

  /** copy constructor */
  protected GetExecutorMessage(final GetExecutorMessage other) {
    super(other);
    this.targetFormat = other.targetFormat;
    this.projectionFixedColumns = other.projectionFixedColumns;
    this.projectionVarColumns = other.projectionVarColumns;
    this.projectionLobColumns = other.projectionLobColumns;
    this.projectionAllColumns = other.projectionAllColumns;
    this.targetFormatOffsetBytes = other.targetFormatOffsetBytes;
    this.hasProjection = other.hasProjection;
    this.forUpdate = other.forUpdate;
    this.hasLoader = other.hasLoader;
    this.canStartTX = other.canStartTX;
    this.queryHDFS = other.queryHDFS;
  }

  @Override
  public Object executeFunction(boolean enableStreaming,
      boolean isPossibleDuplicate, AbstractGemFireResultSet rs,
      boolean orderedReplies, boolean getResult) throws StandardException,
      SQLException {
    // overridden for stats
    final CachePerfStats stats = this.region.getCachePerfStats();
    final long start = stats.startGet();
    Object result = null;
    try {
      result = super.executeFunction(enableStreaming, isPossibleDuplicate, rs,
          orderedReplies, getResult);
    } finally {
      // Fixes for Issue #48947
      // If a SQL select on a PR table with primary key lookup is done it goes
      // by GetExecutorMessage to peer members.
      // As this message bypasses the Region.get() API and directly access
      // InternalDataView.getLocally() the "gets" stats are not getting updated.

      // We need to update the stats from outside to show correct stats.
      if (this.pr != null) {
        this.pr.prStats.endGet(start);
      }
      stats.endGet(start, result == null);
    }
    return result;
  }

  /**
   * @see GfxdFunctionMessage#execute()
   */
  @Override
  protected void execute() throws GemFireCheckedException {
    final boolean doLog = DistributionManager.VERBOSE
        | GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ;
    final boolean forceUseOfPRExecutor = this.forceUseOfPRExecutor;
    // Would be set to true when rescheduled, in executeOneKey
    this.forceUseOfPRExecutor = true;
    final Object resultOneKey = executeOneKey(this, this.key,
        this.callbackArg, this.bucketId, this.pr, this.region, this.regionPath,
        this.isSecondaryCopy, this.hasProjection, this.targetFormat,
        this.projectionFixedColumns, this.projectionVarColumns,
        this.projectionAllColumns, this.projectionLobColumns,
        this.targetFormatOffsetBytes, forceUseOfPRExecutor, this.forUpdate,
        this.queryHDFS);
    if (resultOneKey != INVALID_RESULT) {
      if (doLog) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
            + ": sending lastResult [" + resultOneKey + "] for region "
            + region.getFullPath() + " processorId=" + getProcessorId()
            + ", isSecondaryCopy=" + isSecondaryCopy);
      }

      final boolean isPrimary = !this.isSecondaryCopy;
      try {
        lastResult(resultOneKey,
            isPrimary /* flush any entries added via loader */, isPrimary, true);
      } catch (RuntimeException re) {
        if (GemFireXDUtils.isOffHeapEnabled() && this.isLocallyExecuted()) {
          OffHeapReleaseUtil.freeOffHeapReference(resultOneKey);
        }
        throw re;
      } finally {
        if (GemFireXDUtils.isOffHeapEnabled() && !this.isLocallyExecuted()) {
         OffHeapReleaseUtil.freeOffHeapReference(resultOneKey);
        }
      }
    }
    else {
      if (doLog) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
            + ": Not sending lastResult now for region " + region.getFullPath()
            + " processorId=" + getProcessorId() + ", forceUseOfPRExecutor="
            + forceUseOfPRExecutor);
      }
    }
  }

  /**
   * Executes get() for a single key and returns the result of get. If the
   * returned value is not valid due to entry locking required, then
   * {@link #INVALID_RESULT} is returned.
   */
  public static Object executeOneKey(
      final GfxdFunctionMessage<Object> gfxdFunctionMessage, final Object key,
      final Object callbackArg, final int bucketId, final PartitionedRegion pr,
      final LocalRegion region, final String regionPath,
      final boolean isSecondaryCopy, final boolean hasProjection,
      final RowFormatter targetFormat, final int[] projectionFixedColumns,
      final int[] projectionVarColumns, final int[] projectionAllColumns,
      final int[] projectionLobColumns, final byte targetFormatOffsetBytes,
      final boolean forceUseOfPRExecutor, final boolean forUpdate,
      final boolean queryHDFS) throws GemFireCheckedException {
    final TXStateInterface tx = gfxdFunctionMessage.getTXState();
    final boolean doLog = DistributionManager.VERBOSE
        | GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ;
    final boolean localExecution = gfxdFunctionMessage.isLocallyExecuted();
    final String ID = doLog ? gfxdFunctionMessage.getShortClassName()
        + ".execute" : "";

    @Retained @Released Object val;

    if (pr != null) { // PR case
      final long startTime = !localExecution ? pr.getPrStats()
          .startPartitionMessageProcessing() : 0;
      if (doLog) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
            + " called for PR " + pr.getFullPath() + " forUpdate=" + forUpdate
            + " on key " + key + ", TX is " + tx + ", thread-local is "
            + TXManagerImpl.getCurrentTXState() + " ,queryHDFS=" + queryHDFS);
      }

      if (pr.getDataStore() != null) {
        if (bucketId < 0) {
          Assert.fail("unexpected bucketId=" + bucketId + " for PR " + pr);
        }
        final boolean doNotLockEntry;
        if (forUpdate) {
          val = lockEntryForUpdate(region, pr, key, callbackArg, bucketId, tx,
              queryHDFS);
          doNotLockEntry = false;
        }
        else {
          final InternalDataView view = pr.getDataView(tx);
          doNotLockEntry = !forceUseOfPRExecutor
              && !gfxdFunctionMessage.isDirectAck();
          val = view.getLocally(key, callbackArg, bucketId, pr, doNotLockEntry,
              localExecution, null, null, false, queryHDFS);
        }

        if (val == BucketRegion.RawValue.REQUIRES_ENTRY_LOCK) {
          Assert.assertTrue(doNotLockEntry);
          // the forceUseOfPRExecutor is assumed to be set by caller
          // forceUseOfPRExecutor = true;
          if (doLog) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
                + ": Rescheduling execution due to possible cache-miss: "
                + gfxdFunctionMessage.toString());
          }
          if (gfxdFunctionMessage.dm != null) {
            gfxdFunctionMessage.schedule(gfxdFunctionMessage.dm);
            return INVALID_RESULT;
          }
          else {
            final InternalDataView view = pr.getDataView(tx);
            val = view.getLocally(key, callbackArg, bucketId,
                pr, false, localExecution, null, null, false, queryHDFS);
          }
        }

        if (doLog) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
              + ": sending value [" + val
              + "] back via ReplyMessage with processorId="
              + gfxdFunctionMessage.getProcessorId());
        }

        if (!localExecution) {
          pr.getPrStats().endPartitionMessagesProcessing(startTime);
        }
      }
      else {
        throw new InternalGemFireError(
            LocalizedStrings.GetMessage_GET_MESSAGE_SENT_TO_WRONG_MEMBER
                .toLocalizedString());
      }
    }
    else { // replicated case
      if (doLog) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
            + " called for DR " + regionPath + " on key " + key + ", TX is "
            + tx + ", thread-local is " + TXManagerImpl.getCurrentTXState());
      }
      if (forUpdate) {
        val = lockEntryForUpdate(region, null, key, callbackArg, 0, tx,
            queryHDFS);
      }
      else {
        final InternalDataView view = region.getDataView(tx);
        val = view.getLocally(key, callbackArg, KeyInfo.UNKNOWN_BUCKET, region,
            false, localExecution, null, null, false, queryHDFS);
      }

      if (doLog) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
            + ": sending value [" + val
            + "] for DR back via GetReplyMessage with processorId="
            + gfxdFunctionMessage.getProcessorId());
      }
    }

    final Object result;
    final boolean txCacheLoaded;
    final boolean isPrimary = !isSecondaryCopy;
    ProjectionRow projRow = null;
    if (val instanceof RawValue) {
      projRow = (ProjectionRow)val;
      val = projRow.getRawValue();
    }
    if (val != null && !Token.isInvalid(val) && isPrimary) {
      // apply the projection at this point if required

      // For transaction cache loaded need to add it to gateway via
      // DBSynchronizer message so return full row.
      txCacheLoaded = (tx != null && projRow != null && projRow
          .isFromCacheLoader());
      if (txCacheLoaded) {
        // indicate TX cache loaded value in ProjectionRow by sending full row
        // in ProjectionRow itself
        result = projRow;
      }
      else if (hasProjection) {
        final GemFireContainer container = (GemFireContainer)region
            .getUserAttribute();

        // if sending back result locally then directly create
        // AbstractCompactExecRow
        if (localExecution) {
          try {
            result = ProjectionRow.getCompactExecRow(val, container,
                targetFormat, projectionAllColumns, projectionLobColumns);
          } finally {
            OffHeapHelper.release(val);
          }
        }
        else {
          
          // RawValue created will actually be ProjectionRow; just embed
          // projection information in that
          if (projRow == null) {
            projRow = new ProjectionRow(val);
          }
          projRow.setProjectionInfo(container.getRowFormatter(val),
              projectionFixedColumns, projectionVarColumns,
              projectionLobColumns, targetFormatOffsetBytes);
          result = projRow;
        }
      }
      else {
        result = val;
      }
    }
    else {
      // DUMMY_RESULT indicates dummy result and only for locking
      result = isPrimary ? null : DUMMY_RESULT;      
      OffHeapHelper.release(val);
    }

    if (doLog) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, ID
          + ": result [" + result + "] for region " + region.getFullPath()
          + ", processorId=" + gfxdFunctionMessage.getProcessorId()
          + ", isSecondaryCopy=" + isSecondaryCopy + ", key= " + key.toString()
          + ", bucketId= " + bucketId);
    }

    return result;
  }

  @Retained
  private static Object lockEntryForUpdate(final LocalRegion region,
      final PartitionedRegion pr, final Object key, final Object callbackArg,
      final int bucketId, final TXStateInterface tx, final boolean queryHDFS) {
    // only allowed for transactions
    if (tx == null) {
      throw new InternalGemFireError(
          "SELECT FOR UPDATE invoked without a transaction");
    }
    // get the data region
    final LocalRegion dataRegion;
    if (pr != null) {
      dataRegion = pr.getDataRegionForRead(key, callbackArg, bucketId,
          Operation.GET);
    }
    else {
      dataRegion = region;
    }
    // acquire the lock on entry
    Object txes;
    try {
      txes = tx.lockEntry(null, key, callbackArg, region, dataRegion, true,
          queryHDFS, TXEntryState.getLockForUpdateOp(),
          TXState.LOCK_ENTRY_NOT_FOUND);
    } catch (EntryNotFoundException enfe) {
      txes = null;
    }
    if (txes != null) {
      TXEntryState txs = (TXEntryState)txes;

      final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
          .getInstance();
      if (observer != null) {
        observer.lockingRowForTX(tx.getProxy(),
            (GemFireContainer)region.getUserAttribute(),
            txs.getUnderlyingRegionEntry(), true);
      }
      return txs.getRetainedValueInTXOrRegion();
    }
    else {
      return null;
    }
  }

  public final boolean hasProjection() {
    return this.hasProjection;
  }

  /**
   * @see GfxdFunctionMessage#isHA()
   */
  @Override
  public final boolean isHA() {
    return true;
  }

  /**
   * @see GfxdFunctionMessage#optimizeForWrite()
   */
  @Override
  public final boolean optimizeForWrite() {
    return false;
  }

  /**
   * @see GfxdFunctionMessage#requiresTXFlushAfterExecution()
   */
  @Override
  protected boolean requiresTXFlushAfterExecution() {
    // to flush any entries added by loader
    return this.hasLoader ? true : super.requiresTXFlushAfterExecution();
  }

  /**
   * @see GfxdFunctionMessage#clone()
   */
  @Override
  protected GetExecutorMessage clone() {
    return new GetExecutorMessage(this);
  }

  /**
   * @see TransactionMessage#canStartRemoteTransaction()
   */
  @Override
  public final boolean canStartRemoteTransaction() {
    return this.canStartTX;
  }

  @Override
  public final int getMessageProcessorType() {
    // don't use SERIAL_EXECUTOR if we may have to wait for a pending TX
    // else if may deadlock as the p2p msg reader thread will be blocked
    // don't use SERIAL_EXECUTOR for RepeatableRead isolation level else
    // it can block P2P reader thread thus blocking any possible commits
    // which could have released the SH lock this thread is waiting on
    if (!this.forceUseOfPRExecutor && this.pendingTXId == null
        && !this.hasLoader && !this.canStartTX) {
      // Make this serial so that it will be processed in the p2p msg reader
      // which gives it better performance.
      return DistributionManager.SERIAL_EXECUTOR;
    }
    else {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    }
  }

  /**
   * @see AbstractOperationMessage#computeCompressedShort(short)
   */
  @Override
  protected final short computeCompressedShort(short flags) {
    flags = super.computeCompressedShort(flags);
    if (this.hasProjection) {
      flags |= HAS_PROJECTION;
    }
    if (this.hasLoader) {
      flags |= HAS_LOADER;
    }
    if (this.canStartTX) {
      flags |= CAN_START_TX;
    }
    if (this.forUpdate) {
      flags |= FOR_UPDATE;
    }
    return flags;
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    final long beginTime = this.timeStatsEnabled ? XPLAINUtil
        .recordTiming(ser_deser_time == 0 ? ser_deser_time = -1 /*record*/
        : -2 /*ignore nested call*/) : 0;
    super.toData(out);

    // write the projection
    if (this.hasProjection) {
      writeUIntArray(this.projectionFixedColumns, out);
      writeUIntArray(this.projectionVarColumns, out);
      writeUIntArray(this.projectionLobColumns, out);
      out.writeByte(this.targetFormatOffsetBytes);
    }
    // NOTE: this could have been part of flags, but someone added as a separate
    // boolean increasing payload unnecessarily; not changing this now to
    // avoid rolling upgrade version checks but any new flags should be added
    // in flags like HAS_PROJECTION above; if they have been exhausted then
    // make the boolean below as a byte having bitmasks (with 0x1 for
    // QUERY_HDFS) so then 7 more flags will be available
    out.writeBoolean(this.queryHDFS);

    if (beginTime != 0) {
      this.ser_deser_time = XPLAINUtil.recordTiming(beginTime);
    }
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    ser_deser_time = this.timeStatsEnabled ? (ser_deser_time == 0 ? -1 /*record*/
        : -2/*ignore nested call*/) : 0;
    super.fromData(in);

    final short flags = this.flags;
    // read any projection
    this.hasProjection = (flags & HAS_PROJECTION) != 0;
    if (this.hasProjection) {
      this.projectionFixedColumns = readIntArray(in);
      this.projectionVarColumns = readIntArray(in);
      this.projectionLobColumns = readIntArray(in);
      this.targetFormatOffsetBytes = in.readByte();
    }
    this.queryHDFS = in.readBoolean();

    this.hasLoader = ((flags & HAS_LOADER) != 0);
    this.canStartTX = ((flags & CAN_START_TX) != 0);
    this.forUpdate = ((flags & FOR_UPDATE) != 0);

    // recording end of de-serialization here instead of AbstractOperationMessage.
    if (this.timeStatsEnabled && ser_deser_time == -1) {
      this.ser_deser_time = XPLAINUtil.recordStdTiming(getTimestamp());
    }
  }

  @Override
  public byte getGfxdID() {
    return GET_EXECUTOR_MSG;
  }

  static void writeUIntArray(final int[] v, final DataOutput out)
      throws IOException {
    if (v != null) {
      InternalDataSerializer.writeArrayLength(v.length, out);
      for (int index = 0; index < v.length; index++) {
        InternalDataSerializer.writeUnsignedVL(v[index], out);
      }
    }
    else {
      InternalDataSerializer.writeArrayLength(-1, out);
    }
  }

  static int[] readIntArray(final DataInput in) throws IOException {
    final int size = InternalDataSerializer.readArrayLength(in);
    if (size != -1) {
      final int[] v = new int[size];
      for (int index = 0; index < size; index++) {
        v[index] = (int)InternalDataSerializer.readUnsignedVL(in);
      }
      return v;
    }
    else {
      return null;
    }
  }

  @Override
  protected String getID() {
    return ID;
  }

  @Override
  protected void appendFields(final StringBuilder sb) {
    super.appendFields(sb);
    if (this.hasProjection) {
      if (this.projectionFixedColumns != null) {
        sb.append(";projectionFixedColumns=").append(
            Arrays.toString(this.projectionFixedColumns));
      }
      if (this.projectionVarColumns != null) {
        sb.append(";projectionVarColumns=").append(
            Arrays.toString(this.projectionVarColumns));
      }
      if (this.projectionLobColumns != null) {
        sb.append(";projectionLobColumns=").append(
            Arrays.toString(this.projectionLobColumns));
      }
    }
    if (this.forUpdate) {
      sb.append(";forUpdate=true");
    }
    if (this.hasLoader) {
      sb.append(";hasLoader=true");
    }
    if (this.canStartTX) {
      sb.append(";canStartTX=true");
    }
    sb.append(";queryHDFS=").append(this.queryHDFS);
  }
}
