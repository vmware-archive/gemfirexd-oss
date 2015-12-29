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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.util.BlobHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * This message is used as the request for a
 * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation. The reply is
 * sent in a {@link com.gemstone.gemfire.internal.cache.RemoteGetMessage.GetReplyMessage}. 
 * 
 * Replicate regions can use this message to send a Get request to another peer.
 * 
 * @since 6.5
 */
public final class RemoteGetMessage extends RemoteOperationMessageWithDirectReply
  {
  private Object key;

  /** The callback arg of the operation */
  private Object cbArg;

  /** set to true if there is a loader anywhere in the system for the region */
  private transient boolean hasLoader;

  private ClientProxyMembershipID context;

  /**
   * GET can start a TX at REPEATABLE_READ isolation level or if there is a
   * loader in the region
   */
  private boolean canStartTX;

  protected static final short HAS_LOADER = UNRESERVED_FLAGS_START;
  protected static final short CAN_START_TX = (HAS_LOADER << 1);

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteGetMessage() {
  }

  private RemoteGetMessage(final InternalDistributedMember recipient,
      final LocalRegion r, final DirectReplyProcessor processor,
      final TXStateInterface tx, final Object key,
      final Object aCallbackArgument, final ClientProxyMembershipID context) {
    super(recipient, r, processor, tx);
    this.key = key;
    this.cbArg = aCallbackArgument;

    assert r instanceof DistributedRegion;
    final DistributedRegion dr = (DistributedRegion)r;
    this.hasLoader = dr.basicGetLoader() != null
        || dr.hasNetLoader(dr.getCacheDistributionAdvisor());
    if (getTXId() != null) {
      // check for loader in the region
      this.canStartTX = getLockingPolicy().readCanStartTX() || this.hasLoader;
    }
    this.context = context;
  }

  private RemoteGetMessage(final Set<InternalDistributedMember> recipients,
      final LocalRegion r, final DirectReplyProcessor processor,
      final TXStateInterface tx, final Object key,
      final Object aCallbackArgument, final ClientProxyMembershipID context) {
    super(recipients, r, processor, tx);
    this.key = key;
    this.cbArg = aCallbackArgument;

    assert r instanceof DistributedRegion;
    final DistributedRegion dr = (DistributedRegion)r;
    this.hasLoader = dr.basicGetLoader() != null
        || dr.hasNetLoader(dr.getCacheDistributionAdvisor());
    if (getTXId() != null) {
      // check for loader in the region
      this.canStartTX = getLockingPolicy().readCanStartTX() || this.hasLoader;
    }
    this.context = context;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  public final int getMessageProcessorType() {
    // don't use SERIAL_EXECUTOR if we may have to wait for a pending TX
    // else if may deadlock as the p2p msg reader thread will be blocked
    // don't use SERIAL_EXECUTOR for RepeatableRead isolation level else
    // it can block P2P reader thread thus blocking any possible commits
    // which could have released the SH lock this thread is waiting on
    if (this.pendingTXId == null && !this.hasLoader && !this.canStartTX) {
      // Make this serial so that it will be processed in the p2p msg reader
      // which gives it better performance.
      return DistributionManager.SERIAL_EXECUTOR;
    }
    else {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    }
  }

  @Override
  protected final boolean operateOnRegion(final DistributionManager dm,
      LocalRegion r, long startTime) throws RemoteOperationException {
    final GemFireCacheImpl cache = r.getCache();
    final LogWriterI18n logger = cache.getLoggerI18n();
    if (DistributionManager.VERBOSE || logger.fineEnabled()) {
      logger.info(LocalizedStrings.DEBUG, "RemoteGetMessage operateOnRegion: "
          + r.getFullPath());
    }

    final TXStateInterface tx = getTXState(r);
    final InternalDataView view = r.getDataView(tx);
    // now TX is started for GET only in REPEATABLE_READ
    assert getTXId() == null || !this.canStartTX
        || view instanceof TXStateInterface;

    if (!(r instanceof PartitionedRegion) ) { // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }

    final RawValue valueBytes;
    Object val = null;
    try {
      if (r.keyRequiresRegionContext()) {
        ((KeyWithRegionContext)this.key).setRegionContext(r);
      }
      final KeyInfo keyInfo = r.getKeyInfo(this.key, this.cbArg);
      val = view.getSerializedValue(r, keyInfo, false,
          this.context, null, false, false/*for replicate regions*/);
      valueBytes = val instanceof RawValue ? (RawValue)val : RawValue
          .newInstance(val, cache);
      if (DistributionManager.VERBOSE || logger.fineEnabled()) {
        logger.info(LocalizedStrings.DEBUG, "RemoteGetMessage sending "
            + "serialized value " + valueBytes
            + " back via GetReplyMessage using processorId: " + getProcessorId());
      }

      GetReplyMessage.send(getSender(), getProcessorId(), valueBytes,
          getReplySender(dm), this);

      // Unless there was an exception thrown, this message handles sending the
      // response
      return false;
    } catch (DistributedSystemDisconnectedException sde) {
      sendReply(getSender(), this.processorId, dm, new ReplyException(
          new RemoteOperationException(LocalizedStrings
              .GetMessage_OPERATION_GOT_INTERRUPTED_DUE_TO_SHUTDOWN_IN_PROGRESS_ON_REMOTE_VM
                  .toLocalizedString(), sde)), r, startTime);
      return false;
    } catch (PrimaryBucketException pbe) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), r,
          startTime);
      return false;
    } catch (DataLocationException e) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r,
          startTime);
      return false;
    }finally {
      OffHeapHelper.release(val);
    }

   
  }

  @Override
  public final boolean canStartRemoteTransaction() {
    return this.canStartTX;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(this.key).append("; callback arg=")
        .append(this.cbArg);
    if (this.hasLoader) {
      buff.append("; hasLoader=").append(this.hasLoader);
    }
  }

  public int getDSFID() {
    return R_GET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.cbArg = DataSerializer.readObject(in);
    this.context = DataSerializer.readObject(in);
    if ((flags & HAS_LOADER) != 0) this.hasLoader = true;
    if ((flags & CAN_START_TX) != 0) this.canStartTX = true;
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.cbArg, out);
    DataSerializer.writeObject(this.context, out);
  }

  @Override
  protected short computeCompressedShort(short s) {
    if (this.hasLoader) s |= HAS_LOADER;
    if (this.canStartTX) s |= CAN_START_TX;
    return s;
  }

  public void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * Sends a DistributedRegion
   * {@link com.gemstone.gemfire.cache.Region#get(Object)} message   
   * 
   * @param recipient
   *          the member that the get message is sent to
   * @param r
   *          the ReplicateRegion for which get was performed upon
   * @param tx
   *          the current TX state
   * @param key
   *          the object to which the value should be feteched
   * @param requestingClient the client requesting the value
   * @return the processor used to fetch the returned value associated with the
   *         key
   */
  public static RemoteGetResponse send(InternalDistributedMember recipient,
      LocalRegion r, final TXStateInterface tx, final Object key,
      final Object aCallbackArgument, ClientProxyMembershipID requestingClient)
      throws RemoteOperationException {
    if (recipient == null) {
      Assert.fail("RemoteGetMessage NULL recipient");
    }
    final RemoteGetResponse p = new RemoteGetResponse(r.getSystem(),
        Collections.singleton(recipient), key);
    final RemoteGetMessage m = new RemoteGetMessage(recipient, r, p, tx, key,
        aCallbackArgument, requestingClient);
    final Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.GetMessage_FAILED_SENDING_0.toLocalizedString(m));
    }
    return p;
  }

  /**
   * Sends a DistributedRegion
   * {@link com.gemstone.gemfire.cache.Region#get(Object)} message
   * 
   * @param recipients
   *          the members that the get message is sent to for a netsearch
   * @param r
   *          the ReplicateRegion for which get was performed upon
   * @param tx
   *          the current TX state
   * @param key
   *          the object to which the value should be feteched
   * @return the processor used to fetch the returned value associated with the
   *         key
   */
  public static RemoteGetResponse send(
      final Set<InternalDistributedMember> recipients, final LocalRegion r,
      final TXStateInterface tx, final Object key,
      final Object aCallbackArgument,
      final ClientProxyMembershipID requestingClient)
      throws RemoteOperationException {
    if (recipients == null) {
      Assert.fail("RemoteGetMessage NULL recipients");
    }
    final RemoteGetResponse p = new RemoteGetResponse(r.getSystem(),
        recipients, key);
    final RemoteGetMessage m = new RemoteGetMessage(recipients, r, p, tx, key,
        aCallbackArgument, requestingClient);
    final Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.GetMessage_FAILED_SENDING_0.toLocalizedString(m));
    }
    return p;
  }

  /**
   * This message is used for the reply to a
   * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation This is the
   * reply to a {@link RemoteGetMessage}.
   * 
   * Since the {@link com.gemstone.gemfire.cache.Region#get(Object)}operation
   * is used <bold>very </bold> frequently the performance of this class is
   * critical.
   * 
   * @since 6.5
   */
  public static final class GetReplyMessage extends ReplyMessage
   {
    /** 
     * The raw value in the cache which may be serialized to the output stream, if 
     * it is NOT already a byte array 
     */
    private transient RawValue rawVal;

    /**
     * Indicates that the value already a byte array (aka user blob) and does
     * not need de-serialization. Also indicates if the value has been
     * serialized directly as an object rather than as a byte array.
     */
    byte valueIsByteArray;

    // static values for valueIsByteArray
    static final byte VALUE_IS_SERIALIZED_OBJECT = 0;
    static final byte VALUE_IS_BYTES = 1;
    static final byte VALUE_IS_OBJECT = 2;

    /*
     * Used on the fromData side to transfer the value bytes to the requesting
     * thread
     */
    public transient byte[] valueInBytes;

    public transient Object valueObj;

    public transient Version remoteVersion;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public GetReplyMessage() {
    }

    private GetReplyMessage(int processorId, RawValue val,
        RemoteGetMessage sourceMessage) {
      super(sourceMessage, true, true);
      setProcessorId(processorId);
      this.rawVal = val;
      if (val.isValueByteArray()) {
        this.valueIsByteArray = VALUE_IS_BYTES;
      }
      else if (CachedDeserializableFactory.preferObject()) {
        this.valueIsByteArray = VALUE_IS_OBJECT;
      }
      else {
        this.valueIsByteArray = VALUE_IS_SERIALIZED_OBJECT;
      }
    }

    /** GetReplyMessages are always processed in-line */
    @Override
    protected boolean getMessageInlineProcess() {
      return true;
    }

    /**
     * Return the value from the get operation, serialize it bytes as late as
     * possible to avoid making un-neccesary byte[] copies.  De-serialize those 
     * same bytes as late as possible to avoid using precious threads (aka P2P readers). 
     * @param recipient the origin VM that performed the get
     * @param processorId the processor on which the origin thread is waiting
     * @param val the raw value that will eventually be serialized 
     * @param replySender distribution manager used to send the reply
     */
    public static void send(InternalDistributedMember recipient,
        int processorId, RawValue val, ReplySender replySender,
        RemoteGetMessage sourceMessage) throws RemoteOperationException {
      Assert.assertTrue(recipient != null,
          "PRDistribuedGetReplyMessage NULL reply message");
      GetReplyMessage m = new GetReplyMessage(processorId, val, sourceMessage);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      if (DistributionManager.VERBOSE) {
        l.fine("GetReplyMessage process invoking reply processor with processorId:"
            + this.processorId);
      }

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          l.fine("GetReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.GetMessage_0__PROCESSED__1, new Object[] {
            processor, this });
      }
      dm.getStats().incReplyMessageTime(
          DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_GET_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeByte(this.valueIsByteArray);
      if (this.valueIsByteArray == VALUE_IS_BYTES) {
        DataSerializer.writeByteArray((byte[])this.rawVal.getRawValue(), out);
      }
      else if (this.valueIsByteArray == VALUE_IS_OBJECT) {
        DataSerializer.writeObject(this.rawVal.getRawValue(), out);
      }
      else {
        this.rawVal.writeAsByteArray(out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.valueIsByteArray = in.readByte();
      if (this.valueIsByteArray == VALUE_IS_BYTES) {
        this.valueInBytes = DataSerializer.readByteArray(in);
      }
      else if (this.valueIsByteArray == VALUE_IS_OBJECT) {
        this.valueObj = DataSerializer.readObject(in);
      }
      else {
        this.valueInBytes = DataSerializer.readByteArray(in);
        this.remoteVersion = InternalDataSerializer
            .getVersionForDataStreamOrNull(in);
      }
    }

    @Override
    public String toString()
    {
      final StringBuilder sb = new StringBuilder();
      sb.append("GetReplyMessage ").append("processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender())
          .append(" returning serialized value=")
          .append(this.rawVal);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.RemoteGetMessage.GetReplyMessage}
   * 
   * @author Mitch Thomas
   * @since 5.0
   */
  public static final class RemoteGetResponse extends RemoteOperationResponse {

    private volatile GetReplyMessage getReply;
    private volatile boolean returnValueReceived;
    private volatile long start;
    final Object key;

    public RemoteGetResponse(InternalDistributedSystem ds, Set<?> recipients,
        Object key) {
      super(ds, recipients, false);
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg) {
      if (DistributionStats.enableClockStats) {
        this.start = DistributionStats.getStatTime();
      }
      if (msg instanceof GetReplyMessage) {
        final GetReplyMessage reply = (GetReplyMessage)msg;
        if (reply.valueInBytes != null || reply.valueObj != null) {
          this.getReply = reply;
        }
        this.returnValueReceived = true;
      }
      super.process(msg);
    }

    /**
     * De-seralize the value, if the value isn't already a byte array, this
     * method should be called in the context of the requesting thread for the
     * best scalability
     * 
     * @param preferCD 
     * @see EntryEventImpl#deserialize(byte[])
     * @return the value object
     */
    public Object getValue(boolean preferCD) throws RemoteOperationException {
      final GetReplyMessage reply = this.getReply;
      try {
        if (reply != null) {
          if (reply.valueIsByteArray == GetReplyMessage.VALUE_IS_BYTES) {
            return reply.valueInBytes;
          }
          else if (reply.valueObj != null) {
            return reply.valueObj;
          }
          else if (preferCD && !CachedDeserializableFactory.preferObject()) {
            return CachedDeserializableFactory.create(reply.valueInBytes);
          }
          else {
            return BlobHelper.deserializeBlob(reply.valueInBytes,
                reply.remoteVersion, null);
          }
        }
        return null;
      } catch (IOException e) {
        throw new RemoteOperationException(
            LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_IOEXCEPTION
                .toLocalizedString(), e);
      } catch (ClassNotFoundException e) {
        throw new RemoteOperationException(LocalizedStrings
            .GetMessage_UNABLE_TO_DESERIALIZE_VALUE_CLASSNOTFOUNDEXCEPTION
                .toLocalizedString(), e);
      }
    }

    /**
     * @return Object associated with the key that was sent in the get message
     */
    public Object waitForResponse(boolean preferCD) 
        throws RemoteOperationException {
      try {
        waitForCacheException();
        if (DistributionStats.enableClockStats) {
          getDistributionManager().getStats().incReplyHandOffTime(this.start);
        }
      } catch (RemoteOperationException e) {
        e.checkKey(key);
        final String msg = "RemoteGetResponse throwing RemoteOperationException";
        getDistributionManager().getLoggerI18n().fine(msg, e);
        throw e;
      } catch (TransactionException e) {
        // Throw this up to user!
        throw e;
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(
            LocalizedStrings.GetMessage_NO_RETURN_VALUE_RECEIVED
                .toLocalizedString());
      }
      return getValue(preferCD);
    }
  }
}
