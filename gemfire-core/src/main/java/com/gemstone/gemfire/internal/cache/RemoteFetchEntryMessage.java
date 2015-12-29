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
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This message is used as the request for a
 * {@link com.gemstone.gemfire.cache.Region#getEntry(Object)}operation. The
 * reply is sent in a {@link 
 * com.gemstone.gemfire.internal.cache.RemoteFetchEntryMessage.FetchEntryReplyMessage}.
 * 
 * @author bruce
 * @since 5.1
 */
public final class RemoteFetchEntryMessage extends RemoteOperationMessage
  {
  private Object key;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteFetchEntryMessage() {
  }

  private RemoteFetchEntryMessage(InternalDistributedMember recipient,
      LocalRegion r, ReplyProcessor21 processor, final TXStateInterface tx,
      final Object key) {
    super(recipient, r, processor, tx);
    this.key = key;
  }

  /**
   * Sends a LocalRegion
   * {@link com.gemstone.gemfire.cache.Region#getEntry(Object)} message   
   * 
   * @param recipient
   *          the member that the getEntry message is sent to
   * @param r
   *          the Region for which getEntry was performed upon
   * @param tx
   *          the current TX state
   * @param key
   *          the object to which the value should be feteched
   * @return the processor used to fetch the returned value associated with the
   *         key
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static FetchEntryResponse send(InternalDistributedMember recipient,
      LocalRegion r, final TXStateInterface tx, final Object key)
      throws RemoteOperationException
  {
    Assert.assertTrue(recipient != null, "RemoteFetchEntryMessage NULL recipient");
    FetchEntryResponse p = new FetchEntryResponse(r.getSystem(), Collections
        .singleton(recipient), r, key);
    RemoteFetchEntryMessage m = new RemoteFetchEntryMessage(recipient, r, p,
        tx, key);

    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.RemoteFetchEntryMessage_FAILED_SENDING_0
              .toLocalizedString(m));
    }

    return p;
  }

//  final public int getProcessorType()
//  {
//    return DistributionManager.PARTITIONED_REGION_EXECUTOR;
//  }

  @Override
  public final int getMessageProcessorType() {
    // don't use SERIAL_EXECUTOR if we may have to wait for a pending TX
    // else if may deadlock as the p2p msg reader thread will be blocked
    // don't use SERIAL_EXECUTOR for RepeatableRead isolation level else
    // it can block P2P reader thread thus blocking any possible commits
    // which could have released the SH lock this thread is waiting on
    if (this.pendingTXId == null && !canStartRemoteTransaction()) {
      // Make this serial so that it will be processed in the p2p msg reader
      // which gives it better performance.
      return DistributionManager.SERIAL_EXECUTOR;
    }
    else {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    }
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected final boolean operateOnRegion(DistributionManager dm,
      LocalRegion r, long startTime) throws RemoteOperationException
  {
    // RemoteFetchEntryMessage is used in refreshing client caches during interest list recovery,
    // so don't be too verbose or hydra tasks may time out
//    LogWriterI18n l = r.getCache().getLogger();
//    if (DistributionManager.VERBOSE) {
//      l.fine(this.toString() + "operateOnRegion: " + r.getFullPath());
//    }

    if (!(r instanceof PartitionedRegion)) {
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }
    VersionTag<?> versionTag = null;
    try {
      final TXStateInterface tx = getTXState(r);
      if (r.keyRequiresRegionContext()) {
        ((KeyWithRegionContext)this.key).setRegionContext(r);
      }
      final Region.Entry<?, ?> re = r.getDataView(tx).getEntry(key, null, r,
          true);
      if (re == null) {
        r.checkEntryNotFound(key);
      }
      //r.getPrStats().endRemoteOperationMessagesProcessing(startTime);
      if (re instanceof EntrySnapshot) {
        versionTag = ((EntrySnapshot)re).getVersionTag();
      }
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), re.getValue(),
          versionTag, dm, null, this);
    } catch (TransactionException tex) {
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), null,
          versionTag, dm, new ReplyException(tex), null);
    } catch (EntryNotFoundException enfe) {
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), null,
          versionTag, dm, new ReplyException(LocalizedStrings
              .RemoteFetchEntryMessage_ENTRY_NOT_FOUND.toLocalizedString(),
              enfe), null);
    } catch (PrimaryBucketException pbe) {
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), null,
          versionTag, dm, new ReplyException(pbe), null);
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  public final boolean canStartRemoteTransaction() {
    return getLockingPolicy().readCanStartTX();
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(this.key);
  }

  public int getDSFID() {
    return R_FETCH_ENTRY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  protected short computeCompressedShort(short flags) {
    return flags;
  }

  public void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * This message is used for the reply to a {@link RemoteFetchEntryMessage}.
   * 
   * @author mthomas
   * @since 5.0
   */
  public static final class FetchEntryReplyMessage extends ReplyMessage {

    /** Propagated value from remote node to operation initiator */
    private Object value;

    private VersionTag<?> versionTag;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public FetchEntryReplyMessage() {
    }

    private FetchEntryReplyMessage(int processorId, Object value,
        VersionTag<?> versionTag, ReplyException re,
        RemoteFetchEntryMessage sourceMessage) {
      super(sourceMessage, true, true);
      this.processorId = processorId;
      this.value = value;
      this.versionTag = versionTag;
      setException(re);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
        int processorId, Object value, VersionTag<?> versionTag, DM dm,
        ReplyException re, RemoteFetchEntryMessage sourceMessage) {
      Assert.assertTrue(recipient != null,
          "FetchEntryReplyMessage NULL recipient");
      FetchEntryReplyMessage m = new FetchEntryReplyMessage(processorId, value,
          versionTag, re, sourceMessage);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor)
    {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      if (DistributionManager.VERBOSE) {
        l
            .fine("FetchEntryReplyMessage process invoking reply processor with processorId:"
                + this.processorId);
      }

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          l.fine("FetchEntryReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.RemoteFetchEntryMessage_0__PROCESSED__1, new Object[] {processor, this});
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    public Object getValue() {
      return this.value;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      byte flags = 0x0;
      if (this.value == null) flags = 0x1; // null entry
      if (this.versionTag == null) flags |= 0x2; // no version

      out.writeByte(flags);
      if (this.value != null) {
        DataSerializer.writeObject(this.value, out);
      }
      if (this.versionTag != null) {
        InternalDataSerializer.writeObject(this.versionTag, out);
      }
    }

    @Override
    public int getDSFID() {
      return R_FETCH_ENTRY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      final byte flags = in.readByte();
      if ((flags & 0x1) == 0) {
        // since the Entry object shares state with the PartitionedRegion,
        // we have to find the region and ask it to create a new Entry instance
        // to be populated from the DataInput
        FetchEntryResponse processor = (FetchEntryResponse)ReplyProcessor21
            .getProcessor(this.processorId);
        if (processor == null) {
          throw new InternalGemFireError(
              "FetchEntryReplyMessage had null processor");
        }
        this.value = DataSerializer.readObject(in);
      }
      if ((flags & 0x2) == 0) {
        this.versionTag = DataSerializer.readObject(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("FetchEntryReplyMessage ").append("processorid=")
          .append(this.processorId).append(" reply to sender ")
          .append(this.getSender()).append(" returning value=")
          .append(this.value).append(", version=").append(this.versionTag);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.RemoteFetchEntryMessage.FetchEntryReplyMessage}
   * 
   */
  public static class FetchEntryResponse extends RemoteOperationResponse {

    private volatile NonLocalRegionEntry returnValue;

    final LocalRegion region;
    final Object key;

    public FetchEntryResponse(InternalDistributedSystem ds, Set<?> recipients,
        LocalRegion theRegion, Object key) {
      super(ds, recipients);
      this.region = theRegion;
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof FetchEntryReplyMessage) {
          FetchEntryReplyMessage reply = (FetchEntryReplyMessage)msg;
          final Object value = reply.getValue();
          if (value != null) {
            this.returnValue = NonLocalRegionEntry.newEntry(this.key, value,
                this.region, reply.versionTag);
          }
          if (DistributionManager.VERBOSE) {
            getDistributionManager().getLoggerI18n().fine(
                "FetchEntryResponse return value is " + this.returnValue);
          }
        }
      }
      finally {
        super.process(msg);
      }
    }

    /**
     * @return Object associated with the key that was sent in the get message
     * @throws EntryNotFoundException
     * @throws RemoteOperationException if the peer is no longer available
     * @throws EntryNotFoundException
     */
    public NonLocalRegionEntry waitForResponse() 
        throws EntryNotFoundException, RemoteOperationException {
      try {
        // waitForRepliesUninterruptibly();
        waitForCacheException();
      }
      catch (RemoteOperationException e) {
        e.checkKey(key);
        final String msg = "FetchEntryResponse got remote RemoteOperationException; rethrowing";
        getDistributionManager().getLoggerI18n().fine(msg, e);
        throw e;
      }
      catch (EntryNotFoundException e) {
        throw e;
      }
      catch (TransactionException e) {
        throw e;
      }
      catch (RegionDestroyedException e) {
        throw e;
      }
      catch (CacheException ce) {
        getDistributionManager().getLoggerI18n().fine("FetchEntryResponse got remote CacheException; forcing reattempt.", ce);
        throw new RemoteOperationException(LocalizedStrings.RemoteFetchEntryMessage_FETCHENTRYRESPONSE_GOT_REMOTE_CACHEEXCEPTION_FORCING_REATTEMPT.toLocalizedString(), ce);
      }
      return this.returnValue;
    }
  }

}
