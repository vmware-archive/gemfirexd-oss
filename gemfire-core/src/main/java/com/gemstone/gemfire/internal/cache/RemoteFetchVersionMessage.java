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
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This message is used to request a VersionTag from a remote member.
 * 
 * DistributedRegions with DataPolicy EMPTY, NORMAL, PRELOADED, can use
 * this message to fetch VersionTag for a key.
 * 
 * @author sbawaska
 * @since 7.0
 */
public final class RemoteFetchVersionMessage extends RemoteOperationMessage {

  private Object key;

  /** for deserialization */
  public RemoteFetchVersionMessage() {
  }

  /**
   * Send RemoteFetchVersionMessage to the recipient for the given key
   * @param recipient
   * @param r
   * @param key
   * @return the processor used to fetch the VersionTag for the key
   * @throws RemoteOperationException if the member is no longer available
   */
  public static FetchVersionResponse send(InternalDistributedMember recipient,
      LocalRegion r, Object key) throws RemoteOperationException {
    FetchVersionResponse response = new FetchVersionResponse(r.getSystem(),
        recipient);
    RemoteFetchVersionMessage msg = new RemoteFetchVersionMessage(recipient, r,
        response, key);
    Set<?> failures = r.getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.GetMessage_FAILED_SENDING_0.toLocalizedString(msg));
    }
    return response;
  }

  private RemoteFetchVersionMessage(InternalDistributedMember recipient,
      LocalRegion r, ReplyProcessor21 processor, Object key) {
    super(recipient, r, processor, null);
    this.key = key;
  }

  @Override
  public int getDSFID() {
    return R_FETCH_VERSION_MESSAGE;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected int getMessageProcessorType() {
    return DistributionManager.SERIAL_EXECUTOR;
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  protected short computeCompressedShort(short flags) {
    return flags;
  }

  @Override
  protected boolean operateOnRegion(DistributionManager dm, LocalRegion r,
      long startTime) throws RemoteOperationException {
    if (!(r instanceof PartitionedRegion)) {
      r.waitOnInitialization();
    }
    VersionTag tag;
    try {
      if (r.keyRequiresRegionContext()) {
        ((KeyWithRegionContext) this.key).setRegionContext(r);
      }
      RegionEntry re = r.getRegionEntry(key);
      if (re == null) {
        if (DistributionManager.VERBOSE) {
          dm.getLoggerI18n().fine("RemoteFetchVersionMessage did not find entry for key:"+key);
        }
        r.checkEntryNotFound(key);
      }
      tag = re.getVersionStamp().asVersionTag();
      if (DistributionManager.VERBOSE) {
        dm.getLoggerI18n().fine("RemoteFetchVersionMessage for key:"+key+" returning tag:"+tag);
      }
      FetchVersionReplyMessage.send(getSender(), processorId, tag, dm);

    } catch (EntryNotFoundException e) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r,
          startTime);
    }
    return false;
  }

  /**
   * This message is used to send a reply for RemoteFetchVersionMessage.
   * 
   * @author sbawaska
   */
  public static final class FetchVersionReplyMessage extends ReplyMessage {
    private VersionTag tag;

    /** for deserialization */
    public FetchVersionReplyMessage() {
    }

    private FetchVersionReplyMessage(int processorId, VersionTag tag) {
      setProcessorId(processorId);
      this.tag = tag;
    }

    public static void send(InternalDistributedMember recipient,
        int processorId, VersionTag tag, DM dm) {
      FetchVersionReplyMessage reply = new FetchVersionReplyMessage(
          processorId, tag);
      reply.setRecipient(recipient);
      dm.putOutgoing(reply);
    }

    @Override
    public void process(DM dm, ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      if (DistributionManager.VERBOSE) {
        l.fine("FetchVersionReplyMessage process invoking reply processor with processorId:"
            + this.processorId);
      }

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          l.fine("FetchVersionReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.RemoteFetchEntryMessage_0__PROCESSED__1,
            new Object[] { processor, this });
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_FETCH_VERSION_REPLY;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.tag, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.tag = DataSerializer.readObject(in);
    }
  }

  /**
   * A processor to capture the VersionTag returned by RemoteFetchVersion message.
   * 
   * @author sbawaska
   */
  public static class FetchVersionResponse extends RemoteOperationResponse {

    private volatile VersionTag tag;

    public FetchVersionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member) {
      super(dm, member, true);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof FetchVersionReplyMessage) {
          FetchVersionReplyMessage reply = (FetchVersionReplyMessage) msg;
          this.tag = reply.tag;
          if (DistributionManager.VERBOSE || getDistributionManager().getLoggerI18n().fineEnabled()) {
            getDistributionManager().getLoggerI18n().fine(
                "FetchVersionResponse return tag is " + this.tag);
          }
        }
      } finally {
        super.process(msg);
      }
    }
    
    public VersionTag waitForResponse() throws RemoteOperationException {
      try {
        waitForCacheException();
      } catch (RemoteOperationException e) {
        getDistributionManager().getLoggerI18n().fine("RemoteFetchVersionMessage threw", e);
        throw e;
      } catch (EntryNotFoundException e) {
        getDistributionManager().getLoggerI18n().fine("RemoteFetchVersionMessage threw", e);
        throw e;
      } catch (CacheException e) {
        getDistributionManager().getLoggerI18n().fine("RemoteFetchVersionMessage threw", e);
        throw new RemoteOperationException("RemoteFetchVersionMessage threw exception", e);
      }
      return tag;
    }
  }
}
