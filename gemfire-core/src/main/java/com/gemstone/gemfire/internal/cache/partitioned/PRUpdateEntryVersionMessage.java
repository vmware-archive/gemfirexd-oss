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

package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This message is generated based on event received on GatewayReceiver for
 * updating the time-stamp in a version tag for a RegionEntry.
 * 
 * @author Shobhit Agarwal
 * 
 */
public class PRUpdateEntryVersionMessage extends
    PartitionMessageWithDirectReply {

  /** The key associated with the value that must be sent */
  private Object key;

  /** The operation performed on the sender */
  private Operation op;

  /** event identifier */
  private EventID eventId;

  protected VersionTag versionTag;

  /**
   * for deserialization
   */
  public PRUpdateEntryVersionMessage() {
    super();
  }

  /**
   * @param recipients
   * @param regionId
   * @param processor
   */
  public PRUpdateEntryVersionMessage(
      Collection<InternalDistributedMember> recipients, int regionId,
      DirectReplyProcessor processor) {
    super(recipients, regionId, processor, null /* TXState */);
  }

  /**
   * @param recipients
   * @param regionId
   * @param processor
   * @param event
   */
  public PRUpdateEntryVersionMessage(Set recipients, int regionId,
      DirectReplyProcessor processor, EntryEventImpl event) {
    super(recipients, regionId, processor, event, null /* TXState */);
    this.key = event.getKey();
    this.op = event.getOperation();
    this.eventId = event.getEventId();
    this.versionTag = event.getVersionTag();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return PR_UPDATE_ENTRY_VERSION_MESSAGE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage#
   * operateOnPartitionedRegion
   * (com.gemstone.gemfire.distributed.internal.DistributionManager,
   * com.gemstone.gemfire.internal.cache.PartitionedRegion, long)
   */
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException,
      QueryException, DataLocationException, InterruptedException, IOException {
    LogWriterI18n l = pr.getCache().getLoggerI18n();

    if (pr.keyRequiresRegionContext()) {
      ((KeyWithRegionContext) key).setRegionContext(pr);
    }

    final EntryEventImpl event = EntryEventImpl.create(pr, getOperation(),
        getKey(), null, /* newValue */
        null, /* callbackargs */
        false /* originRemote - false to force distribution in buckets */,
        getSender() /* eventSender */, false /* generateCallbacks */, false /* initializeId */);
    event.disallowOffHeapValues();

    Assert.assertTrue(eventId != null);
    if (this.versionTag != null) {
      event.setVersionTag(this.versionTag);
    }
    event.setEventId(eventId);
    event.setPossibleDuplicate(this.posDup);
    event.setInvokePRCallbacks(false);
    event.setCausedByMessage(this);

    boolean sendReply = true;

    if (!notificationOnly) {

      PartitionedRegionDataStore ds = pr.getDataStore();
      Assert.assertTrue(ds != null,
          "This process should have storage for an item in " + this.toString());
      try {
        Integer bucket = Integer.valueOf(PartitionedRegionHelper
            .getHashKey(event));

        pr.getDataView().updateEntryVersion(event);

        if (DistributionManager.VERBOSE) {
          l.info(LocalizedStrings.ONE_ARG, getClass().getName()
              + " updateEntryVersionLocally in bucket: " + bucket + ", key: "
              + key);
        }
      } catch (EntryNotFoundException eee) {
        // failed = true;
        if (l.fineEnabled()) {
          l.fine(getClass().getName()
              + ": operateOnRegion caught EntryNotFoundException");
        }
        sendReply(getSender(), getProcessorId(), dm, null /*
                                                           * No need to send
                                                           * exception back
                                                           */, pr, startTime);
        sendReply = false; // this prevents us from acknowledging later
      } catch (PrimaryBucketException pbe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe),
            pr, startTime);
        return false;
      }

    }
    return sendReply;
  }

  private Operation getOperation() {
    return this.op;
  }

  private Object getKey() {
    return this.key;
  }

  @Override
  public final void fromData(DataInput in)
          throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    this.eventId = (EventID) DataSerializer.readObject(in);
    this.versionTag = DataSerializer.readObject(in);
  }

  @Override
  public final void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(getKey(), out);
    out.writeByte(this.op.ordinal);
    DataSerializer.writeObject(this.eventId, out);
    DataSerializer.writeObject(this.versionTag, out);
  }

  /**
   * Assists the toString method in reporting the contents of this message
   * 
   * @see PartitionMessage#toString()
   */

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey());
    buff.append("; op=").append(this.op);

    if (eventId != null) {
      buff.append("; eventId=").append(eventId);
    }
    if (this.versionTag != null) {
      buff.append("; version=").append(this.versionTag);
    }
  }

  /**
   * Response for PartitionMessage {@link PRUpdateEntryVersionMessage}.
   * 
   */
  public static final class UpdateEntryVersionResponse extends
      PartitionResponse {

    private volatile boolean versionUpdated;
    private final Object key;

    public UpdateEntryVersionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member, Object k) {
      super(dm, member);
      this.key = k;
    }

    public UpdateEntryVersionResponse(InternalDistributedSystem dm,
        Set recipients, Object k) {
      super(dm, recipients, false, null /* TXState */);
      this.key = k;
    }

    public void setResponse(ReplyMessage msg) {
      this.versionUpdated = true;
    }

    /**
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public void waitForResult() throws CacheException, ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException e) {
        e.checkKey(key);
        throw e;
      }

      if (!this.versionUpdated) {
        final LogWriterI18n logger = this.system.getLogWriter()
            .convertToLogWriterI18n();
        if (logger.finerEnabled()) {
          logger
              .fine("UpdateEntryVersionResponse: Update entry version failed for key: "
                  + key);
        }
      }
    }
  }

  public static UpdateEntryVersionResponse send(
      InternalDistributedMember recipient, PartitionedRegion r,
      EntryEventImpl event) throws ForceReattemptException {
    Set recipients = Collections.singleton(recipient);
    UpdateEntryVersionResponse p = new UpdateEntryVersionResponse(
        r.getSystem(), recipient, event.getKey());
    PRUpdateEntryVersionMessage m = new PRUpdateEntryVersionMessage(recipients,
        r.getPRId(), p, event);

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          LocalizedStrings.UpdateEntryVersionMessage_FAILED_SENDING_0
              .toLocalizedString(m));
    }
    return p;
  }
}