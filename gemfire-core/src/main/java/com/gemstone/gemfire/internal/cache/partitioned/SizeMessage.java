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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.AbstractOperationMessage;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.SizeEntry;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This message is used to determine the number of Entries in a Region, or its
 * size.
 * 
 * @author mthomas
 * @since 5.0
 */
public final class SizeMessage extends PartitionMessage
  {
  /** The list of buckets whose size is needed, if null, then all buckets */
  private ArrayList<Integer> bucketIds;
  
  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public SizeMessage() {}

  // reuse the NOTIFICATION_ONLY flag since it is not used for SizeMessage 
  /** flag to indicate that only an estimated size is required */
  public static final short ESTIMATE = NOTIFICATION_ONLY;

  private transient boolean estimate;

  /**
   * The message sent to a set of {@link InternalDistributedMember}s to caculate the
   * number of Entries in each of their buckets
   * 
   * @param recipients
   *          members to receive the message
   * @param tx
   *          the current TX state
   * @param regionId
   *          the <code>PartitionedRegion<code> regionId
   * @param processor the reply processor used to wait on the response
   * @param bucketIds the list of bucketIds to get the size for or null for all buckets
   */
  private SizeMessage(Set recipients, int regionId, ReplyProcessor21 processor,
      TXStateInterface tx, ArrayList<Integer> bucketIds, boolean estimate) {
    super(recipients, regionId, processor, tx);
    if (bucketIds != null && bucketIds.isEmpty()) {
      this.bucketIds = null;
    } else {
      this.bucketIds = bucketIds;
    }
    this.estimate = estimate;
  }

  /**
   * sends a message to the given recipients asking for the size of either
   * their primary bucket entries or the values sets of their primary
   * buckets
   * @param recipients recipients of the message
   * @param r the local PartitionedRegion instance
   * @param tx the current TX state
   * @param bucketIds the buckets to look for, or null for all buckets
   * @param estimate true if only a estimate is required
   */
  public static SizeResponse send(Set recipients, PartitionedRegion r,
      TXStateInterface tx, ArrayList<Integer> bucketIds, boolean estimate) {
    Assert.assertTrue(recipients != null, "SizeMessage NULL recipients set");
    SizeResponse p = new SizeResponse(r.getSystem(), recipients);
    SizeMessage m = new SizeMessage(recipients, r.getPRId(), p, tx, bucketIds, estimate);
    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is
   * completely initialized due to the RegionAdvisor(s) knowing about the
   * existance of a partitioned region at a very early part of the
   * initialization
   */
  @Override
  protected final boolean failIfRegionMissing() {
    return false;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected void setBooleans(short s) {
    this.estimate = ((s & ESTIMATE) != 0);
  }

  @Override
  protected short computeCompressedShort(short s) {
    if (this.estimate) s |= ESTIMATE;
    return s;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion r, long startTime) throws CacheException, ForceReattemptException {
    LogWriterI18n l;
    Map<Integer, SizeEntry> sizes;
    
    l = dm.getLoggerI18n();
//    if (DistributionManager.VERBOSE) {  this was causing an NPE when r==null
//      l.fine(getClass().getName() + " operateOnRegion: " + r.getFullPath());
//    }
    if (r != null) {
      PartitionedRegionDataStore ds = r.getDataStore();
      if (ds != null) { // datastore exists
        if (this.bucketIds != null) {
          if (estimate) {
            sizes = ds.getSizeEstimateLocallyForBuckets(this.bucketIds);
          } else {
            sizes = ds.getSizeLocallyForBuckets(this.bucketIds);
          }
        }
        else {
          if (estimate) {
            sizes = ds.getSizeEstimateForLocalPrimaryBuckets();
          } else {
            sizes = ds.getSizeForLocalBuckets();
          }
        }
//        if (DistributionManager.VERBOSE) {
//          l.fine(getClass().getName() + " send sizes back using processorId: "
//              + getProcessorId());
//        }
        r.getPrStats().endPartitionMessagesProcessing(startTime); 
        SizeReplyMessage.send(getSender(), getProcessorId(), dm, sizes, this);
      } // datastore exists
      else {
        l.warning(LocalizedStrings.SizeMessage_SIZEMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER);
        ReplyMessage.send(getSender(), getProcessorId(),
            new ReplyException(new ForceReattemptException(LocalizedStrings
                .SizeMessage_0_1_NO_DATASTORE_HERE_2.toLocalizedString())),
                dm, this, r.isInternalRegion());
      }
    }
    else {
      l.warning(LocalizedStrings.SizeMessage_SIZEMESSAGE_REGION_NOT_FOUND_FOR_THIS_MEMBER,regionId);
      ReplyMessage
          .send(getSender(),getProcessorId(),new ReplyException(
                  new ForceReattemptException(
                      LocalizedStrings.SizeMessage_0_COULD_NOT_FIND_PARTITIONED_REGION_WITH_ID_1
                          .toLocalizedString(new Object[] {
                              dm.getDistributionManagerId(),
                              Integer.valueOf(regionId) }))), dm, this,
                              r != null && r.isInternalRegion());
    }
    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketIds=").append(this.bucketIds);
  }

  public int getDSFID() {
    return PR_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketIds = DataSerializer.readArrayList(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeArrayList(this.bucketIds, out);
  }

  public static final class SizeReplyMessage extends ReplyMessage
   {
    /** Propagated exception from remote node to operation initiator */
    private Map<Integer, SizeEntry> bucketSizes;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public SizeReplyMessage() {
    }

    private SizeReplyMessage(int processorId,
        Map<Integer, SizeEntry> bucketSizes, AbstractOperationMessage srcMessage) {
      super(srcMessage, true, true);
      this.processorId = processorId;
      this.bucketSizes = bucketSizes;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
        int processorId, DM dm, Map<Integer, SizeEntry> sizes,
        AbstractOperationMessage srcMessage) {
      Assert.assertTrue(recipient != null,
          "SizeReplyMessage NULL reply message");
      SizeReplyMessage m = new SizeReplyMessage(processorId, sizes, srcMessage);
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
        l.fine(getClass().getName()
            + " process invoking reply processor with processorId:"
            + this.processorId);
      }

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          l.info(LocalizedStrings.SizeMessage_0_PROCESSOR_NOT_FOUND, getClass().getName());
        }
        return;
      }
      processor.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.SizeMessage_0__PROCESSED__1, new Object[] {processor, this});
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.bucketSizes, out);
    }

    @Override
  public int getDSFID() {
    return PR_SIZE_REPLY_MESSAGE;
  }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.bucketSizes = (Map<Integer, SizeEntry>)DataSerializer.readObject(in);
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append(this.getClass().getName()).append(" processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender()).append(" returning bucketSizes.size=")
          .append(getBucketSizes().size());
      return sb.toString();
    }

    public Map<Integer, SizeEntry> getBucketSizes()
    {
      return this.bucketSizes;
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage}
   * 
   * @author mthomas
   * @since 5.0
   */
  public static class SizeResponse extends ReplyProcessor21
   {
    private final HashMap<Integer, SizeEntry> returnValue = new HashMap<Integer, SizeEntry>();

    public SizeResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof SizeReplyMessage) {
          SizeReplyMessage reply = (SizeReplyMessage)msg;
          synchronized (this.returnValue) {
            for (Map.Entry<Integer, SizeEntry> me : reply.getBucketSizes().entrySet()) {
              Integer k = me.getKey();
              if (!this.returnValue.containsKey(k) || !this.returnValue.get(k).isPrimary()) {
                this.returnValue.put(k, me.getValue());
              }
            }
          }
        }
      }
      finally {
        super.process(msg);
      }
    }
    
    

    @Override
    protected synchronized void processException(ReplyException ex) {
      //Ignore exceptions due to the remote side closing the cache
      if(!(ex.getCause() instanceof CancelException)
          && !(ex.getCause() instanceof ForceReattemptException)
          && !(ex.getCause() instanceof PrimaryBucketException)) {
        super.processException(ex);
      }
    }

    /**
     * @return Map buckets and their associated sizes
     */
    public Map<Integer, SizeEntry> waitBucketSizes()
    {
      try {
        waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        getDistributionManager().getLoggerI18n().fine(
            getClass().getName() + " waitBucketSizes caught exception", e);
        throw e;
      }
      synchronized (this.returnValue) {
        return this.returnValue;
      }
    }
  }

}
