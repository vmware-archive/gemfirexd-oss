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

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * This message is used be a replicate region to perform region-level ops like
 * clear() and invalidateRegion().  It is used when the target region has
 * concurrency control enabled so that region-version-vectors must be used to
 * execute these operations.
 * 
 * @since 7.0
 */
public final class RemoteRegionOperation extends
    RemoteOperationMessageWithDirectReply {

  private enum Operation {
    CLEAR,
//    INVALIDATE
  }

  transient private DistributedRegion region;
  private Operation op;

  public RemoteRegionOperation() {
  }

  public static RemoteRegionOperation clear(InternalDistributedMember recipient, DistributedRegion region) {
    return new RemoteRegionOperation(recipient, region, Operation.CLEAR);
  }
  
//  public static RemoteRegionOperation invalidate(InternalDistributedMember recipient, DistributedRegion region) {
//    return new RemoteRegionOperation(recipient, region, Operation.INVALIDATE);
//  }

  private RemoteRegionOperation(InternalDistributedMember recipient,
      DistributedRegion region, Operation op) {
    super(recipient, region, new RemoteOperationResponse(region.getSystem(),
        Collections.singleton(recipient)), null);
    this.op = op;
    this.region = region;
  }

  /**
   */
  public void distribute() throws RemoteOperationException {
    RemoteOperationResponse p = (RemoteOperationResponse)this.processor;

    Set<?> failures = region.getDistributionManager().putOutgoing(this);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          LocalizedStrings.FAILED_SENDING_0.toLocalizedString(this));
    }

    p.waitForCacheException();
  }

  @Override
  protected boolean operateOnRegion(DistributionManager dm,
      LocalRegion r, long startTime) throws CacheException,
      RemoteOperationException
  {
    LogWriterI18n l = r.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("DistributedRemoteRegionOperation operateOnRegion: "
          + r.getFullPath());
    }
    
    
    if ( !(r instanceof PartitionedRegion) ) {
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }

    if (op.equals(Operation.CLEAR)) {
      r.clear();
//    } else {
//      r.invalidateRegion();
    }

    //r.getPrStats().endPartitionMessagesProcessing(startTime); 
    RemoteRegionOperationReplyMessage.send(getSender(), getProcessorId(), getReplySender(dm));

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; operation=").append(this.op);
  }

  public int getDSFID() {
    return R_REGION_OP;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.op = Operation.values()[in.readByte()];
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeByte(this.op.ordinal());
  }

  @Override
  protected short computeCompressedShort(short flags) {
    return flags;
  }

  public static final class RemoteRegionOperationReplyMessage extends
      ReplyMessage
   {

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoteRegionOperationReplyMessage() {
    }

    private RemoteRegionOperationReplyMessage(int processorId) {
      this.processorId = processorId;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender)
    {
      Assert.assertTrue(recipient != null,
          "RemoteRegionOperationReplyMessage NULL reply message");
      RemoteRegionOperationReplyMessage m = new RemoteRegionOperationReplyMessage(
          processorId);
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
    public void process(final DM dm, ReplyProcessor21 processor)
    {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      //if (DistributionManager.VERBOSE) {
      //  l.fine("ContainsKeyValueReplyMessage process invoking reply processor with processorId:"
      //          + this.processorId);
      //}

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          l.fine("RemoteRegionOperationReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_REGION_OP_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("RemoteRegionOperationReplyMessage ").append(
          "processorid=").append(this.processorId).append(" reply to sender ")
          .append(this.getSender());
      return sb.toString();
    }

  }


}