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
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXStateInterface;

/**
 * Used for partitioned region messages which support direct ack responses.
 * Direct ack should be used for message with a response from a single member,
 * or responses which are small.
 * 
 * Messages that extend this class *must* reply using the ReplySender returned
 * by {@link DistributionMessage#getReplySender(com.gemstone.gemfire.distributed.internal.DM)}
 * 
 * Additionally, if the ReplyProcessor used for this message extends PartitionResponse, it should
 * pass false for the register parameter of the PartitionResponse.
 * @author dsmith
 *
 */
public abstract class PartitionMessageWithDirectReply extends
    PartitionMessage implements DirectReplyMessage {

  protected DirectReplyProcessor processor;

  protected boolean posDup = false;

  public PartitionMessageWithDirectReply() {
    super();
  }

  public PartitionMessageWithDirectReply(final TXStateInterface tx) {
    super(tx);
  }

  public PartitionMessageWithDirectReply(
      Collection<InternalDistributedMember> recipients, int regionId,
      DirectReplyProcessor processor, final TXStateInterface tx) {
    super(recipients, regionId, processor, tx);
    this.processor = processor;
    this.posDup = false;
  }

  public PartitionMessageWithDirectReply(Set recipients, int regionId,
      DirectReplyProcessor processor, EntryEventImpl event, TXStateInterface tx) {
    super(recipients, regionId, processor, tx);
    this.processor = processor;
    this.posDup = event.isPossibleDuplicate();
  }

  public PartitionMessageWithDirectReply(InternalDistributedMember recipient,
      int regionId, DirectReplyProcessor processor, TXStateInterface tx) {
    super(recipient, regionId, processor, tx);
    this.processor = processor;
  }

  /**
   * @param original
   */
  public PartitionMessageWithDirectReply(
      PartitionMessageWithDirectReply original, EntryEventImpl event) {
    super(original);
    this.processor = original.processor;
    if (event != null) {
      this.posDup = event.isPossibleDuplicate();
    }
    else {
      this.posDup = original.posDup;
    }
  }

  public boolean supportsDirectAck() {
    return true;
  }
  
  public DirectReplyProcessor getDirectReplyProcessor() {
    return processor;
  }

  public void registerProcessor() {
    this.processorId = processor.register();
  }

  @Override
  public boolean canStartRemoteTransaction() {
    // by default all read/write operations will create TXStateProxies for locks
    // read operations will disable this at READ_COMMITTED isolation level
    return true;
  }

  @Override
  public boolean containsRegionContentChange() {
    // for TX state flush but not for inline processing case since for that
    // case the operation will be received when endOperation has been invoked
    // and CreateRegionMessage was waiting on ops in progress
    return isTransactional() && this.processorId != 0
        && canStartRemoteTransaction();
  }

  @Override
  public Set relayToListeners(Set cacheOpRecipients, Set adjunctRecipients,
      FilterRoutingInfo filterRoutingInfo, 
      EntryEventImpl event, PartitionedRegion r, DirectReplyProcessor p)
  {
    this.processor = p;
    return super.relayToListeners(cacheOpRecipients, adjunctRecipients,
        filterRoutingInfo, event, r, p);
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.posDup) {
      s |= POS_DUP;
    }
    return s;
  }

  @Override
  protected void setBooleans(short s) {
    super.setBooleans(s);
    this.posDup = ((s & POS_DUP) != 0);
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; posDup=").append(this.posDup);
  }
}
