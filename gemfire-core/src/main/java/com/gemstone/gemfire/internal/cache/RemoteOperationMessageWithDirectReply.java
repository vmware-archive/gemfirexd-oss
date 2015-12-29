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

import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;

/**
 * Used for partitioned region messages which support direct ack responses.
 * Direct ack should be used for message with a response from a single member,
 * or responses which are small.
 * 
 * Messages that extend this class *must* reply using the ReplySender returned
 * by
 * {@link DistributionMessage#getReplySender(com.gemstone.gemfire.distributed.internal.DM)}
 * 
 * Additionally, if the ReplyProcessor used for this message extends
 * PartitionResponse, it should pass false for the register parameter of the
 * PartitionResponse.
 * 
 * @author dsmith
 */
public abstract class RemoteOperationMessageWithDirectReply extends
    RemoteOperationMessage implements DirectReplyMessage {

  protected DirectReplyProcessor processor;

  public RemoteOperationMessageWithDirectReply() {
  }

  public RemoteOperationMessageWithDirectReply(Set<?> recipients,
      LocalRegion r, DirectReplyProcessor processor, TXStateInterface tx) {
    super(recipients, r, processor, tx);
    this.processor = processor;
  }

  public RemoteOperationMessageWithDirectReply(
      InternalDistributedMember recipient, LocalRegion r,
      DirectReplyProcessor processor, TXStateInterface tx) {
    super(recipient, r, processor, tx);
    this.processor = processor;
  }

  /**
   * @param original
   */
  public RemoteOperationMessageWithDirectReply(
      RemoteOperationMessageWithDirectReply original) {
    super(original);
    this.processor = original.processor;
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
}
