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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.FunctionStreamingOrderedReplyMessage;
import com.gemstone.gemfire.internal.cache.FunctionStreamingReplyMessage;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.TransactionMessage;
import com.gemstone.gemfire.internal.cache.execute.FunctionRemoteContext;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class PartitionedRegionFunctionStreamingMessage extends PartitionMessage {
  
  private boolean replyLastMsg;

  private int replyMsgNum;

  //private Object result;
  
  private FunctionRemoteContext context ;  

  public PartitionedRegionFunctionStreamingMessage() {
    super();
  }
  
  public PartitionedRegionFunctionStreamingMessage(
      InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, FunctionRemoteContext context) {
    super(recipient, regionId, processor, context.getTXState());
    this.context = context;
  }

  public PartitionedRegionFunctionStreamingMessage(DataInput in)
      throws IOException, ClassNotFoundException {
    fromData(in);
  }

  @Override
  final public int getMessageProcessorType() {
    return DistributionManager.REGION_FUNCTION_EXECUTION_EXECUTOR;
  }

  /**
   * An operation upon the messages partitioned region. Here we have to execute
   * the function and send the result one by one.
   */
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion r, long startTime) {
    if (DistributionManager.VERBOSE) {
      r.getCache().getLogger().fine(
          "PartitionedRegionFunctionResultStreamerMessage operateOnRegion: "
              + r.getFullPath());
    }

    
    if (this.context.getFunction() == null) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(
          new FunctionException(LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(this.context.getFunctionId()))), r, startTime);
      return false;
    }
    PartitionedRegionDataStore ds = r.getDataStore();
    if (ds != null) {
      // check if the routingKeyorKeys is null
      // if null call executeOnDataStore otherwise execute on LocalBuckets
      ds.executeOnDataStore(context.getFilter(), context.getFunction(), context
          .getArgs(), getProcessorId(), context.getBucketSet(), context
          .isReExecute(), this, startTime, null);
              
      if (!this.replyLastMsg && context.getFunction().hasResult()) {
        sendReply(
            getSender(),
            getProcessorId(),
            dm,
            new ReplyException(
                new FunctionException(
                    LocalizedStrings.ExecuteFunction_THE_FUNCTION_0_DID_NOT_SENT_LAST_RESULT
                        .toString(context.getFunction().getId()))), r,
            startTime);
        return false;
      }
    }
    else {
      throw new InternalError(
          "PartitionedRegionFunctionResultStreamerMessage sent to an accessor vm :"
              + dm.getId().getId());
    }
    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  public final boolean canStartRemoteTransaction() {
    return true;
  }

  /**
   * It sends message one by one until it gets lastMessage. Have to handle
   * scenario when message is large, in that case message will be broken into
   * chunks and then sent across.
   */
  public synchronized boolean sendReplyForOneResult(DM dm, PartitionedRegion pr,
      long startTime, Object oneResult, boolean lastResult,
      boolean sendResultsInOrder, boolean sendTXChanges) throws CacheException,
      ForceReattemptException, InterruptedException {
    if(this.replyLastMsg) {
      return false;
    }
    if (Thread.interrupted())
      throw new InterruptedException();
    LogWriterI18n logger = pr.getLogWriterI18n();
    int msgNum = this.replyMsgNum;
    this.replyLastMsg = lastResult;

    sendReply(getSender(), this.processorId, dm, null, oneResult, pr, startTime,
        msgNum, lastResult, sendResultsInOrder, sendTXChanges);

    if (logger.fineEnabled()) {
      logger.fine("Sending reply message count: " + replyMsgNum
          + " to co-ordinating node");
    }
    
    this.replyMsgNum++;
    return false;
  }
  
    
  protected void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, Object result, PartitionedRegion pr, long startTime,
      int msgNum, boolean lastResult, boolean sendResultsInOrder,
      boolean sendTXChanges) {
    // if there was an exception, then throw out any data
    if (ex != null) {
      //this.result = null;
      this.replyMsgNum = 0;
      this.replyLastMsg = true;
    }
    if (this.replyLastMsg) {
      if (pr != null && startTime > 0) {
        pr.getPrStats().endPartitionMessagesProcessing(startTime);
      }
    }
    if (sendResultsInOrder) {
      FunctionStreamingOrderedReplyMessage.send(member, procId, ex, dm, result,
          msgNum, lastResult, this, sendTXChanges);
    }
    else {
      FunctionStreamingReplyMessage.send(member, procId, ex, dm, result,
          msgNum, lastResult, this, sendTXChanges);
    }
  }

  /**
   * @see TransactionMessage#useTransactionProxy()
   */
  @Override
  public final boolean useTransactionProxy() {
    return true;
  }

  public int getDSFID() {
    return PR_FUNCTION_STREAMING_MESSAGE;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; functionRemoteContext=").append(this.context);
    buff.append("; replyMsgNum=").append(this.replyMsgNum);
    buff.append("; replyLastMsg=").append(this.replyLastMsg);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.context = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.context, out);
  }
}
