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


import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * 
 * @author ymahajan
 *
 */
public class FunctionStreamingOrderedReplyMessage extends
    FunctionStreamingReplyMessage {

  public FunctionStreamingOrderedReplyMessage() {
    super();
  }

  public FunctionStreamingOrderedReplyMessage(AbstractOperationMessage txMsg,
      boolean sendTXChanges) {
    super(txMsg, sendTXChanges);
  }

  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DM dm, Object result, int msgNum,
      boolean lastMsg, AbstractOperationMessage txMsg, boolean sendTXChanges) {
    FunctionStreamingOrderedReplyMessage m =
        new FunctionStreamingOrderedReplyMessage(txMsg, sendTXChanges);
    m.processorId = processorId;
    if (exception != null) {
      m.setException(exception);
      dm.getLoggerI18n().fine("Replying with exception: " + m, exception);
    }
    m.setRecipient(recipient);
    m.msgNum = msgNum;
    m.lastMsg = lastMsg;
    m.result = result;
    dm.putOutgoing(m);
  }
  
  @Override
  public int getDSFID() {
    return FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE;
  }

  @Override
  final public int getProcessorType() {
    return DistributionManager.SERIAL_EXECUTOR;
  }
}
