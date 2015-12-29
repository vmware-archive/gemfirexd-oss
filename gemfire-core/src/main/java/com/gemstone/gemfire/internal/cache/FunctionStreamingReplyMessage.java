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
import java.io.NotSerializableException;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public class FunctionStreamingReplyMessage extends ReplyMessage {

  /** the number of this message */
  protected int msgNum;
  
  /** whether this message is the last one in this series */
  protected boolean lastMsg;

  protected Object result;

  protected transient AbstractOperationMessage txMsg;

  protected transient boolean sendTXChanges;

  protected static final byte IS_LAST = 0x1;
  protected static final byte HAS_TX_CHANGES = 0x2;

  public FunctionStreamingReplyMessage() {
    super();
  }

  public FunctionStreamingReplyMessage(AbstractOperationMessage txMsg,
      boolean sendTXChanges) {
    super(null, false, false);
    this.txMsg = txMsg;
    this.sendTXChanges = sendTXChanges;
  }

  /**
   * @param msgNum message number in this series (0-based)
   * @param lastMsg if this is the last message in this series
   */
  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DM dm, Object result, int msgNum,
      boolean lastMsg, AbstractOperationMessage txMsg, boolean sendTXChanges) {
    FunctionStreamingReplyMessage m = new FunctionStreamingReplyMessage(txMsg,
        sendTXChanges);
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

  public int getMessageNumber() {
    return this.msgNum;
  }
  
  public boolean isLastMessage() {
    return this.lastMsg;
  }
  
  public Object getResult() {
    return this.result;
  }
  
  @Override
  public int getDSFID() {
    return FUNCTION_STREAMING_REPLY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.msgNum = in.readInt();
    this.processorId = in.readInt();
    try {
      this.result = DataSerializer.readObject(in);
      byte flags = in.readByte();
      this.lastMsg = (flags & IS_LAST) != 0;
      if ((flags & HAS_TX_CHANGES) != 0) {
        this.txChanges = TXChanges.fromData(in);
      }
    }
    catch (Exception e) { // bug fix 40670
      // Seems odd to throw a NonSerializableEx when it has already been
      // serialized and we are failing because we can't deserialize.
      NotSerializableException ioEx = new NotSerializableException();
      ioEx.initCause(e);
      throw ioEx;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    byte flags = this.lastMsg ? IS_LAST : 0;
    out.writeInt(this.msgNum);
    out.writeInt(this.processorId);
    
    //soubhik. fix for ticket 40670
    try {
      DataSerializer.writeObject(this.result, out);
      if (this.lastMsg) {
        super.finishTX(this.txMsg, this.sendTXChanges, true);
      }
      if (this.txChanges != null) {
        flags |= HAS_TX_CHANGES;
      }
      out.writeByte(flags);
      if (this.txChanges != null) {
        this.txChanges.toData(out);
      }
    } 
    catch(Exception ex) {
      if (ex instanceof CancelException) {
        throw new DistributedSystemDisconnectedException(ex);
      }
      NotSerializableException ioEx = new NotSerializableException(this.result
          .getClass().getName());
      ioEx.initCause(ex);
      throw ioEx;
    }
  }

  @Override
  protected StringBuilder getStringBuilder() {
    final StringBuilder buff = super.getStringBuilder();
    if (this.getException() == null) {
      buff.append(" result=").append(this.result);
    }
    buff.append(" msgNum=").append(this.msgNum);
    buff.append(" lastMsg=").append(this.lastMsg);
    if (this.txChanges != null) {
      buff.append(' ');
      this.txChanges.toString(buff);
    }
    return buff;
  }
}
