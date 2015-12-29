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

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A processor for telling the old grantor that he is deposed by a new grantor. 
 * Processor waits for ack before completing.
 *
 * @since 4.0
 * @author Darrel Schneider
 * @author Kirk Lund (renamed from ExpectTransferProcessor)
 */
public class DeposeGrantorProcessor extends ReplyProcessor21 {
  
  ////////// Public static entry point /////////


  /**
   * Send a message to oldGrantor telling him that he is deposed by newGrantor. 
   * Send does not complete until an ack is received or the oldGrantor leaves
   *  the system.
   */
  static void send(String serviceName, 
                   InternalDistributedMember oldGrantor,
                   InternalDistributedMember newGrantor, 
                   long newGrantorVersion,
                   int newGrantorSerialNumber,
                   DM dm) {
    final InternalDistributedMember elder = dm.getId();
    if (elder.equals(oldGrantor)) {
      doOldGrantorWork(
          serviceName, elder, newGrantor, newGrantorVersion, newGrantorSerialNumber, dm, null);
    } else {
      DeposeGrantorProcessor processor = 
          new DeposeGrantorProcessor(dm, oldGrantor);
      DeposeGrantorMessage.send(
          serviceName, oldGrantor, newGrantor, newGrantorVersion, newGrantorSerialNumber, dm, processor);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        e.handleAsUnexpected();
      }
    }
  }
  static protected void doOldGrantorWork(final String serviceName,
                                         final InternalDistributedMember elder,
                                         final InternalDistributedMember youngTurk,
                                         final long newGrantorVersion,
                                         final int newGrantorSerialNumber,
                                         final DM dm,
                                         final DeposeGrantorMessage msg) {
    try {
      DLockService svc = 
        DLockService.getInternalServiceNamed(serviceName);
      if (svc != null) {
        LockGrantorId newLockGrantorId = 
            new LockGrantorId(dm, youngTurk, newGrantorVersion, newGrantorSerialNumber);
        svc.deposeOlderLockGrantorId(newLockGrantorId);
      }
    }
    finally {
      if (msg != null) {
        msg.reply(dm);
      }
    }
  }
  
  ////////////  Instance methods //////////////
  
  /** Creates a new instance of DeposeGrantorProcessor
   */
  private DeposeGrantorProcessor(DM dm, InternalDistributedMember oldGrantor) {
    super(dm, oldGrantor);
  }

  
  ///////////////   Inner message classes  //////////////////
  
  public static final class DeposeGrantorMessage
    extends PooledDistributionMessage implements MessageWithReply
  {
    private int processorId;
    private String serviceName;
    private InternalDistributedMember newGrantor;
    private long newGrantorVersion;
    private int newGrantorSerialNumber;
    
    protected static void send(String serviceName,
                             InternalDistributedMember oldGrantor,
                             InternalDistributedMember newGrantor,
                             long newGrantorVersion,
                             int newGrantorSerialNumber,
                             DM dm, 
                             ReplyProcessor21 proc)
    {
      DeposeGrantorMessage msg = new DeposeGrantorMessage();
      msg.serviceName = serviceName;
      msg.newGrantor = newGrantor;
      msg.newGrantorVersion = newGrantorVersion;
      msg.newGrantorSerialNumber = newGrantorSerialNumber;
      msg.processorId = proc.getProcessorId();
      msg.setRecipient(oldGrantor);
      if (DLockLogWriter.fineEnabled(dm)) {
        DLockLogWriter.fine(dm, "DeposeGrantorMessage sending " + msg + " to " + oldGrantor);
      }
      dm.putOutgoing(msg);
    }
  
    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    void reply(DM dm) {
      ReplyMessage.send(this.getSender(), this.getProcessorId(),
                        null, dm, null);
    }
    
    @Override
    protected void process(final DistributionManager dm) {
      // if we are currently the grantor then
      // mark it as being destroyed until we hear from this.newGrantor
      // or he goes away or the grantor that sent us this message goes away.
      
      InternalDistributedMember elder = this.getSender();
      InternalDistributedMember youngTurk = this.newGrantor;

      doOldGrantorWork(
          this.serviceName, elder, youngTurk, this.newGrantorVersion, this.newGrantorSerialNumber, dm, this);
    }
    
    public int getDSFID() {
      return DEPOSE_GRANTOR_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.serviceName = DataSerializer.readString(in);
      this.newGrantor = (InternalDistributedMember)DataSerializer.readObject(in);
      this.newGrantorVersion = in.readLong();
      this.newGrantorSerialNumber = in.readInt();
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeString(this.serviceName, out);
      DataSerializer.writeObject(this.newGrantor, out);
      out.writeLong(this.newGrantorVersion);
      out.writeInt(this.newGrantorSerialNumber);
    }
    
    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("DeposeGrantorMessage (serviceName='")
        .append(this.serviceName)
        .append("' processorId=")
        .append(this.processorId)
        .append(" newGrantor=")
        .append(this.newGrantor)
        .append(" newGrantorVersion=")
        .append(this.newGrantorVersion)
        .append(" newGrantorSerialNumber=")
        .append(this.newGrantorSerialNumber)
        .append(")");
      return buff.toString();
    }
  }
}

