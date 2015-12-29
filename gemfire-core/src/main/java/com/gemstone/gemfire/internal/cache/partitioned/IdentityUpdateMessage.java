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
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;


public final class IdentityUpdateMessage extends DistributionMessage implements MessageWithReply
{
  private int processorId;
  
  private int newId;

  /**
   * Empty constructor to support DataSerializable instantiation
   */
  public IdentityUpdateMessage() {
  }
  
  public IdentityUpdateMessage(Set recipients, int processorId, int newId) {
    setRecipients(recipients);
    this.processorId = processorId;
    this.newId = newId;
  }

  @Override
  public int getProcessorType()
  {
    return DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }

  @Override
  protected void process(DistributionManager dm)
  {
    LogWriterI18n logger = null;
    try {
      logger = dm.getLoggerI18n();
      if (logger.fineEnabled()) {
        logger.fine(getClass().getName() + ": processing message " + this);
      }
      
      IdentityRequestMessage.setLatestId(this.newId);
      
      ReplyMessage.send(getSender(), getProcessorId(), null, dm, null);
    }
    catch (Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fine(this + " Caught throwable", t);
    }
  }

  @Override
  public int getProcessorId()
  {
    return this.processorId;
  }

  public static IdentityUpdateResponse send(Set recipients, InternalDistributedSystem is, int currentPRId)
  {
    Assert.assertTrue(recipients != null, "IdentityUpdateMessage NULL recipients set");
    IdentityRequestMessage.setLatestId(currentPRId); // set local value 
    IdentityUpdateResponse p = new IdentityUpdateResponse(is, recipients);
    IdentityUpdateMessage m = new IdentityUpdateMessage(recipients, p.getProcessorId(), currentPRId);
    is.getDistributionManager().putOutgoing(m);  // set remote values
    return p;
  }
  

  public int getDSFID() {
    return PR_IDENTITY_UPDATE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.processorId = in.readInt();
    this.newId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.processorId);
    out.writeInt(this.newId);
  }
  
  @Override
  public String toString()
  {
    return new StringBuffer()
      .append(getClass().getName())
      .append("(sender=")
      .append(getSender())
      .append("; processorId=")
      .append(this.processorId)
      .append("; newPRId=")
      .append(this.newId)
      .append(")")
      .toString();
  }
  
  /**
   * A processor that ignores exceptions, silently removing those nodes that reply with problems
   * @author mthomas
   * @since 5.0
   */
  public static class IdentityUpdateResponse extends ReplyProcessor21 {
    
    public IdentityUpdateResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /** 
     * The IdentityResponse processor ignores remote exceptions by implmenting this method.  Ignoring remote exceptions is acceptable
     * since the response is only meant to wait for all healthy recipients to receive their {@link IdentityUpdateMessage}
     */
    @Override
    protected synchronized void processException(ReplyException ex)
    {
      getDistributionManager().getLoggerI18n().fine("IdentityUpdateResponse ignoring exception", ex);
    }
  }
}
