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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 *
 * @author Eric Zoerner
 *
 */
  
/** Creates a new instance of CloseCacheMessage */
public final class CloseCacheMessage extends HighPriorityDistributionMessage
  implements MessageWithReply {
  
  private int processorId;
  
  @Override
  public int getProcessorId() {
    return this.processorId;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }
  
  @Override
  protected void process(DistributionManager dm) {
    // Now that Cache.close calls close on each region we don't need
    // any of the following code so we can just do an immediate ack.
    boolean systemError = false;
    try {
      try {
          PartitionedRegionHelper.cleanUpMetaDataOnNodeFailure(getSender());
      }
      catch (Throwable t) {
        Error err;
        if (t instanceof Error && SystemFailure.isJVMFailureError(
            err = (Error)t)) {
          systemError = true;
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
        dm.getLoggerI18n().fine("Throwable caught while processing cache close message from:"+getSender(),t);
      }
    } finally {
      if (!systemError) {
        ReplyMessage.send(getSender(), processorId, null, dm, false, false,
            null, true);
      }
    }
  }
  
  public void setProcessorId(int id) {
    this.processorId = id;
  }
  
  @Override
  public String toString() {
    return super.toString() + " (processorId=" + processorId + ")";
  }

  public int getDSFID() {
    return CLOSE_CACHE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
  }
}
 
