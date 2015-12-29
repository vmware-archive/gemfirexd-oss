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
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;

public class ReleaseClearLockMessage extends
  HighPriorityDistributionMessage implements MessageWithReply {

    private String regionPath;
    private int processorId;

    /** for deserialization */
    public ReleaseClearLockMessage() {
    }

    public ReleaseClearLockMessage(String regionPath, int processorId) {
      this.regionPath = regionPath;
      this.processorId = processorId;
    }
    
    public static void send(
        Set<InternalDistributedMember> members, DM dm, String regionPath) throws ReplyException {
      ReplyProcessor21 processor = new ReplyProcessor21(dm, members);
      ReleaseClearLockMessage msg = new ReleaseClearLockMessage(regionPath, processor.getProcessorId());
      msg.setRecipients(members);

      dm.putOutgoing(msg);
      processor.waitForRepliesUninterruptibly();
    }

    @Override
    protected void process(DistributionManager dm) {
      LogWriterI18n log = dm.getLoggerI18n();
      ReplyException exception = null;
      try {
        DistributedRegion region = DistributedClearOperation.regionUnlocked(getSender(), regionPath);
        if(region != null && region.getVersionVector() != null) {
          region.getVersionVector().unlockForClear(getSender());
        }
      } catch (Throwable t) {
        Error err;
        if (t instanceof Error
            && SystemFailure.isJVMFailureError(err = (Error)t)) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        exception = new ReplyException(t);
      }
      finally {
        ReplyMessage replyMsg = new ReplyMessage();
        replyMsg.setProcessorId(processorId);
        replyMsg.setRecipient(getSender());
        if(exception != null) {
          replyMsg.setException(exception);
        }
        if(log.fineEnabled()) {
          log.fine("Received " + this + ",replying with " + replyMsg);
        }
        dm.putOutgoing(replyMsg);
      }
    }

    public int getDSFID() {
      return RELEASE_CLEAR_LOCK_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
    ClassNotFoundException {
      super.fromData(in);
      regionPath = DataSerializer.readString(in);
      processorId = in.readInt();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(regionPath, out);
      out.writeInt(processorId);
    }
  }