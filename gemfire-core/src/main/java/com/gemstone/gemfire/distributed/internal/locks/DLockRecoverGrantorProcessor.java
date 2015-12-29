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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import java.io.*;
import java.util.*;

/**
 * Handles messaging to all members of a lock service for the purposes of
 * recoverying from the loss of the lock grantor. The 
 * <code>DLockRecoverGrantorMessage</code> is sent out by the new lock grantor
 * and all members reply with details on held and pending locks.
 *
 * @author Kirk Lund
 */
public class DLockRecoverGrantorProcessor extends ReplyProcessor21 {

  protected final static DefaultMessageProcessor nullServiceProcessor = 
      new DefaultMessageProcessor(); 
  
  private final DM dm;
  
  private final DLockGrantor newGrantor;
  
  // ------------------------------------------------------------------------- 
  //   Static operations for recovering from loss of the lock grantor
  // -------------------------------------------------------------------------  
  
  /**
   * Sends DLockRecoverGrantorMessage to all participants of the DLockService
   * in order to recover from the loss of the previous lock grantor.  Each
   * member will reply with details on currently held locks, pending locks,
   * and known stuck locks.
   * <p>
   * This method should block until transfer of lock grantor has completed.
   */
  static boolean recoverLockGrantor(Set members,
                                    DLockService service,
                                    DLockGrantor newGrantor,
                                    DM dm,
                                    InternalDistributedMember elder) {
    // proc will wait for replies from everyone including THIS member...
    DLockRecoverGrantorProcessor processor = 
        new DLockRecoverGrantorProcessor(dm, members, newGrantor);

    DLockRecoverGrantorMessage msg = new DLockRecoverGrantorMessage();
    msg.serviceName = service.getName();
    msg.processorId = processor.getProcessorId();
    msg.grantorVersion = newGrantor.getVersionId();
    msg.grantorSerialNumber = service.getSerialNumber();
    msg.elder = elder;
    
    // send msg to all members EXCEPT this member...
    Set recipients = new HashSet(members);
    recipients.remove(dm.getId());
    if (!recipients.isEmpty()) {
      msg.setRecipients(recipients);
      /*if (DLockLogWriter.fineEnabled(dm)) {
        DLockLogWriter.fine(dm, "Sending DLockRecoverGrantorMessage: " + msg);
      }*/
      dm.putOutgoing(msg);
    }

    // process msg and reply from this VM...
    if (msg.getSender() == null) msg.setSender(dm.getId());
    msg.processMessage(dm);
        
    // keep waiting even if interrupted
    try { 
      processor.waitForRepliesUninterruptibly();
    }
    catch (ReplyException e) {
      e.handleAsUnexpected();
    }
    if (processor.error) {
      return false;
    }
    
    //return newGrantor.makeReady(false);
    return true;
  }
  
  // ------------------------------------------------------------------------- 
  //   Constructors
  // -------------------------------------------------------------------------  
  
  /** Creates a new instance of DLockRecoverGrantorProcessor */
  private DLockRecoverGrantorProcessor(DM dm, 
                                       Set members, 
                                       DLockGrantor newGrantor) {
    super(dm, members);
    this.dm = dm;
    this.newGrantor = newGrantor;
  }
  
  // ------------------------------------------------------------------------- 
  //   Instance methods
  // -------------------------------------------------------------------------
  
  private volatile boolean error = false;
  
  @Override
  protected boolean canStopWaiting() {
    return this.error;
  }
  
  @Override
  public void process(DistributionMessage msg) {
    try {
      Assert.assertTrue(msg instanceof DLockRecoverGrantorReplyMessage, 
          "DLockRecoverGrantorProcessor is unable to process message of type " +
          msg.getClass());

      DLockRecoverGrantorReplyMessage reply = (DLockRecoverGrantorReplyMessage)msg;
      // build grantTokens from each reply...
      switch (reply.replyCode) {
        case DLockRecoverGrantorReplyMessage.GRANTOR_DISPUTE:
          if (DLockLogWriter.fineEnabled(this.dm)) {
            DLockLogWriter.fine(this.dm, 
                "Failed DLockRecoverGrantorReplyMessage: {" + reply + "}");
          }
          this.error = true;
          break;
        case DLockRecoverGrantorReplyMessage.OK:  
          // collect results...
          if (DLockLogWriter.fineEnabled(this.dm)) {
            DLockLogWriter.fine(this.dm, 
                "Processing DLockRecoverGrantorReplyMessage: {" + reply + "}");
          }
          
          Set lockSet = new HashSet();
          DLockRemoteToken[] heldLocks = reply.heldLocks;
          if (heldLocks.length > 0) {
            for (int i = 0; i < heldLocks.length; i++) {
              lockSet.add(heldLocks[i]);
            }
            try {
              this.newGrantor.initializeHeldLocks(msg.getSender(), lockSet);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              dm.getCancelCriterion().checkCancelInProgress(e);
            }
          }
          break;
        default:
          throw new IllegalStateException("Invalid reply.replyCode " + reply.replyCode);
      }
      // maybe build up another reply to indicate lock recovery status?
    }
    catch (IllegalStateException e) {
      if (DLockLogWriter.fineEnabled(this.dm)) {
        DLockLogWriter.fine(this.dm,
            "Processing of DLockRecoverGrantorReplyMessage " + 
            msg + " resulted in " + e.getMessage(), e);
      }
    }
    finally {
      super.process(msg);
    }
  }
  
  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }
  
  // ------------------------------------------------------------------------- 
  //   DLockRecoverGrantorMessage
  // -------------------------------------------------------------------------  
  public static final class DLockRecoverGrantorMessage
  extends PooledDistributionMessage 
  implements MessageWithReply {

    /** The name of the DistributedLockService */
    protected String serviceName;
    
    /** The reply processor to route replies to */
    protected int processorId;

    /** The version, from the elder, of the grantor doing the recovery. */
    protected long grantorVersion;
    
    /** The DLS serial number of the grantor doing the recovery. */
    protected int grantorSerialNumber;

    /** The elder that made this member the grantor */
    protected InternalDistributedMember elder;

    public String getServiceName() {
      return this.serviceName;
    }
    public void setServiceName(final String serviceName) {
      this.serviceName = serviceName;
    }
  
    @Override
    public int getProcessorId() {
      return this.processorId;
    }
    public void setProcessorId(final int processorId) {
      this.processorId = processorId;
    }

    public long getGrantorVersion() {
      return this.grantorVersion;
    }
    
    public int getGrantorSerialNumber() {
      return this.grantorSerialNumber;
    }
    
    public InternalDistributedMember getElder() {
      return this.elder;
    }
    
    @Override
    protected void process(DistributionManager dm) {
      processMessage(dm);
    }
    
    protected void processMessage(DM dm) {
      MessageProcessor processor = nullServiceProcessor;
      
      DLockService svc = 
          DLockService.getInternalServiceNamed(this.serviceName);
      if (svc != null) {
        if (svc.getDLockRecoverGrantorMessageProcessor() == null) {
          svc.setDLockRecoverGrantorMessageProcessor(new DefaultMessageProcessor());
        }
        processor = svc.getDLockRecoverGrantorMessageProcessor();
      }
      
      processor.process(dm, this);
    }
    
    public int getDSFID() {
      return DLOCK_RECOVER_GRANTOR_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.serviceName = DataSerializer.readString(in);
      this.processorId = in.readInt();
      this.grantorSerialNumber = in.readInt();
      this.grantorVersion = in.readLong();
      this.elder = (InternalDistributedMember)DataSerializer.readObject(in);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.serviceName, out);
      out.writeInt(this.processorId);
      out.writeInt(this.grantorSerialNumber);
      out.writeLong(this.grantorVersion);
      DataSerializer.writeObject(this.elder, out);
    }
    
    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("DLockRecoverGrantorMessage (service='");
      buff.append(this.serviceName);
      buff.append("'; processorId=");
      buff.append(this.processorId);
      buff.append("'; grantorVersion=");
      buff.append(this.grantorVersion);
      buff.append("'; grantorSerialNumber=");
      buff.append(this.grantorSerialNumber);
      buff.append("'; elder=");
      buff.append(this.elder);
      buff.append(")");
      return buff.toString();
    }
   }
  
  // ------------------------------------------------------------------------- 
  //   DLockRecoverGrantorReplyMessage
  // -------------------------------------------------------------------------  
  public static final class DLockRecoverGrantorReplyMessage 
  extends ReplyMessage {
    
    public static final int OK = 0;
    public static final int GRANTOR_DISPUTE = 1;
    
    protected int replyCode;
    
    /**
     * Locks that are currently held...
     * Serializable owner, Object name, long leaseTime
     */
    protected DLockRemoteToken[] heldLocks;

//    /** Identifies who the responder believes the lock grantor currently is */
//    private InternalDistributedMember grantor;
    
    public int getReplyCode() {
      return this.replyCode;
    }
    public void setReplyCode(final int replyCode) {
      this.replyCode = replyCode;
    }
    
    public DLockRemoteToken[] getHeldLocks() {
      return this.heldLocks;
    }
    public void setHeldLocks(final DLockRemoteToken[] heldLocks) {
      this.heldLocks = heldLocks;
    }
    
    @Override
    public int getDSFID() {
      return DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.replyCode = in.readByte();
      this.heldLocks = (DLockRemoteToken[]) DataSerializer.readObjectArray(in);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeByte(this.replyCode);
      DataSerializer.writeObjectArray(this.heldLocks, out);
    }

    @Override
    public String toString() {
      String response = null;
      switch (this.replyCode) {
        case OK:                response = "OK"; break;
        case GRANTOR_DISPUTE:   response = "GRANTOR_DISPUTE"; break;
        default: response = String.valueOf(this.replyCode); break;
      }
      return "DLockRecoverGrantorReplyMessage (processorId=" + processorId +
             "; replyCode=" + this.replyCode + "=" + response + 
             "; heldLocks=" + Arrays.asList(this.heldLocks) +
             "; sender=" + getSender() + 
             ")";
    }
  }
  
  public static interface MessageProcessor {
    public void process(DM dm, DLockRecoverGrantorMessage msg);
  }
  
  static class DefaultMessageProcessor implements MessageProcessor {
    public void process(DM dm, DLockRecoverGrantorMessage msg) {
      LogWriterI18n log = dm.getLoggerI18n();
      ReplyException replyException = null;
      int replyCode = DLockRecoverGrantorReplyMessage.OK;
      DLockRemoteToken[] heldLocks = new DLockRemoteToken[0];
      
      //DLockLogWriter.fine(dm, "[DLockRecoverGrantorMessage.process]");
      try {
        // get the service from the name
        DLockService svc = 
            DLockService.getInternalServiceNamed(msg.getServiceName());
        
        if (svc != null) {
          replyCode = DLockRecoverGrantorReplyMessage.OK;
          
          LockGrantorId lockGrantorId = new LockGrantorId(
              dm, msg.getSender(), msg.getGrantorVersion(), msg.getGrantorSerialNumber());
          
          Set heldLockSet = svc.getLockTokensForRecovery(lockGrantorId);
          
          if (heldLockSet == null) {
            replyCode = DLockRecoverGrantorReplyMessage.GRANTOR_DISPUTE;
          }
          else {
            heldLocks = (DLockRemoteToken[]) 
                heldLockSet.toArray(new DLockRemoteToken[heldLockSet.size()]);
          }
        }
      }
      catch (RuntimeException e) {
        log.warning(LocalizedStrings.DLOCKRECOVERGRANTORPROCESSOR_DLOCKRECOVERGRANTORMESSAGE_PROCESS_THROWABLE, e);
        replyException = new ReplyException(e);
      }
//      catch (Throwable t) {
//        Error err;
//        if (t instanceof Error && SystemFailure.isJVMFailureError(
//            err = (Error)t)) {
//          SystemFailure.initiateFailure(err);
//          // If this ever returns, rethrow the error. We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        // Whenever you catch Error or Throwable, you must also
//        // check for fatal JVM error (see above). However, there is
//        // _still_ a possibility that you are dealing with a cascading
//        // error condition, so you also need to check to see if the JVM
//        // is still usable:
//        SystemFailure.checkFailure();
//        log.warning(LocalizedStrings.DLockRecoverGrantorProcessor_DLOCKRECOVERGRANTORMESSAGEPROCESS_THROWABLE, t);
//        replyException = new ReplyException(t);
//      }
      finally {
        DLockRecoverGrantorReplyMessage replyMsg = 
            new DLockRecoverGrantorReplyMessage();
        replyMsg.replyCode = replyCode;
        replyMsg.heldLocks = heldLocks;
        replyMsg.setProcessorId(msg.getProcessorId());
        replyMsg.setRecipient(msg.getSender());
        replyMsg.setException(replyException);
        if (msg.getSender().equals(dm.getId())) {
          // process in-line in this VM
          DLockLogWriter.fine(dm, "[DLockRecoverGrantorMessage.process] locally process reply");
          replyMsg.setSender(dm.getId());
          replyMsg.dmProcess(dm);
        }
        else {
          DLockLogWriter.fine(dm, "[DLockRecoverGrantorMessage.process] send reply");
          dm.putOutgoing(replyMsg);
        }
      }
    }
  }
}

