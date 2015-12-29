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

package com.pivotal.gemfirexd.internal.engine.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.LockNotHeldException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.locks.DLockLogWriter;
import com.gemstone.gemfire.distributed.internal.locks.DLockReleaseProcessor;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdReplyMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdResponseCode;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;

/**
 * This class provides release of remote and local distributed write lock
 * requests. Lock release couterpart of {@link GfxdDRWLockRequestProcessor}.
 * Currently this is used for releasing GemFireXD write locks during DDL
 * execution by {@link GfxdDRWLockService}.
 * 
 * The distributed write lock release request of a client (
 * {@link GfxdDRWLockService#writeUnlock()}) that has previously acquired a
 * DLock and distributed write lock by invoking
 * {@link GfxdDRWLockService#writeLock()} is handled by this class. First a
 * distributed write lock release message is sent to all members using
 * {@link RWLockReleaseProcessor} to release the write lock acquired on the
 * members locally. Subsequently the DLock is released using the
 * {@link DLockReleaseProcessor}.
 * 
 * @see GfxdDRWLockRequestProcessor
 * @see GfxdDRWLockService
 * @author swale
 */
public final class GfxdDRWLockReleaseProcessor extends DLockReleaseProcessor {

  public GfxdDRWLockReleaseProcessor(final DM dm,
      final InternalDistributedMember member, final String serviceName,
      final Object objectName) {
    super(dm, member, serviceName, objectName);
  }

  /**
   * Send message to release the distributed write lock. The DLock must be
   * released separately by invoking the release method.
   */
  protected static void releaseDRWLock(final DM dm, final String serviceName,
      final Object objectName, final Object lockOwner, final LogWriterI18n log)
      throws ReplyException {
    final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
    final InternalDistributedMember myId = dm.getDistributionManagerId();
    boolean toSelf = members.remove(myId);
    GfxdDRWLockReleaseMessage.send(dm, members, toSelf, serviceName,
        objectName, lockOwner, true, log);
  }

  // -------------------------------------------------------------------------
  // RWLockReleaseProcessor
  // -------------------------------------------------------------------------
  /**
   * Implements the reply processor for distributed write lock release part of
   * Distributed ReadWriteLock release. The write lock release request is sent
   * to all members of the DS and the replies gathered and errors logged at fine
   * level.
   * 
   * @see GfxdDRWLockReleaseProcessor
   * @author swale
   */
  public static final class RWLockReleaseProcessor extends ReplyProcessor21 {

    private final Object objectName;

    private final boolean ignoreNotOwner;

    private final LogWriterI18n log;

    public RWLockReleaseProcessor(final DM dm, final Set<?> members,
        final Object object, final LogWriterI18n logger,
        final boolean ignoreError) {
      super(dm, members);
      this.log = logger;
      this.objectName = object;
      this.ignoreNotOwner = ignoreError;
    }

    static void handleNotOwner(final boolean ignoreNotOwner,
        final DistributedMember sender, final String serviceName,
        final Object objectName, final LogWriterI18n log) {
      if (!ignoreNotOwner) {
        if (log.fineEnabled()) {
          log.fine(sender + " has responded GfxdDRWLockReleaseReplyMessage."
              + "NOT_HELD for " + objectName + " in " + serviceName);
        }
      }
      else {
        if (log.warningEnabled()) {
          log.warning(LocalizedStrings.DEBUG, sender + " has responded "
              + "GfxdDRWLockReleaseReplyMessage.NOT_HELD for " + objectName
              + " in " + serviceName);
        }
      }
    }

    @Override
    public void process(final DistributionMessage msg) {
      final GfxdDRWLockReleaseReplyMessage reply =
        (GfxdDRWLockReleaseReplyMessage)msg;
      try {
        if (this.log.fineEnabled()) {
          this.log.fine("Processing: " + reply);
        }
        // acknowledged release of lock...
        if (reply.responseCode == GfxdDRWLockReleaseReplyMessage.OK) {
          if (this.log.fineEnabled()) {
            this.log.fine(reply.getSender() + " has successfully "
                + "released write lock for " + this.objectName + " in "
                + reply.serviceName);
          }
        }
        else if (reply.responseCode == GfxdDRWLockReleaseReplyMessage.NOT_HELD) {
          handleNotOwner(this.ignoreNotOwner, reply.getSender(),
              reply.serviceName, this.objectName, this.log);
        }
        else if (reply.responseCode == GfxdDRWLockReleaseReplyMessage.EXCEPTION) {
          // nothing to be done here; will be handled by super.process()
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * Control of reply processor waiting behavior in the face of exceptions.
     * 
     * @return true to stop waiting when exceptions are present
     */
    @Override
    protected boolean stopBecauseOfExceptions() {
      // for GFXD do not stop because of exceptions since we still need to
      // wait for all nodes to complete processing
      return false;
    }
  }

  // -------------------------------------------------------------------------
  // GfxdDRWLockReleaseMessage
  // -------------------------------------------------------------------------
  /**
   * Implements the distributed write lock release message for Distributed
   * ReadWriteLock release. The message is sent to all members of the DS.
   * 
   * @see GfxdDRWLockRequestProcessor
   * @author swale
   */
  public static final class GfxdDRWLockReleaseMessage extends GfxdMessage
      implements MessageWithReply {

    /** The name of the DistributedLockService */
    protected String serviceName;

    /** The object name */
    protected Object objectName;

    /**
     * The {@link GfxdDRWLockService.DistributedLockOwner} of the lock being
     * released.
     */
    protected Object lockOwner;

    /** Transient flag to indicate whether lock release was successful. */
    private transient boolean released;

    public GfxdDRWLockReleaseMessage() {
    }

    /**
     * Sends a {@link GfxdDRWLockReleaseMessage} to given members of the
     * distributed system to release a distributed write lock.
     */
    public static void send(final DM dm, final Set<DistributedMember> members,
        final boolean toSelf, final String serviceName,
        final Object objectName, final Object lockOwner,
        final boolean ignoreError, final LogWriterI18n log)
        throws ReplyException {
      final GfxdDRWLockReleaseMessage msg = new GfxdDRWLockReleaseMessage();
      final RWLockReleaseProcessor processor = new RWLockReleaseProcessor(dm,
          members, objectName, log, ignoreError);
      msg.processorId = processor.getProcessorId();
      msg.serviceName = serviceName;
      msg.objectName = objectName;
      msg.lockOwner = lockOwner;
      msg.setRecipients(members);
      final GfxdDRWLockService svc = (GfxdDRWLockService)DistributedLockService
          .getServiceNamed(serviceName);
      try {
        if (toSelf) {
          svc.getLocalLockService().writeUnlock(objectName, lockOwner);
        }
        if (members.size() > 0) {
          if (DistributionManager.VERBOSE || log.fineEnabled()) {
            log.fine(msg.toString()
                + "#send: sending writeLock release message " + "for object ["
                + objectName + "] with owner=" + lockOwner + ", to members: "
                + members + ", myId: " + dm.getDistributionManagerId());
          }
          dm.putOutgoing(msg);
          processor.waitForReplies();
        }
      } catch (ReplyException ex) {
        // first check this node going down
        Misc.checkIfCacheClosing(ex);
        // ignore if remote node is going down
        if (!GemFireXDUtils.retryToBeDone(ex)) {
          throw ex;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        // check node going down
        Misc.checkIfCacheClosing(ie);
      } catch (LockNotHeldException e) {
        RWLockReleaseProcessor.handleNotOwner(ignoreError,
            dm.getDistributionManagerId(), serviceName, objectName, log);
      }
    }

    /**
     * Processes this message - invoked on all the nodes.
     */
    @Override
    protected void processMessage(final DistributionManager dm) {
      this.released = false;
      final LogWriterI18n log = new DLockLogWriter(dm.getLoggerI18n());
      final DistributedLockService svc = DistributedLockService
          .getServiceNamed(this.serviceName);
      this.released = true;
      if (svc != null && svc instanceof GfxdDRWLockService) {
        // release the write lock
        final GfxdDRWLockService rwsvc = (GfxdDRWLockService)svc;
        if (log.fineEnabled()) {
          log.fine("GfxdDRWLockReleaseMessage#process: releasing write lock"
              + " for object [" + this.objectName + "] for owner="
              + this.lockOwner);
        }
        try {
          rwsvc.getLocalLockService().writeUnlock(this.objectName,
              this.lockOwner);
        } catch (LockNotHeldException e) {
          this.released = false;
        }
        if (log.fineEnabled()) {
          log.fine("GfxdDRWLockReleaseMessage#process: writeUnlock for "
              + "object [" + this.objectName + "] for owner=" + this.lockOwner
              + ", was " + (this.released ? "successful"
                  : ("unsuccessful with current owner "
                      + rwsvc.getWriteLockOwner(this.objectName))));
        }
      }
    }

    @Override
    protected void sendReply(ReplyException ex, DistributionManager dm) {
      final GfxdDRWLockReleaseReplyMessage response =
        new GfxdDRWLockReleaseReplyMessage();
      response.setProcessorId(getProcessorId());
      response.setRecipient(getSender());
      response.serviceName = this.serviceName;
      if (ex != null) {
        response.setException(new ReplyException(ex));
        final LogWriterI18n log = new DLockLogWriter(dm.getLoggerI18n());
        if (log.fineEnabled()) {
          log.fine("While processing <" + this
              + ">, got exception, returning to sender", response
              .getException());
        }
      }
      else if (this.released) {
        response.responseCode = GfxdDRWLockReleaseReplyMessage.OK;
      }
      else {
        response.responseCode = GfxdDRWLockReleaseReplyMessage.NOT_HELD;
      }
      dm.putOutgoing(response);
    }

    @Override
    protected boolean waitForNodeInitialization() {
      return false;
    }

    @Override
    public byte getGfxdID() {
      return GfxdSerializable.DRWLOCK_RELEASE_MESSAGE;
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.serviceName, out);
      DataSerializer.writeObject(this.objectName, out);
      DataSerializer.writeObject(this.lockOwner, out);
    }

    @Override
    public void fromData(DataInput in)
        throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.serviceName = DataSerializer.readString(in);
      this.objectName = DataSerializer.readObject(in);
      this.lockOwner = DataSerializer.readObject(in);
    }

    @Override
    protected void appendFields(final StringBuilder sb) {
      super.appendFields(sb);
      sb.append("; serviceName=").append(this.serviceName);
      sb.append("; name=").append(this.objectName);
      sb.append("; owner=").append(this.lockOwner);
      sb.append("; sender=").append(getSender());
    }
  } // GfxdDRWLockReleaseMessage

  // -------------------------------------------------------------------------
  // GfxdDRWLockReleaseReplyMessage
  // -------------------------------------------------------------------------
  /**
   * This is a response to an {@link GfxdDRWLockReleaseMessage}. A response
   * communicates one of three things:
   * 
   * 1. OK - the lock was successfully released
   * 
   * 2. NOT_HELD - no lock is currently held for the object
   * 
   * 3. EXCEPTION - some exception during message processing
   * 
   * @see GfxdDRWLockReleaseMessage
   * @author swale
   */
  public static final class GfxdDRWLockReleaseReplyMessage extends
      GfxdReplyMessage {

    static final int EXCEPTION = 0;

    static final int OK = 1;

    static final int NOT_HELD = 2;

    /** Name of service to release the lock in; for toString only */
    protected String serviceName;

    /** OK, EXCEPTION or NOT_HELD for the service */
    protected int responseCode = EXCEPTION; // default

    @Override
    public byte getGfxdID() {
      return GfxdSerializable.DRWLOCK_RELEASE_REPLY_MESSAGE;
    }

    @Override
    public GfxdResponseCode getResponseCode() {
      return null;
    }

    @Override
    public void setException(ReplyException ex) {
      super.setException(ex);
      this.responseCode = EXCEPTION;
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.serviceName, out);
      out.writeInt(this.responseCode);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
            ClassNotFoundException {
      super.fromData(in);
      this.serviceName = DataSerializer.readString(in);
      this.responseCode = in.readInt();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();

      sb.append("{GfxdDRWLockReleaseReplyMessage id=" + this.processorId);
      sb.append(" serviceName=" + this.serviceName);
      sb.append(" responseCode=");
      switch (this.responseCode) {
        case EXCEPTION:
          sb.append("EXCEPTION");
          break;
        case OK:
          sb.append("OK");
          break;
        case NOT_HELD:
          sb.append("NOT_HELD");
          break;
        default:
          sb.append(String.valueOf(this.responseCode));
          break;
      }
      sb.append(" sender=" + getSender());
      sb.append(" processorId=" + this.processorId);
      sb.append("}");
      return sb.toString();
    }
  } // GfxdDRWLockReleaseReplyMessage
}
