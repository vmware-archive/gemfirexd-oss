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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gemfire.cache.LockTimeoutException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gnu.trove.THashMap;

/**
 * Message sent to remote nodes to perform the first phase commit of an active
 * transaction. Currently this is only invoked for REPEATABLE_READ, but in
 * future may also be used for persistent transactions.
 * This is also done for cases when the region has queue attached to it.
 * 
 * This can possibly throw exceptions like {@link ConflictException}, or
 * {@link LockTimeoutException} that will cause the transaction to fail.
 * 
 * This message can be either piggy-backed with {@link TXBatchMessage}, or sent
 * by itself. In the former case even the replies will be piggy-backed with that
 * of {@link TXBatchMessage}.
 * 
 * @author swale
 * @since 7.0
 */
public final class TXRemoteCommitPhase1Message extends TXMessage {

  private transient CommitPhase1Response processor;

  private boolean doPreCommit;

  private static final short DO_PRE_COMMIT = UNRESERVED_FLAGS_START;

  /** for deserialization */
  public TXRemoteCommitPhase1Message() {
  }

  private TXRemoteCommitPhase1Message(final TXStateInterface tx,
      final boolean doPreCommit, final CommitPhase1Response processor) {
    super(tx, processor);
    this.doPreCommit = doPreCommit;
    this.processor = processor;
  }

  @Override
  public CommitPhase1Response getReplyProcessor() {
    return this.processor;
  }

  /**
   * Creates a new commit phase1 message with a reply processor (
   * {@link #getReplyProcessor()}. Note that the set of recipients is set on the
   * reply processor but not on the message itself since caller may batch it up
   * with other messages. Also the reply processor itself is not registered and
   * caller may need to do that explicitly if the message was not piggy-backed
   * with a {@link TXBatchMessage}.
   */
  public static TXRemoteCommitPhase1Message create(
      final InternalDistributedSystem system, final DM dm,
      final TXStateProxy tx, boolean doPreCommit, final Set<?> recipients) {
    final CommitPhase1Response response = new CommitPhase1Response(system, dm,
        recipients);
    final TXRemoteCommitPhase1Message msg = new TXRemoteCommitPhase1Message(tx,
        doPreCommit, response);
    return msg;
  }

  @Override
  protected boolean operateOnTX(final TXStateProxy txProxy,
      DistributionManager dm) {

    if (txProxy != null) {
      if (this.doPreCommit) {
        txProxy.lock.lock();
        try {
          txProxy.preCommit();
        } finally {
          txProxy.lock.unlock();
        }
      } else {
        final TXState localState;
        if ((localState = txProxy.getLocalTXState()) != null) {
          localState.commitPhase1();
        }
      }
    }
    return true;
  }

  @Override
  protected void sendReply(InternalDistributedMember recipient,
      int processorId, DistributionManager dm, ReplyException rex,
      TXStateProxy proxy) {
    // send a reply regardless of processorId since this may be piggy-backed
    // to a TXBatchMessage
    CommitPhase1ReplyMessage.sendResponse(recipient, processorId, rex,
        getReplySender(dm), null /* don't update regions */,
        proxy != null ? proxy.eventsToBePublished : null);
  }

  /**
   * @see TransactionMessage#useTransactionProxy()
   */
  @Override
  public boolean useTransactionProxy() {
    // if proxy is present then by default the commit/rollback should act on it
    return true;
  }

  @Override
  protected final boolean isTXEnd() {
    return true;
  }

  @Override
  protected short computeCompressedShort(short flags) {
    if (this.doPreCommit) {
      flags |= DO_PRE_COMMIT;
    }
    return flags;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.doPreCommit = (flags & DO_PRE_COMMIT) != 0;
  }

  /**
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return TX_REMOTE_COMMIT_PHASE1_MESSAGE;
  }

  /**
   * @see AbstractOperationMessage#appendFields(StringBuilder)
   */
  @Override
  protected void appendFields(StringBuilder sb) {
    sb.append("; doPreCommit=").append(this.doPreCommit);
  }

  public final static class CommitPhase1Response extends ReplyProcessor21 {

    private final Map<InternalDistributedMember,
        Map<String, TObjectLongHashMapDSFID>> eventsToBeDispatched;
    
    public CommitPhase1Response(final InternalDistributedSystem system,
        final DM dm, final Collection<?> initMembers) {
      super(dm, system, initMembers, null, false);
      this.eventsToBeDispatched = new ConcurrentHashMap<InternalDistributedMember,
          Map<String, TObjectLongHashMapDSFID>>(16, 0.75f, 2);
    }

    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof CommitPhase1ReplyMessage) {
        CommitPhase1ReplyMessage reply = (CommitPhase1ReplyMessage)msg;
        if (reply.getEventsToBePublished() != null) {
          this.eventsToBeDispatched.put(reply.getSender(),
              reply.getEventsToBePublished());
          if (getDistributionManager().getLoggerI18n().fineEnabled()) {
            getDistributionManager().getLoggerI18n().fine(
                "CommitPhase1Response got following events to be Dispatched "
                    + reply.getEventsToBePublished() + " from "
                    + reply.getSender());
          }
        }

        if (getDistributionManager().getLoggerI18n().fineEnabled()) {
          getDistributionManager().getLoggerI18n().fine(
              "CommitPhase1Response got following events to be Dispatched "
                  + reply.getEventsToBePublished() + " from "
                  + reply.getSender());
        }
      }
      super.process(msg);
    }

    @Override
    public final boolean waitForProcessedReplies() throws InterruptedException {
      try {
        waitForReplies(0L);
      } catch (ReplyException re) {
        final Throwable cause = re.getCause();
        re.fixUpRemoteEx(cause);
        if (cause instanceof TransactionException) {
          throw (TransactionException)cause;
        } else {
          throw re;
        }
      } finally {
        getDistributionManager().getStats().incCommitPhase1Waits();
      }
      return true;
    }

    @Override
    protected final boolean stopBecauseOfExceptions() {
      // never stop because of exceptions
      return false;
    }
    
    public Map<InternalDistributedMember,
        Map<String, TObjectLongHashMapDSFID>> getEventsToBeDispatched() {
      return eventsToBeDispatched.isEmpty() ? null : eventsToBeDispatched;
    }
  }

  public final static class CommitPhase1ReplyMessage extends ReplyMessage {

    private THashMap/*<String, TObjectLongHashMap>*/ eventsToBePublished;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public CommitPhase1ReplyMessage() {
    }

    public CommitPhase1ReplyMessage(DataInput in) throws IOException,
        ClassNotFoundException {
      fromData(in);
    }

    private CommitPhase1ReplyMessage(int processorId, THashMap events,
        ReplyException ex) {
      super();
      super.setException(ex);
      setProcessorId(processorId);
      this.eventsToBePublished = (THashMap)events;
    }

    public static void sendResponse(InternalDistributedMember recipient,
        int processorId, ReplyException rex, ReplySender dm,
        AbstractOperationMessage sourceMessage, THashMap events) {
      Assert.assertTrue(recipient != null,
          "CommitPhase1ReplyMessage NULL reply message");
      CommitPhase1ReplyMessage m = new CommitPhase1ReplyMessage(processorId,
          events, rex);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (DistributionManager.VERBOSE) {
        LogWriterI18n l = dm.getLoggerI18n();
        l.fine("CommitPhase1ReplyMessage process invoking reply processor "
            + "with processorId: " + this.processorId);
      }

      if (processor == null) {
        if (DistributionManager.VERBOSE) {
          LogWriterI18n l = dm.getLoggerI18n();
          l.fine("CommitPhase1ReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (DistributionManager.VERBOSE) {
        LogWriterI18n logger = dm.getLoggerI18n();
        logger.info(LocalizedStrings.DEBUG, processor + " processed " + this);
      }
      dm.getStats().incReplyMessageTime(
          DistributionStats.getStatTime() - startTime);
    }

    @SuppressWarnings("unchecked")
    public Map<String, TObjectLongHashMapDSFID> getEventsToBePublished() {
      return eventsToBePublished;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(eventsToBePublished != null);
      if (eventsToBePublished != null) {
        DataSerializer.writeTHashMap(eventsToBePublished, out);
      }
    }

    @Override
    public int getDSFID() {
      return COMMIT_PHASE1_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      boolean hasToBePublishedEvent = in.readBoolean();
      if (hasToBePublishedEvent) {
        eventsToBePublished = DataSerializer.readTHashMap(in);
      }
    }

    @Override
    public String toString() {
      return new StringBuilder().append("CommitPhase1ReplyMessage ")
          .append("processorid=").append(this.processorId)
          .append("ToBePublishedEvents=").append(this.eventsToBePublished)
          .toString();
    }
  }
}
