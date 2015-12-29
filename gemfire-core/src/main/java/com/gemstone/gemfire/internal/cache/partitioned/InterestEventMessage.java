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
import com.gemstone.gemfire.cache.InterestRegistrationEvent;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * This message is used as the notification that a client interest registration or
 * unregistration event occurred.
 *
 * @author Barry Oglesby
 * @since 5.8BetaSUISSE
 */
public class InterestEventMessage extends PartitionMessage {
  /** The <code>InterestRegistrationEvent</code>  */
  private InterestRegistrationEvent event;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public InterestEventMessage() {
  }

  private InterestEventMessage(Set recipients, int regionId, int processorId,
      final InterestRegistrationEvent event, ReplyProcessor21 processor) {
    super(recipients, regionId, processor, null);
    this.event = event;
  }

  @Override
  final public int getMessageProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }

  @Override  
  protected final boolean operateOnPartitionedRegion(
      final DistributionManager dm, PartitionedRegion r, long startTime)
      throws ForceReattemptException {
    final LogWriterI18n l = r.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("InterestEventMessage operateOnPartitionedRegion: "
          + r.getFullPath());
    }

    PartitionedRegionDataStore ds = r.getDataStore();

    if (ds != null) {
      try {
        ds.handleInterestEvent(this.event);
        r.getPrStats().endPartitionMessagesProcessing(startTime);
        InterestEventReplyMessage.send(getSender(), getProcessorId(), dm);
      }
      catch (Exception e) {
        sendReply(
            getSender(),
            getProcessorId(),
            dm,
            new ReplyException(new ForceReattemptException(
                "Caught exception during interest registration processing:", e)),
            r, startTime);
        return false;
      }
    }
    else {
      throw new InternalError(
          "InterestEvent message was sent to a member with no storage.");
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; event=").append(this.event);
  }

  @Override  
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.event = (InterestRegistrationEvent)DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.event, out);
  }

  /**
   * Sends an InterestEventMessage message
   *
   * @param recipients
   *          the Set of members that the get message is being sent to
   * @param region
   *          the PartitionedRegion for which interest event was received
   * @param event
   *          the InterestRegistrationEvent to send
   * @return the InterestEventResponse
   * @throws ForceReattemptException
   *           if the peer is no longer available
   */
  public static InterestEventResponse send(Set recipients,
      PartitionedRegion region, final InterestRegistrationEvent event)
      throws ForceReattemptException {
    InterestEventResponse response = new InterestEventResponse(region
        .getSystem(), recipients, null);
    InterestEventMessage m = new InterestEventMessage(recipients, region
        .getPRId(), response.getProcessorId(), event, response);

    Set failures = region.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + m + "> to "
          + failures);
    }
    return response;
  }

  /**
   * This message is used for the reply to a {@link InterestEventMessage}.
   *
   * @author Barry Oglesby
   * @since 5.8BetaSUISSE
   */
  public static class InterestEventReplyMessage extends
      HighPriorityDistributionMessage {
    /** The shared obj id of the ReplyProcessor */
    private int processorId;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public InterestEventReplyMessage() {
    }

    private InterestEventReplyMessage(int processorId) {
      this.processorId = processorId;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
        int processorId, DM dm) throws ForceReattemptException {
      InterestEventReplyMessage m = new InterestEventReplyMessage(processorId);
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
    protected void process(final DistributionManager dm) {
      final long startTime = getTimestamp();
      LogWriterI18n l = dm.getLoggerI18n();
      if (DistributionManager.VERBOSE) {
        l
            .fine("InterestEventReplyMessage process invoking reply processor with processorId:"
                + this.processorId);
      }

      try {
        ReplyProcessor21 processor = ReplyProcessor21
            .getProcessor(this.processorId);

        if (processor == null) {
          if (DistributionManager.VERBOSE) {
            l.fine("InterestEventReplyMessage processor not found");
          }
          return;
        }
        processor.process(this);

        if (DistributionManager.VERBOSE) {
          LogWriterI18n logger = dm.getLoggerI18n();
          logger.info(LocalizedStrings.ONE_ARG, processor + " processed " + this);
        }
      }
      finally {
        dm.getStats().incReplyMessageTime(
            DistributionStats.getStatTime() - startTime);
      }
    }

    @Override  
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(processorId);
    }

    @Override  
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
    }

    @Override  
    public String toString() {
      StringBuffer sb = new StringBuffer().append("InterestEventReplyMessage ")
          .append("processorid=").append(this.processorId).append(
              " reply to sender ").append(this.getSender());
      return sb.toString();
    }

    public int getDSFID() {
      return INTEREST_EVENT_REPLY_MESSAGE;
    }
  }

  /**
   * A processor to capture the value returned by {@link
   * com.gemstone.gemfire.internal.cache.partitioned.InterestEventMessage.InterestEventReplyMessage}
   *
   * @author Barry Oglesby
   * @since 5.1
   */
  public static class InterestEventResponse extends PartitionResponse {

    public InterestEventResponse(InternalDistributedSystem ds, Set recipients,
        final TXStateInterface tx) {
      super(ds, recipients, tx);
    }

    /**
     * @throws ForceReattemptException if the peer is no longer available
     */
    public void waitForResponse() throws ForceReattemptException {
      try {
        waitForCacheException();
      }
      catch (ForceReattemptException e) {
        final String msg = "InterestEventResponse got ForceReattemptException; rethrowing";
        getDistributionManager().getLoggerI18n().fine(msg, e);
        throw e;
      }
      catch (CacheException e) {
        final String msg = "InterestEventResponse got remote CacheException, throwing ForceReattemptException";
        getDistributionManager().getLoggerI18n().fine(msg, e);
        throw new ForceReattemptException(msg, e);
      }
    }
  }

  public int getDSFID() {
    return INTEREST_EVENT_MESSAGE;
  }
}
