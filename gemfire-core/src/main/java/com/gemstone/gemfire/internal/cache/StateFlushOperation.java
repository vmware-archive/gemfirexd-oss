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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This operation ensures that a particular member has seen all state
 * changes for a Region prior to a point in time.  Currently this is
 * fixed at the time the member using this operation exchanged profiles
 * with other users of the Region, and is useful only for ensuring
 * consistency for InitialImageOperation.
 * 
 * StateFlushOperation works with distribution advisors and with the
 * membership manager to flush cache operations from threads to communications
 * channels and then from the communications channels to the cache of the
 * member selected to be an initial image provider.
 * 
 * To make an operation subject to StateFlushOperation you must encapsulate
 * the message part of the operation (prior to asking for distribution advice)
 * in a try/finally block.  The try/finally block must work with the
 * distribution manager like this:
 * 
 * <pre>
 * try {
 *   long version = advisor.startOperation();
 *   ... get advice and write the message (dm.putOutgoing())
 *   advisor.endOperation(version);
 *   version = -1;
 *   ... wait for replies, etc.
 * } finally {
 *   if (version >= 0) {
 *     advisor.endOperation(version);
 *   }
 * }
 * </pre> 
 * 
 * On the receiving side the messaging system will look at the result of
 * invoking containsCacheContentChange() on the message.  If the message
 * does not return true from this message then state-flush will not wait
 * for it to be applied to the cache before GII starts.
 * 
 * <pre>
 * \@Override
 * public boolean containsCacheContentChange() {
 *   return true;
 * }
 * </pre>
 * 
 * The messaging infrastructure will handle the rest for you.  For examples
 * look at the uses of startOperation() and endOperation().  There are some
 * complex examples in transaction processing and a more straightforward
 * example in DistributedCacheOperation.
 * 
 * @author Bruce Schuchardt
 * @since 5.0.1
 */
public class StateFlushOperation  {

  public final static boolean DEBUG = Boolean.getBoolean("StateFlushOperation.DEBUG");

  private DistributedRegion region;
  
  private final DM dm;
  
  
  /** flush current ops to the given members for the given region */
  public static void flushTo(Set<InternalDistributedMember> targets, DistributedRegion region) {
    DM dm = region.getDistributionManager();
    DistributedRegion r = region;
    boolean initialized = r.isInitialized();
    if (initialized) {
      r.getDistributionAdvisor().forceNewMembershipVersion(); //force a new "view" so we can track current ops
      try {
        r.getDistributionAdvisor().waitForCurrentOperations(dm.getLoggerI18n());
      } catch (RegionDestroyedException e) {
        return;
      }
    }
    // send all state-flush messages and then wait for replies
    Set<ReplyProcessor21> processors = new HashSet<ReplyProcessor21>();
    for (InternalDistributedMember target: targets) {
      StateStabilizationMessage gr = new StateStabilizationMessage();
      gr.isSingleFlushTo = true; // new for flushTo operation
      gr.requestingMember = dm.getDistributionManagerId();
      gr.setRecipient(target);
      ReplyProcessor21 processor = new ReplyProcessor21(dm, target);
      gr.processorId = processor.getProcessorId();
      gr.channelState = dm.getMembershipManager().getChannelStates(target, false);
      if (StateFlushOperation.DEBUG && ((gr.channelState != null) && (gr.channelState.size() > 0)) ) {
        dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_CHANNEL_STATES_0, gr.channelStateDescription(gr.channelState));
      }
      if (StateFlushOperation.DEBUG) {
        dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_SENDING__0, gr);
      }
      dm.putOutgoing(gr);
      processors.add(processor);
    }
    for (ReplyProcessor21 processor: processors) {
      try {
        processor.waitForReplies();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Constructor for StateFlushOperation
   * @param r
   *    The region whose state is to be flushed
   */
  public StateFlushOperation(DistributedRegion r) {
    this.region = r;
    this.dm = r.getDistributionManager();
  }
  
  /**
   * Constructor for StateFlushOperation for flushing all regions
   * @param dm the distribution manager to use in distributing the operation
   */
  public StateFlushOperation(DM dm) {
    this.dm = dm;
  }
  
  
 /**
  * flush state to the given target
  * @param recipients
  *    The members who may be making state changes to the region.  This is
  *    typically taken from a CacheDistributionAdvisor membership set
 * @param target
  *    The member who should have all state flushed to it
 * @param processorType
  *    The execution processor type for the marker message that is sent to
  *    all members using the given region
 * @param flushNewOps
 *      normally only ops that were started before region profile exchange
 *      are flushed.  Setting this to true causes the flush to wait for
 *      any started after the profile exchange as well.
  * @throws InterruptedException
  *     If the operation is interrupted, usually for shutdown, an
  *     InterruptedException will be thrown
  * @return
  *    true if the state was flushed, false if not
  */
  public boolean flush(
      Set recipients, 
      DistributedMember target, 
      int processorType, boolean flushNewOps)
  throws InterruptedException  {
    
    Set recips = recipients;  // do not use recipients parameter past this point
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    
    LogWriterI18n logger = this.dm.getLoggerI18n();
    InternalDistributedMember myId = this.dm.getDistributionManagerId();
    
    if (!recips.contains(target) && !myId.equals(target)) {
      recips = new HashSet(recipients);
      recips.add(target);
    }
    // partial fix for bug 38773 - ensures that this cache will get both
    // a cache op and an adjunct message when creating a bucket region
//    if (recips.size() < 2 && !myId.equals(target)) {
//      if(logger.fineEnabled()) {
//        logger.fine("Not flushing state, there is only 1 recipient");
//      }
//      return true; // no state to flush to a single holder of the region
//    }
    StateMarkerMessage smm = new StateMarkerMessage();
    smm.relayRecipient = target;
    smm.processorType = processorType;
    smm.flushNewOps = flushNewOps;
    if (region == null) {
      smm.allRegions = true;
    }
    else {
      smm.regionPath = region.getFullPath();
    }
    smm.setRecipients(recips);

    StateFlushReplyProcessor gfprocessor =
      new StateFlushReplyProcessor(dm, recips, target);
    smm.processorId = gfprocessor.getProcessorId();
    if (region != null &&
        region.isUsedForPartitionedRegionBucket() &&
        region.getDistributionConfig().getAckSevereAlertThreshold() > 0) {
      smm.severeAlertEnabled = true;
      gfprocessor.enableSevereAlertProcessing();
    }
    if (StateFlushOperation.DEBUG) {
      logger.info(
          LocalizedStrings.ONE_ARG, "Sending " + smm + " with processor " + gfprocessor);
    }
    Set failures = this.dm.putOutgoing(smm);
    if (failures != null) {
      if (failures.contains(target)) {
        if (StateFlushOperation.DEBUG) {
          logger.info(
              LocalizedStrings.ONE_ARG,
              "failed to send StateMarkerMessage to target "
              + target + "; returning from flush without waiting for replies");
        }
        return false;
      }
      gfprocessor.messageNotSentTo(failures);
    }

    try {
//      try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } // DEBUGGING - stall before getting membership to increase odds that target has left
      gfprocessor.waitForReplies();
      if (StateFlushOperation.DEBUG) {
        logger.info(
            LocalizedStrings.ONE_ARG, "Finished processing " + smm);
      }
    }
    catch (ReplyException re) {
      logger.warning(LocalizedStrings.StateFlushOperation_STATE_FLUSH_TERMINATED_WITH_EXCEPTION, re);
      return false;
    }
    return true;
  }

  /**
   * This message is sent, e.g., before requesting an initial image from a single provider.
   * It is sent to all members holding the region, and has the effect of causing
   * those members to send a serial distribution message (a StateStabilizationMessage)
   * to the image provider.  The provider then sends a reply message back to
   * this process on behalf of the member receiving the .
   * <pre>
   * requestor ----> member1 --StateStabilizationMessage--> provider --StateStabilizedMessage--> requestor
   *           ----> member2 --StateStabilizationMessage--> provider --StateStabilizedMessage--> requestor
   *           ----> provider --StateStabilizedMessage--> requestor
   * </pre>
   * This flushes the ordered messages in flight between members and the gii
   * provider, so we don't miss data when the image is requested.
   * 
   * @author bruce
   * @since 5.0.1
   * @see StateFlushOperation.StateStabilizationMessage
   * @see StateFlushOperation.StateStabilizedMessage
   *
   */
  public static final class StateMarkerMessage
    extends DistributionMessage implements MessageWithReply {
    /** roll the membership version to force flushing of new ops */
    public boolean flushNewOps;
    /** the member acting as the relay point */
    protected DistributedMember relayRecipient;
    /** the reply processor identity */
    protected int processorId;
    /** the type of executor to use */
    protected int processorType;
    /** the target region's full path */
    protected String regionPath;
    /** the associated Region */
    protected DistributedRegion region;
    /** whether to enable severe alert processing */
    protected transient boolean severeAlertEnabled;
    /**
     * whether all regions must be flushed to the relay target.
     * If this is true, then regionPath may be null.
     */
    protected boolean allRegions;
    
    public StateMarkerMessage() {
      super();
    }
    
    @Override
    public int getProcessorId() { return this.processorId; }
    
    @Override
    final public int getProcessorType() {
      return processorType;
    }
    
    private CacheDistributionAdvisee getRegion(DistributionManager dm) {
      if (region != null) {
        return region;
      }
      // set the init level requirement so that we don't hang in CacheFactory.getInstance() (bug 36175)
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        GemFireCacheImpl gfc = (GemFireCacheImpl)CacheFactory.getInstance(dm.getSystem());
        Region r = gfc.getRegionByPathForProcessing(this.regionPath);
        if (r instanceof DistributedRegion) {
          region = (DistributedRegion)r;
        }
        else if (r == null) {
          // check for ProxyBucketRegion; may need to wait for TX ops on
          // proxy bucket region

          Bucket bucket = PartitionedRegionHelper.getProxyBucketRegion(gfc,
              this.regionPath, false);
          if (bucket != null) {
            return bucket;
          }
        }
      } catch (PRLocallyDestroyedException prlde) {
        // ignore
      }
      finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
      return region;
    }
    
    /** returns a set of all DistributedRegions for allRegions processing */
    private Set<CacheDistributionAdvisee> getAllRegions(DistributionManager dm) {
      // set the init level requirement so that we don't hang in CacheFactory.getInstance() (bug 36175)
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        GemFireCacheImpl gfc = (GemFireCacheImpl)CacheFactory.getInstance(dm.getSystem());
        Set<CacheDistributionAdvisee> result = new HashSet<CacheDistributionAdvisee>();
        for (LocalRegion r: gfc.getAllRegions()) {
          // it's important not to check if the cache is closing, so access
          // the isDestroyed boolean directly
          if (r instanceof DistributedRegion && !r.isDestroyed) {
            result.add((DistributedRegion)r);
          }
        }
        return result;
      }
      finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
    
    @Override
    protected void process(DistributionManager dm) {
      if (StateFlushOperation.DEBUG) {
        dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_PROCESSING__0, this.toString());
      }
      if (dm.getDistributionManagerId().equals(relayRecipient)) {
        // no need to send a relay request to this process - just send the
        // ack back to the sender
        StateStabilizedMessage ga = new StateStabilizedMessage();
        ga.sendingMember = relayRecipient;
        ga.setRecipient(this.getSender());
        ga.setProcessorId(processorId);
        dm.putOutgoing(ga);
      }
      else {
        // 1) wait for all messages based on the membership version (or older)
        //    at which the sender "joined" this region to be put on the pipe
        // 2) record the state of all communication channels from this process
        //    to the relay point
        // 3) send a stabilization message to the relay point that holds the
        //    communication channel state information
        StateStabilizationMessage gr = new StateStabilizationMessage();
        gr.setRecipient((InternalDistributedMember)relayRecipient);
        gr.requestingMember = this.getSender();
        gr.processorId = processorId;
        try {
          Set<CacheDistributionAdvisee> regions;
          if (this.allRegions) {
            regions = getAllRegions(dm);
          } else {
            regions = Collections.singleton(this.getRegion(dm));
          }
          for (CacheDistributionAdvisee r: regions) {
            if (r == null && DistributionManager.VERBOSE) {
              dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_REGION_NOT_FOUND__SKIPPING_CHANNEL_STATE_ASSESSMENT);
            }
            if (r != null) {
              DistributedRegion dr = null;
              if (r instanceof DistributedRegion) {
                dr = (DistributedRegion)r;
              }
              if (this.allRegions && dr != null && dr.doesNotDistribute()) {
                // no need to flush a region that does no distribution
                continue;
              }
              boolean initialized = dr == null || dr.isInitialized();
              if (initialized) {
                if (this.flushNewOps) {
                  r.getDistributionAdvisor().forceNewMembershipVersion(); //force a new "view" so we can track current ops
                }
                try {
                  r.getDistributionAdvisor().waitForCurrentOperations(dm.getLoggerI18n());
                } catch (RegionDestroyedException e) {
                  // continue with the next region
                }
              }
              boolean useMulticast = dr != null && dr.getMulticastEnabled()
                                    && dr.getSystem().getConfig().getMcastPort() != 0;
              if (initialized) {
                HashMap channelStates = dm.getMembershipManager().getChannelStates(relayRecipient, useMulticast);
                if (gr.channelState != null) {
                  gr.channelState.putAll(channelStates);
                } else {
                  gr.channelState = channelStates;
                }
                if (StateFlushOperation.DEBUG && ((gr.channelState != null) && (gr.channelState.size() > 0)) ) {
                  dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_CHANNEL_STATES_0, gr.channelStateDescription(gr.channelState));
                }
              }
            }
          }
        }
        catch (CancelException cce) {
          // cache is closed - no distribution advisor available for the region so nothing to do but
          // send the stabilization message
        }
        catch (Exception e) {
          dm.getLoggerI18n().severe(LocalizedStrings.StateFlushOperation_0__EXCEPTION_CAUGHT_WHILE_DETERMINING_CHANNEL_STATE, this.toString(), e);
        }
        catch (ThreadDeath td) {
          throw td;
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
          dm.getLoggerI18n().severe(LocalizedStrings.StateFlushOperation_0__THROWABLE_CAUGHT_WHILE_DETERMINING_CHANNEL_STATE, this.toString(), t);
        }
        finally {
          if (StateFlushOperation.DEBUG) {
            dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_SENDING__0, gr);
          }
          dm.putOutgoing(gr);
        }
      }
    }
    
    @Override
    public void toData(DataOutput dout) throws IOException {
      super.toData(dout);
      DataSerializer.writeObject(relayRecipient, dout);
      dout.writeInt(processorId);
      dout.writeInt(processorType);
      dout.writeBoolean(allRegions);
      if (!allRegions) {
        DataSerializer.writeString(regionPath, dout);
      }
    }
    
    public int getDSFID() {
      return STATE_MARKER_MESSAGE;
    }

    @Override
    public void fromData(DataInput din) throws IOException, ClassNotFoundException {
      super.fromData(din);
      relayRecipient = (DistributedMember)DataSerializer.readObject(din);
      processorId = din.readInt();
      processorType = din.readInt();
      allRegions = din.readBoolean();
      if (!allRegions) {
        regionPath = DataSerializer.readString(din);
      }
    }
    
    @Override
    public String toString() {
      return "StateMarkerMessage(requestingMember="+this.getSender()
      +",processorId="+processorId+",target="+relayRecipient
      +",region="+regionPath+")";
    }

    @Override
    public boolean isSevereAlertCompatible() {
      return severeAlertEnabled;
    }
    
    
  }
  
  /**
   * StateStabilizationMessage is sent by a distributed member to a member who
   * is the target of a state flush.  The target then sends a StateStabilizedMessage
   * to the sender of the StateStabilizationMessage when all state has been
   * flushed to it.
   * <p>author bruce
   * @see StateFlushOperation.StateStabilizedMessage
   * @see StateFlushOperation.StateMarkerMessage
   * @since 5.0.1
   */
  public static final class StateStabilizationMessage
    extends SerialDistributionMessage  {
    /** the member that requested StateStabilizedMessages */
    protected DistributedMember requestingMember;
    /** the processor id for the requesting member */
    protected int processorId;
    /** a map of the communication channel state between the sending process
     *  and the receiving process */
    protected HashMap channelState;
    /** whether this is a simple request/response two-party flush or (false) a proxied flush */
    protected boolean isSingleFlushTo;
    
    public StateStabilizationMessage() {
      super();
    }
    
    public String channelStateDescription(Object state) {
      if ( ! (state instanceof Map) ) {
        return "unknown channelState content";
      }
      else {
        Map csmap = (Map)state;
        StringBuilder result = new StringBuilder(200);
        for (Iterator it=csmap.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry entry = (Map.Entry)it.next();
          result.append(entry.getKey()).append('=').append(entry.getValue());
          if (it.hasNext()) {
            result.append(", ");
          }
        }
        return result.toString();
      }
    }
    
    @Override
    protected void process(final DistributionManager dm) {
      // though this message must be transmitted on an ordered connection to
      // ensure that datagram channnels are flushed, we need to execute
      // in the waiting pool to avoid blocking those connections
      dm.getWaitingThreadPool().execute(new Runnable() {
        public void run() {
          if (StateFlushOperation.DEBUG) {
            dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_PROCESSING__0,
                StateStabilizationMessage.this);
          }
          try {
            if (channelState != null) {
              if (StateFlushOperation.DEBUG && ((channelState != null) && (channelState.size() > 0)) ) {
                dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_WAITING_FOR_CHANNEL_STATES__0, channelStateDescription(channelState));
              }
              for (;;) {
                dm.getCancelCriterion().checkCancelInProgress(null);
                boolean interrupted = Thread.interrupted();
                try {
                  dm.getMembershipManager().waitForChannelState(getSender(), channelState);
                  break;
                }
                catch (InterruptedException e) {
                  interrupted = true;
                }
                finally {
                  if (interrupted) {
                    Thread.currentThread().interrupt();
                  }
                }
              } // for
            }
          } 
          catch (ThreadDeath td) {
            throw td;
          }
          catch (Throwable e) {
            Error err;
            if (e instanceof Error && SystemFailure.isJVMFailureError(
                err = (Error)e)) {
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
            dm.getLoggerI18n().severe(LocalizedStrings.StateFlushOperation_EXCEPTION_CAUGHT_WHILE_WAITING_FOR_CHANNEL_STATE, e);
          }
          finally {
            StateStabilizedMessage ga = new StateStabilizedMessage();
            ga.setRecipient((InternalDistributedMember)requestingMember);
            if (isSingleFlushTo) {
              // not a proxied message but a simple request-response
              ga.sendingMember = dm.getDistributionManagerId();
            } else {
              ga.sendingMember = getSender();
            }
            ga.setProcessorId(processorId);
            if (StateFlushOperation.DEBUG) {
              dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_SENDING__0, ga);
            }
            if(requestingMember.equals(dm.getDistributionManagerId())) {
              ga.dmProcess(dm);  
            } else {
              dm.putOutgoing(ga);
            }
          }
        }
      });
    }
    
    @Override
    public void toData(DataOutput dout) throws IOException {
      super.toData(dout);
      dout.writeInt(processorId);
      DataSerializer.writeHashMap(channelState, dout);
      DataSerializer.writeObject(requestingMember, dout);
      dout.writeBoolean(this.isSingleFlushTo);
    }
    
    public int getDSFID() {
      return STATE_STABILIZATION_MESSAGE;
    }

    @Override
    public void fromData(DataInput din) throws IOException, ClassNotFoundException {
      super.fromData(din);
      processorId = din.readInt();
      channelState = DataSerializer.readHashMap(din);
      requestingMember = (DistributedMember)DataSerializer.readObject(din);
      this.isSingleFlushTo = din.readBoolean();
    }
    
    @Override
    public String toString() {
      return "StateStabilizationMessage(recipients=" + getRecipientsDescription() + ",requestingMember="+requestingMember+",processorId="+processorId+")";
    }
  }
  
  /**
   * StateStabilizedMessage is sent from a VM that will provide an initial image and is
   * part of a higher-order protocol that is intended to force data in serial
   * execution queues to be processed before the initial image is requested.
   * <p>author bruce
   * @see StateFlushOperation.StateMarkerMessage
   * @see StateFlushOperation.StateStabilizationMessage
   * @since 5.0.1
   *
   */
  public static final class StateStabilizedMessage extends ReplyMessage {
    /** the member for whom this ack is being sent */
    protected DistributedMember sendingMember;
    
    public StateStabilizedMessage() {
      super();
    }

    // overridden to spoof the source of the message
    @Override
    public InternalDistributedMember getSender() {
      return (InternalDistributedMember)this.sendingMember;
    }
    
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor) {
      if (StateFlushOperation.DEBUG) {
        dm.getLoggerI18n().info(LocalizedStrings.StateFlushOperation_PROCESSING__0, this.toString());
      }
      super.process(dm, processor);
    }

    @Override
    public void toData(DataOutput dout) throws IOException {
      super.toData(dout);
      DataSerializer.writeObject(sendingMember, dout);
    }
    
    @Override
    public int getDSFID() {
      return STATE_STABILIZED_MESSAGE;
    }

    @Override
    public void fromData(DataInput din) throws IOException, ClassNotFoundException {
      super.fromData(din);
      sendingMember = (DistributedMember)DataSerializer.readObject(din);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("StateStabilizedMessage ");
      sb.append(this.processorId);
      if (super.getSender() != null) {
        sb.append(" from ");
        sb.append(super.getSender());
      }
      if (getRecipients().length > 0) {
        String recip = getRecipientsDescription();
        sb.append(" to ");
        sb.append(recip);
      }
      sb.append(" on behalf of ");
      sb.append(sendingMember);
      ReplyException ex = this.getException();
      if (ex != null) {
        sb.append(" with exception ");
        sb.append(ex);
      }

      return sb.toString();
    }
  }
  
  /**
   * StateFlushReplyProcessor waits for proxy acks (StateStabilizedMessages) from the target
   * vm.  If the target vm goes away, this processor wakes up immediately
   */
  public static class StateFlushReplyProcessor extends ReplyProcessor21  {
  
    /** the target of the StateFlushOperation */
    InternalDistributedMember targetMember;
    
    int originalCount;
    
    /** whether the target member has left the distributed system */
    boolean targetMemberHasLeft;
  
    public StateFlushReplyProcessor(DM manager, Set initMembers, DistributedMember target) {
      super(manager, initMembers);
      this.targetMember = (InternalDistributedMember)target;
      this.originalCount = initMembers.size();
      this.targetMemberHasLeft = targetMemberHasLeft // bug #43583 - perform an initial membership check
              || !manager.isCurrentMember((InternalDistributedMember)target);
    }
    
    /** process the failure set from sending the message */
    public void messageNotSentTo(Set failures) {
      for (Iterator it=failures.iterator(); it.hasNext(); ) {
        this.memberDeparted((InternalDistributedMember)it.next(), true);
      }
    }

    @Override
    public void memberDeparted(final InternalDistributedMember id, final boolean crashed) {
//      LogWriterI18n log = this.system.getLogWriterI18n();
//      log.warning(LocalizedStrings.DEBUG, "DEBUG: member departed event in SFO for " + id + " crashed=" + crashed + " processor=" + this.toString());
      super.memberDeparted(id, crashed);
    }
    
    @Override
    protected void processActiveMembers(Set activeMembers) {
      super.processActiveMembers(activeMembers);
      if (!activeMembers.contains(this.targetMember)) {
//        LogWriterI18n log = this.system.getLogWriterI18n();
//        log.severe(LocalizedStrings.DEBUG, "DEBUG: found that target member was missing in processActiveMembers");
        targetMemberHasLeft = true;
      }
    }
    
    @Override
    protected boolean stillWaiting() {
      targetMemberHasLeft = targetMemberHasLeft || !getDistributionManager().isCurrentMember(targetMember);
      return super.stillWaiting() && !targetMemberHasLeft;
    }
    
    @Override
    public String toString() {
      return "<" + shortName() + " " + this.getProcessorId() +
        " targeting " + targetMember + " waiting for " + numMembers()
        + " replies out of " + this.originalCount + " " + 
        (exception == null ? "" : (" exception: " + exception)) +
        " from " + membersToString() + ">";
    }
  }
}
