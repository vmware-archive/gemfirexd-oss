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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.AbstractOperationMessage;
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionException;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.sequencelog.EntryLogger;
import com.gemstone.gnu.trove.THashMap;

/**
 * The base PartitionedRegion message type upon which other messages should be
 * based.
 * 
 * @author mthomas
 * @author bruce
 * @since 5.0
 */
public abstract class PartitionMessage extends AbstractOperationMessage
    implements MessageWithReply {

  /** default exception to ensure a false-positive response is never returned */
  static final ForceReattemptException UNHANDLED_EXCEPTION
     = (ForceReattemptException)new ForceReattemptException(LocalizedStrings.PartitionMessage_UNKNOWN_EXCEPTION.toLocalizedString()).fillInStackTrace();

  int regionId;

  /**
   * whether this message is being sent for listener notification
   */
  boolean notificationOnly;

  protected boolean sendDeltaWithFullValue = true;

  /** flag to indicate notification message */
  protected static final short NOTIFICATION_ONLY =
    AbstractOperationMessage.UNRESERVED_FLAGS_START;
  /** flag to indicate ifNew in PutMessages */
  protected static final short IF_NEW = (NOTIFICATION_ONLY << 1);
  /** flag to indicate ifOld in PutMessages */
  protected static final short IF_OLD = (IF_NEW << 1);
  /** flag to indicate that oldValue is required for PutMessages and others */
  protected static final short REQUIRED_OLD_VAL = (IF_OLD << 1);
  /** flag to indicate filterInfo in message */
  protected static final short HAS_FILTER_INFO = (REQUIRED_OLD_VAL << 1);
  /** flag to indicate delta as value in message */
  protected static final short HAS_DELTA = (HAS_FILTER_INFO << 1);
  /** the unreserved flags start for child classes */
  protected static final short UNRESERVED_FLAGS_START = (HAS_DELTA << 1);

  public PartitionMessage() {
  }

  public PartitionMessage(final TXStateInterface tx) {
    super(tx);
  }

  public PartitionMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, final TXStateInterface tx) {
    super(tx);
    Assert.assertTrue(recipient != null,
        "PartitionMesssage recipient can not be null");
    setRecipient(recipient);
    this.regionId = regionId;
    this.processorId = processor == null ? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
  }

  public PartitionMessage(Collection<InternalDistributedMember> recipients,
      int regionId, ReplyProcessor21 processor, final TXStateInterface tx) {
    super(tx);
    setRecipients(recipients);
    this.regionId = regionId;
    this.processorId = processor == null ? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
  }

  /**
   * Copy constructor that initializes the fields declared in this class
   * @param other
   */
  public PartitionMessage(PartitionMessage other) {
    super(other);
    this.regionId = other.regionId;
    this.notificationOnly = other.notificationOnly;
  }

  /**
   * Severe alert processing enables suspect processing at the ack-wait-threshold
   * and issuing of a severe alert at the end of the ack-severe-alert-threshold.
   * Some messages should not support this type of processing
   * (e.g., GII, or DLockRequests)
   * @return whether severe-alert processing may be performed on behalf
   * of this message
   */
  @Override
  public boolean isSevereAlertCompatible() {
    return true;
  }

  protected int getMessageProcessorType() {
    if (this.notificationOnly) {
      // bug #47964 - deadlock with all PR threads busy.  When using UDP
      // messaging serial-executor messages are not in-line processed.  See
      // r37496 for more details.
      InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
      if (system != null && system.getConfig().getDisableTcp()) {
        return DistributionManager.SERIAL_EXECUTOR;
      }
    }
    return DistributionManager.PARTITIONED_REGION_EXECUTOR;
  }

  @Override
  public final int getProcessorType() {
    return this.processorType == 0 ? getMessageProcessorType()
        : this.processorType;
  }

  @Override
  public void setProcessorType(boolean isReaderThread) {
    // if this is a reader/processor thread itself, then use a waiting pool
    // executor on the remote end (#44913/#42459)
    if (isReaderThread && getMessageProcessorType() != DistributionManager
        .WAITING_POOL_EXECUTOR) {
      this.processorType = DistributionManager.WAITING_POOL_EXECUTOR;
    }
  }

  /**
   * @return the compact value that will be sent which represents the
   *         PartitionedRegion
   * @see PartitionedRegion#getPRId()
   */
  public final int getRegionId()
  {
    return regionId;
  }

  /**
   * @param processorId1 the {@link 
   * com.gemstone.gemfire.distributed.internal.ReplyProcessor21} id associated 
   * with the message, null if no acknowlegement is required.
   */
  public final void registerProcessor(int processorId1)
  {
    this.processorId = processorId1;
  }

  /**
   * @return return the message that should be sent to listeners, or null if this message
   * should not be relayed
   */
  public PartitionMessage getMessageForRelayToListeners(EntryEventImpl event, Set recipients) {
    return null;
  }

  /**
   * check to see if the cache is closing
   */
  final public boolean checkCacheClosing(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    // return (cache != null && cache.isClosed());
    return cache == null || cache.isClosed();
  }

  /**
   * check to see if the distributed system is closing
   * @return true if the distributed system is closing
   */
  final public boolean checkDSClosing(DistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }

  /**
   * Upon receipt of the message, both process the message and send an
   * acknowledgement, not necessarily in that order. Note: Any hang in this
   * message may cause a distributed deadlock for those threads waiting for an
   * acknowledgement.
   * 
   * @throws PartitionedRegionException if the region does not exist (typically, if it has been destroyed)
   */
  @Override
  protected void basicProcess(final DistributionManager dm) {
    Throwable thr = null;
    LogWriterI18n logger = null;
    boolean sendReply = true;
    PartitionedRegion pr = null;
    long startTime = 0;
    EntryLogger.setSource(getSender(), "PR");
    try {
      logger = dm.getLoggerI18n();
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        String msg = LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0
            .toLocalizedString(dm.getId());
        thr = new ForceReattemptException(msg, new CacheClosedException(msg));
        return;
      }
//    logger.info("Trying to get pr with id : "+this.regionId);
      pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr == null && failIfRegionMissing()) {
        // if the distributed system is disconnecting, don't send a reply saying
        // the partitioned region can't be found (bug 36585)
        thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_0_COULD_NOT_FIND_PARTITIONED_REGION_WITH_ID_1.toLocalizedString(new Object[] {dm.getDistributionManagerId(), Integer.valueOf(regionId)}));
        return;  // reply sent in finally block below
      }

      if (pr != null) {
        startTime = pr.getPrStats().startPartitionMessageProcessing();
      }
      thr = UNHANDLED_EXCEPTION;

      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache==null) {
        throw new CacheClosedException(
            LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0
                .toLocalizedString(dm.getId()));
      }
      if (getTXId() == null) {
        sendReply = operateOnPartitionedRegion(dm, pr, startTime);
      }
      else {
        final TXManagerImpl txMgr = cache.getTxManager();
        final TXManagerImpl.TXContext context = txMgr.masqueradeAs(this, false,
            true);
        try {
          sendReply = operateOnPartitionedRegion(dm, pr, startTime);
        } finally {
          txMgr.unmasquerade(context, true);
        }
      }
      thr = null;
    } catch (ForceReattemptException fre) {
      thr = fre;
    } catch (DataLocationException fre) {
      thr = new ForceReattemptException(fre.getMessage(), fre);
    }
    catch (DistributedSystemDisconnectedException se) {
      // bug 37026: this is too noisy...
//      throw new CacheClosedException("remote system shutting down");
//      thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.fineEnabled()) {
        logger.fine("shutdown caught, abandoning message: " + se);
      }
    }
    catch (CancelException ce) {
      // force retry
      thr = new ForceReattemptException(ce.getMessage(), ce);
    }
    catch (RegionDestroyedException rde) {
//      if (logger.fineEnabled()) {
//        logger.fine("Region is Destroyed " + rde);
//      }
      // [bruce] RDE does not always mean that the sender's region is also
      //         destroyed, so we must send back an exception.  If the sender's
      //         region is also destroyed, who cares if we send it an exception
      //if (pr != null && pr.isClosed) {
        thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_REGION_IS_DESTROYED_IN_0.toLocalizedString(dm.getDistributionManagerId()), rde);
      //}
    } catch (final Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
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
      // log the exception at fine level if there is no reply to the message
      thr = null;
      if (sendReply) {
        // GemFireXD can throw force reattempt exception wrapped in function
        // exception from index management layer for retries
        if (t.getCause() instanceof ForceReattemptException) {
          thr = t.getCause();
        }
        else if (!checkDSClosing(dm)) {
          thr = t;
        }
        else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_DISTRIBUTED_SYSTEM_IS_DISCONNECTING.toLocalizedString());
        }
      }
      if (logger.fineEnabled()) {
        if (DistributionManager.VERBOSE && (t instanceof RuntimeException)
            && !(t.getCause() instanceof ForceReattemptException)) {
          logger.fine("Exception caught while processing message", t);
        }
      }
    }
    finally {
      if (sendReply) {
        ReplyException rex = null;
        
        if (thr != null) {
          // don't transmit the exception if this message was to a listener
          // and this listener is shutting down
          boolean excludeException = 
            this.notificationOnly
                 && ((thr instanceof CancelException)
                      || (thr instanceof ForceReattemptException));
          
          if (!excludeException) {
            rex = new ReplyException(thr);
          }
        }

        // Send the reply if the operateOnPartitionedRegion returned true
        sendReply(getSender(), this.processorId, dm, rex, pr, startTime);
        EntryLogger.clearSource();
      } 
    }
  }
  
  /** Send a generic ReplyMessage.  This is in a method so that subclasses can override the reply message type
   * @param pr the Partitioned Region for the message whose statistics are incremented
   * @param startTime the start time of the operation in nanoseconds
   *  @see PutMessage#sendReply
   */
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, PartitionedRegion pr, long startTime) {
    if (pr != null && startTime > 0) {
      pr.getPrStats().endPartitionMessagesProcessing(startTime); 
    }

    ReplyMessage.send(member, procId, ex, getReplySender(dm), this,
        pr != null && pr.isInternalRegion());
  }

  /**
   * Allow classes that over-ride to choose whether 
   * a RegionDestroyException is thrown if no partitioned region is found (typically occurs if the message will be sent 
   * before the PartitionedRegion has been fully constructed.
   * @return true if throwing a {@link RegionDestroyedException} is acceptable
   */
  protected boolean failIfRegionMissing() {
    return true;
  }

  /**
   * relay this message to another set of recipients for event notification
   * @param cacheOpRecipients recipients of associated bucket CacheOperationMessage
   * @param adjunctRecipients recipients who unconditionally get the message
   * @param filterRoutingInfo routing information for all recipients
   * @param event the event causing this message
   * @param r the region being operated on
   * @param processor the reply processor to be notified
   */
  public Set relayToListeners(Set cacheOpRecipients, Set adjunctRecipients,
      
      FilterRoutingInfo filterRoutingInfo, 
      EntryEventImpl event, PartitionedRegion r, DirectReplyProcessor processor)
  {
    LogWriterI18n log = r.getCache().getLoggerI18n();
    this.processorId = processor == null? 0 : processor.getProcessorId();
    this.notificationOnly = true;
     if(!adjunctRecipients.isEmpty()) {     
      resetRecipients();
      this.setFilterInfo(filterRoutingInfo);
      setRecipients(adjunctRecipients);
      if (DistributionManager.VERBOSE) {
        log.fine("Relaying partition message to other processes for listener notification");
      }
      return r.getDistributionManager().putOutgoing(this);
      
    }
    return null;
  }

  /**
   * return a new reply processor for this class, for use in relaying a response.
   * This <b>must</b> be an instance method so subclasses can override it
   * properly.
   */
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients,
      final TXStateInterface tx) {
    //r.getCache().getLogger().warning("PartitionMessage.createReplyProcessor()", new Exception("stack trace"));
    return new PartitionResponse(r.getSystem(), recipients, tx);
  }

  /**
   * An operation upon the messages partitioned region which each subclassing
   * message must implement
   * 
   * @param dm
   *          the manager that received the message
   * @param pr
   *          the partitioned region that should be modified
   * @param startTime the start time of the operation
   * @return true if a reply message should be sent
   * @throws CacheException if an error is generated in the remote cache
   * @throws DataLocationException if the peer is no longer available
   */
  protected abstract boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException, QueryException,
      DataLocationException, InterruptedException, IOException;

  /**
   * Fill out this instance of the message using the <code>DataInput</code>
   * Required to be a {@link com.gemstone.gemfire.DataSerializable}Note: must
   * be symmetric with {@link #toData(DataOutput)}in what it reads
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    setBooleans(this.flags);
    this.regionId = in.readInt();
  }

  /**
   * Re-construct the booleans using the compressed short. A subclass must override
   * this method if it is using bits in the compressed short.
   */
  protected void setBooleans(short s) {
    this.notificationOnly = ((s & NOTIFICATION_ONLY) != 0);
  }

  /**
   * Send the contents of this instance to the DataOutput Required to be a
   * {@link com.gemstone.gemfire.DataSerializable}Note: must be symmetric with
   * {@link #fromData(DataInput)}in what it writes
   */
  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.regionId);
  }

  /**
   * Sets the bits of a short by using the bit masks. A subclass must override
   * this method if it is using bits in the compressed short.
   * @return short with appropriate bits set
   */
  @Override
  protected short computeCompressedShort(short s) {
    if (this.notificationOnly) s |= NOTIFICATION_ONLY;
    return s;
  }

  //private final static String PN_TOKEN = ".cache."; 

  /**
   * Helper class of {@link #toString()}
   * 
   * @param buff
   *          buffer in which to append the state of this instance
   */
  @Override
  protected void appendFields(StringBuilder buff) {
    // className.substring(className.lastIndexOf('.', className.lastIndexOf('.')
    // - 1) + 1); // partition.<foo> more generic version
    //buff.append(className.substring(className.indexOf(PN_TOKEN) +
    //PN_TOKEN.length())); // partition.<foo>
    buff.append("; prid="); // make sure this is the first one
    buff.append(this.regionId);

    // Append name, if we have it
    String name = null;
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr != null) {
        name = pr.getFullPath();
      }
    } catch (Exception e) { /* ignored */
    }
    if (name != null) {
      buff.append("; name=\"").append(name).append('"');
    }
    if (this.notificationOnly) {
      buff.append("; notificationOnly=").append(this.notificationOnly);
    }
  }

  public InternalDistributedMember getRecipient() {
    return getRecipients()[0];
  }
  
  public void setOperation(Operation op) {
    // override in subclasses holding operations
  }
  
  /**
   * added to support old value to be written on wire.
   * @param value true or false
   * @since 5.5
   */
  public void setHasOldValue(boolean value) {
    // override in subclasses which need old value to be serialized.
    // overridden by classes like PutMessage, DestroyMessage.
  }
  
  /**
   * added to support routing of notification-only messages to clients
   */
  public void setFilterInfo(FilterRoutingInfo filterInfo) {
    // subclasses that support routing to clients should reimplement this method
  }

  /*
  public void appendOldValueToMessage(EntryEventImpl event) {
    
  }*/

  public void setSendDeltaWithFullValue(boolean bool) {
    this.sendDeltaWithFullValue = bool;
  }
  /**
   * A processor on which to await a response from the {@link PartitionMessage}
   * recipient, capturing any CacheException thrown by the recipient and handle
   * it as an expected exception.
   * 
   * @author Mitch Thomas
   * @since 5.0
   * @see #waitForCacheException()
   */
  public static class PartitionResponse extends DirectReplyProcessor {
    /**
     * The exception thrown when the recipient does not reply
     */
    volatile ForceReattemptException prce;
    
    /**
     * Whether a response has been received
     */
    volatile boolean responseReceived;
    
    /**
     * whether a response is required
     */
    boolean responseRequired;

    final transient TXStateInterface txState;

    volatile THashMap exceptions;

    public PartitionResponse(InternalDistributedSystem dm, Set initMembers,
        final TXStateInterface tx) {
      this(dm, initMembers, true, tx);
    }

    public PartitionResponse(InternalDistributedSystem dm, Set initMembers,
        boolean register, final TXStateInterface tx) {
      super(dm, initMembers);
      this.txState = tx;
      if (register) {
        register();
      }
    }

    public PartitionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member) {
      this(dm, member, true);
    }

    public PartitionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member, boolean register) {
      // all callers to this method are currently non-transactional
      this(dm, member, register, null);
    }

    public PartitionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member, boolean register,
        final TXStateInterface tx) {
      super(dm, member);
      this.txState = tx;
      if (register) {
        register();
      }
    }

    /**
     * require a response message to be received
     */
    public void requireResponse() {
      this.responseRequired = true;
    }
    
    @Override
    public void memberDeparted(final InternalDistributedMember id, final boolean crashed) {
      if (id != null) {
        if (removeMember(id, true)) {
          this.prce =  new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1.toLocalizedString(new Object[] {id, Boolean.valueOf(crashed)}));
        }
        checkIfDone();
      } else {
        Exception e = new Exception(LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID.toLocalizedString());
        getDistributionManager().getLoggerI18n().info(LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID_CRASHED_0, Boolean.valueOf(crashed), e);
      }
    }

    /**
     * Waits for the response from the {@link PartitionMessage}'s recipient
     * @throws CacheException  if the recipient threw a cache exception during message processing 
     * @throws ForceReattemptException if the recipient left the distributed system before the response
     * was received.  
     * @throws PrimaryBucketException 
     */
    final public void waitForCacheException() 
        throws CacheException, ForceReattemptException, PrimaryBucketException {
      try {
        waitForRepliesUninterruptibly();
        if (this.prce!=null || (this.responseRequired && !this.responseReceived)) {
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_ATTEMPT_FAILED.toLocalizedString(), this.prce);
        }
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CacheException) {
          throw (CacheException)t;
        }
        else if (t instanceof ForceReattemptException) {
          ForceReattemptException ft = (ForceReattemptException)t;
          // See FetchEntriesMessage, which can marshal a ForceReattempt
          // across to the sender
          ForceReattemptException fre = new ForceReattemptException(LocalizedStrings.PartitionMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
          if (ft.hasHash()) {
            fre.setHash(ft.getHash());
          }
          throw fre;
        }
        else if (t instanceof PrimaryBucketException) {
          // See FetchEntryMessage, GetMessage, InvalidateMessage,
          // PutMessage
          // which can marshal a ForceReattemptacross to the sender
          throw new PrimaryBucketException(LocalizedStrings.PartitionMessage_PEER_FAILED_PRIMARY_TEST.toLocalizedString(), t);
        }
        else if (t instanceof CancelException) {
          if (getDistributionManager().getLoggerI18n().fineEnabled()) {
            getDistributionManager().getLoggerI18n().fine("PartitionResponse got CacheClosedException from "
                + e.getSender() + ", throwing ForceReattemptException");
          }
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION.toLocalizedString(), t);
        }
        else if (t instanceof DiskAccessException) {
          if (getDistributionManager().getLoggerI18n().fineEnabled()) {
            getDistributionManager().getLoggerI18n().fine("PartitionResponse got DiskAccessException from "
                + e.getSender() + ", throwing ForceReattemptException");
          }
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION.toLocalizedString(), t);
        }
        else if (t instanceof LowMemoryException) {
          if (getDistributionManager().getLoggerI18n().fineEnabled()) {
            getDistributionManager().getLoggerI18n().fine("PartitionResponse re-throwing remote LowMemoryException from "
                + e.getSender(), t);
          }
          throw (LowMemoryException) t;
        }
        e.handleAsUnexpected();
      }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final Map<InternalDistributedMember, ReplyException> getExceptions() {
      if (this.exceptions != null) {
        synchronized (this) {
          return new THashMap((Map)this.exceptions);
        }
      }
      return null;
    }

    @Override
    protected final boolean stopBecauseOfExceptions() {
      // do not stop in transactions since we have to ensure that commit or
      // rollback is fired only after everything is done
      if (this.txState == null) {
        return super.stopBecauseOfExceptions();
      }
      return false;
    }

    @Override
    protected final synchronized void processException(
        final DistributionMessage msg, final ReplyException ex) {
      if (this.exceptions == null) {
        this.exceptions = new THashMap();
      }
      this.exceptions.put(msg.getSender(), ex);
      super.processException(msg, ex);
    }

    /* overridden from ReplyProcessor21 */
    @Override
    public void process(DistributionMessage msg) {
      this.responseReceived = true;
      super.process(msg);
    }
  }
}
