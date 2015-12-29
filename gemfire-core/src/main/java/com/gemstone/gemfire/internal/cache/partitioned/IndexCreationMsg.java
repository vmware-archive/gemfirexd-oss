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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.IndexCreationException;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionException;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class represents a partion index creation message. An instance of this
 * class is send over the wire to created indexes on remote vms. This class
 * extends PartitionMessage
 * {@link com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage}
 * 
 * @author rdubey
 * 
 */
public final class IndexCreationMsg extends PartitionMessage
  {
  // this class should have all the relevant information about parition index.
  // so that the index on remote host should be able to create an index of
  // exactlly the same type name etc.

  /**
   * String representing the name of the index.
   */
  private String name;

  /**
   * String representing the from clause for the index.
   */
  private String fromClause;

  /**
   * String representing the indexed expression for the partitioned index.
   */
  private String indexedExpression;

  /**
   * Byte representing what type of index is needed. 0 for primary index and 1
   * for functional index.
   */
  private byte indexType;

//  /**
//   * byte value for primary index.
//   */
//  private byte primaryIndex = 0;

  /**
   * byte value for funcitonal index.
   */
  private final byte functional = 1;
  
  /**
   * byte value for hash index.
   */
  private byte hash = 2;
  
  /**
   * Imports needed to create an index.
   */
  private String imports;
  
  /**
   * Boolean indicating imports are needed.
   */
  private boolean importsNeeded;

  /**
   * This message may be sent to nodes before the PartitionedRegion is
   * completely initialized due to the RegionAdvisor(s) knowing about the
   * existance of a partitioned region at a very early part of the
   * initialization
   */
  @Override
  protected final boolean failIfRegionMissing() {
    return false;
  }

 
  /**
   * This method actually operates on the partitioned region and creates a given
   * type of index from an index creation message.
   * 
   * @param dm
   *          distribution manager.
   * @param pr
   *          partitioned region on which to create an index.
   * @throws CacheException
   *           indicating a cache level error
   * @throws ForceReattemptException
   *           if the peer is no longer available
   */
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException, ForceReattemptException
  {
    // region exists
    final LogWriterI18n l = pr.getCache().getLoggerI18n();
    ReplyException replyEx = null;
    boolean result = false;
    boolean alreadyHasAnIndex = false;
    if (l.fineEnabled()) {
      l.fine("Processing index creation message on this remote partitioned region vm: " +
        this.name + ", the from clause: " + this.fromClause + ", the indexed expression: " +
        this.indexedExpression + ", and the type of index: " + this.indexType + ".");
    }

    try {
      if (0 == this.indexType)
        pr.createIndex(true, IndexType.PRIMARY_KEY, this.name,
            this.indexedExpression, this.fromClause, this.imports);
      if (1 == this.indexType)
        pr.createIndex(true, IndexType.FUNCTIONAL, this.name,
            this.indexedExpression, this.fromClause, this.imports);
      if (2 == this.indexType)
        pr.createIndex(true, IndexType.HASH, this.name,
            this.indexedExpression, this.fromClause, this.imports);

      // l.info("XXXX will be throwing an exception ");
      // throw new IndexCreationException("Test exception thrown from
      // operateOnPartitionedRegion in IndexCreationMsg."); for testing .
    }
    catch (IndexCreationException exx) {
      if (l.fineEnabled()) {
        l.fine("Got an IndexCreationException" +  exx.getMessage());
      }
      replyEx = new ReplyException(LocalizedStrings.IndexCreationMsg_REMOTE_INDEX_CREAION_FAILED.toLocalizedString(), exx);
    }
    catch (IndexNameConflictException exx) {
      if (l.fineEnabled()) {
        l.fine("Got an IndexNameConflictException for Index name : " + 
          this.name  + ". Exception recieved is : " + exx.getMessage());
      }
      replyEx = new ReplyException(LocalizedStrings.IndexCreationMsg_REMOTE_INDEX_CREAION_FAILED.toLocalizedString(), exx);
    }
    catch (IndexExistsException existsException) {
      if (l.fineEnabled()) {
        l.fine("Got an IndexExistsExcepiton :" + existsException.getMessage());
      }
      alreadyHasAnIndex = true;
      replyEx = new ReplyException(LocalizedStrings.IndexCreationMsg_REMOTE_INDEX_CREATION_FAILED.toLocalizedString(), existsException);
    }// catch()
    
    if (null == replyEx) {
      result = true;
    }
    
    if (result && pr.getIndex() != null && (pr.getIndex()).containsKey(this.name)) {
      Map index = pr.getIndex();
      PartitionedIndex pi = (PartitionedIndex)index.get(this.name);
      int indexedBuckets;
      if (pi == null) { // no index
        indexedBuckets = 0;
      }
      else {
        indexedBuckets = pi.getNumberOfIndexedBuckets();
      }
      sendReply(getSender(), getProcessorId(), dm, replyEx, result,
          indexedBuckets, pr.getDataStore().getAllLocalBuckets().size(),
          alreadyHasAnIndex);
    }
    else {
      // send 0 instead of actual number of buckets indexed.
      sendReply(getSender(), getProcessorId(), dm, replyEx, result, 0, pr
          .getDataStore().getAllLocalBuckets().size(), alreadyHasAnIndex);
    }

    if (l.fineEnabled()) {
      l.fine("Index creation completed on remote host and has sent the reply to the originating vm.");
    }
    return false;
  }

  /**
   * Process this index creation message on the receiver.
   */
  @Override
   protected final void basicProcess(final DistributionManager dm) {

     Throwable thr = null;
     LogWriterI18n logger = null;
     boolean sendReply = true;
     PartitionedRegion pr = null;
     
     try {
         logger = dm.getLoggerI18n();
         if ( logger.fineEnabled() ) {
           logger.fine("Trying to get pr with id: " + this.regionId);
         }
         try {
            if ( logger.fineEnabled() ) {
              logger.fine("Again trying to get pr with id : "+this.regionId);
            }
           pr = PartitionedRegion.getPRFromId(this.regionId);
           if ( logger.fineEnabled() ) {
             logger.fine("Index creation message got the pr " + pr);
           }
           if(null == pr) {
               boolean wait = true;
               int attempts = 0;
               while (wait && attempts < 30) { // max 30 seconds of wait.
                 dm.getCancelCriterion().checkCancelInProgress(null);
                 if ( logger.fineEnabled() ) {
                   logger.fine("Waiting for Partitioned Region to be intialized with id "+this.regionId+" for processing index creation messages ");
                 }
                 try {
                   boolean interrupted = Thread.interrupted();
                   try {
                     Thread.sleep(500);
                   }
                   catch (InterruptedException e) {
                     interrupted = true;
                     dm.getCancelCriterion().checkCancelInProgress(e);
                   }
                   finally {
                     if (interrupted) Thread.currentThread().interrupt();
                   }

                   pr = PartitionedRegion.getPRFromId(this.regionId);
                   if ( null != pr ) {
                     wait = false;
                     if (logger.fineEnabled()) {
                       logger.fine("Indexcreation message got the pr " + pr);
                     }
                   }
                   attempts++;
                 }
                 catch (CancelException ignorAndLoopWait) {
                   if (logger.fineEnabled() ) {
                     logger.fine("IndexCreationMsg waiting for pr to be properly"
                       + " created with prId : " + this.regionId); 
                   }
                 }
               }
                          
           }
         }
         catch (CancelException letPRInitialized) {
           // Not sure if the CacheClosedException is still thrown in response
           // to the PR being initialized.
           if (logger.fineEnabled() ) {
           logger
               .fine("Waiting for notification from pr being properly created "
                   + "" + this.regionId);
           }
                       
             boolean wait = true;
             while (wait) {
               dm.getCancelCriterion().checkCancelInProgress(null);
               try {
                 boolean interrupted = Thread.interrupted();
                 try {
                   Thread.sleep(500);
                 }
                 catch (InterruptedException e) {
                   interrupted = true;
                   dm.getCancelCriterion().checkCancelInProgress(e);
                 }
                 finally {
                   if (interrupted) Thread.currentThread().interrupt();
                 }
                 pr = PartitionedRegion.getPRFromId(this.regionId);
                 wait = false;
                 if (logger.fineEnabled() ) {
                   logger.fine("Indexcreation message got the pr " + pr);
                 }
               }
               catch (CancelException ignorAndLoopWait) {
                 if (logger.fineEnabled()) {
                 logger.fine("IndexCreationMsg waiting for pr to be properly"
                     + " created with prId : " + this.regionId);
                 }
               }
             }
           
         }

         if (pr == null /* && failIfRegionMissing() */ ) {
           String msg = LocalizedStrings.IndexCreationMsg_COULD_NOT_GET_PARTITIONED_REGION_FROM_ID_0_FOR_MESSAGE_1_RECEIVED_ON_MEMBER_2_MAP_3.toLocalizedString(new Object[] {Integer.valueOf(this.regionId), this, dm.getId(), PartitionedRegion.dumpPRId()});
           throw new PartitionedRegionException(msg, new RegionNotFoundException(msg));
         }
         sendReply = operateOnPartitionedRegion(dm, pr, 0);

     
     }
     catch (PRLocallyDestroyedException pre) {
       if (logger.fineEnabled()) {
         logger.fine("Region is locally Destroyed ");
       }
       thr = pre;
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
       // check for fatal JVM error (see above). However, there is
       // _still_ a possibility that you are dealing with a cascading
       // error condition, so you also need to check to see if the JVM
       // is still usable:
       SystemFailure.checkFailure();
       // log the exception at fine level if there is no reply to the message
       if (this.processorId == 0) {
         logger.fine(this + " exception while processing message:", t);
       }
       else if (DistributionManager.VERBOSE && (t instanceof RuntimeException)) {
         logger.fine("Exception caught while processing message", t);
       }
       if (t instanceof RegionDestroyedException && pr != null) {
         if (pr.isClosed) {
           logger.info(LocalizedStrings.IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_THROWING_REGIONDESTROYEDEXCEPTION_FOR__0, pr);
           thr = new RegionDestroyedException(LocalizedStrings.IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_ON_0.toLocalizedString(dm.getId()), pr.getFullPath());
         }
       }
       else {
         thr = t;
       }
     }
     finally {
       if (sendReply && this.processorId != 0) {
         ReplyException rex = null;
         if (thr != null) {
           rex = new ReplyException(thr);
         }
         sendReply(getSender(), this.processorId, dm, rex, pr, 0);
       }
     }
   
     
   }

  /**
   * Constructor for index creation message to be sent over the wire with all
   * the relevant information.
   * 
   * @param recipients
   *          members to which this message has to be sent
   * @param regionId
   *          partitioned region id
   * @param processor
   *          The processor to reply to 
   * @param name
   *          name of the index
   * @param fromClause
   *          from clause for the index
   * @param indexExpression
   *          indexed expression for the index
   * @param type
   *          the type of index.
   */

  IndexCreationMsg(Set recipients, int regionId, ReplyProcessor21 processor,
      String name, String fromClause, String indexExpression,
      IndexType type, String imports) {
    super(recipients, regionId, processor, null);
    this.name = name;
    this.fromClause = fromClause;
    this.indexedExpression = indexExpression;
    if (IndexType.FUNCTIONAL == type)
      this.indexType = functional;
    if (IndexType.HASH == type)
      this.indexType = hash;
    this.imports= imports;
    if (null != imports)
      this.importsNeeded = true;

  }

  /**
   * Empty default constructor.
   * 
   */
  public IndexCreationMsg() {

  }

  /**
   * Methods that sends the actual index creation message to all the members.
   * 
   * @param recipient
   *          set of members.
   * @param pr
   *          partitoned region associated with the index.
   * @param index
   *          actual index created.
   * @return partitionresponse a response for the index creation
   */
  public static PartitionResponse send(InternalDistributedMember recipient,
      PartitionedRegion pr, PartitionedIndex index)
  {

    RegionAdvisor advisor = (RegionAdvisor)(pr.getDistributionAdvisor());
    final Set<InternalDistributedMember> recipients;
    /*
     * Will only send create index to all the members storing data
     */
    if (null == recipient) {
      recipients = new HashSet(advisor.adviseDataStore());
    }
    else {
      recipients = new HashSet<InternalDistributedMember>();
      recipients.add(recipient);
    }
    LogWriterI18n logger = pr.getLogWriterI18n();
    IndexCreationResponse processor = null;
    if (logger.fineEnabled()) {
      logger.fine("Will be sending create index msg to : " + recipients.toString());
    }
    if (recipients.size() > 0) {
      processor = (IndexCreationResponse)(new IndexCreationMsg())
        .createReplyProcessor(pr, recipients, null);
    }
    // PartionedIndex index = (PartionedIndex)pr.getIndex();

    IndexCreationMsg indMsg = new IndexCreationMsg(recipients, pr.getPRId(),
        processor, index.getName(),
        index.getCanonicalizedFromClause(), index
            .getCanonicalizedIndexedExpression(), index.getType(), index.getImports());
    if (logger.fineEnabled()) {
      logger.fine("Sending index creation message: " + indMsg + ", to member(s) " +
          recipients.toString() + ".");
    }
    /*Set failures =*/ pr.getDistributionManager().putOutgoing(indMsg);
    // Set failures =r.getDistributionManager().putOutgoing(m);
    // if (failures != null && failures.size() > 0) {
    // throw new ForceReattemptException("Failed sending <" + indMsg + ">");
    // }
    return processor;
  }
  
  
  // override reply processor type from PartitionMessage
  @Override
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients,
      final TXStateInterface tx) {
    // r.getCache().getLogger().warning("PutMessage.createReplyProcessor()", new
    // Exception("stack trace"));
    return new IndexCreationResponse(r.getSystem(), recipients, tx);
  }

  /**
   * Send a reply for index creation message.
   * 
   * @param member
   *          representing the actual index creatro in the system
   * @param procId
   *          waiting processor
   * @param dm
   *          distirbution manager to send the message
   * @param ex
   *          any exceptions
   * @param result
   *          represents index created properly or not.
   * @param bucketsIndexed
   *          number of local buckets indexed.
   * @param totalNumBuckets
   *          number of total buckets.
   * @param alreadyHasAnIndex
   *          represents that this index alredy exists
   */
  void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, boolean result, int bucketsIndexed,
      int totalNumBuckets, boolean alreadyHasAnIndex)
  {
    IndexCreationReplyMsg.send(member, procId, dm, ex, result, bucketsIndexed,
        totalNumBuckets, alreadyHasAnIndex);
  }

  public int getDSFID() {
    return PR_INDEX_CREATION_MSG;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.name = in.readUTF();
    this.fromClause = in.readUTF();
    this.indexedExpression = in.readUTF();
    this.indexType = in.readByte();
    this.importsNeeded = in.readBoolean();
    if (this.importsNeeded)
      this.imports = in.readUTF();
    // this.ifNew = in.readBoolean();
    // this.ifOld = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    out.writeUTF(this.name);
    out.writeUTF(this.fromClause);
    out.writeUTF(this.indexedExpression);
    out.writeByte(this.indexType);
    out.writeBoolean(this.importsNeeded);
    if (this.importsNeeded)
      out.writeUTF(this.imports);
    // out.writeBoolean(this.ifNew);
    // out.writeBoolean(this.ifOld);
  }

  /**
   * Helper class of {@link #toString()}
   * 
   * @param buff
   *          buffer in which to append the state of this instance
   */
  @Override
  protected void appendFields(StringBuilder buff) {
    buff.append("; Index name [ " + this.name);
    buff.append(" ] Index from Clause [ " + this.fromClause);
    buff.append(" ] Index indexedExpression [ " + this.indexedExpression);
    if (this.indexType == 0) {
      buff.append(" ] Index type [ " + IndexType.PRIMARY_KEY);
    }
    else if (this.indexType == 2){
      buff.append(" ] Index type [ " + IndexType.HASH+" ] ");
    }
    else {
      buff.append(" ] Index type [ " + IndexType.FUNCTIONAL + " ] ");
    }
  }

  /**
   * Class representing index creation response. This class has all the
   * information for successful or unsucessful index creation on this member of
   * the partitioned region.
   * 
   * @author rdubey
   * 
   */
  public static class IndexCreationResponse extends PartitionResponse
   {
    /** Index created or not. */
//    private boolean result;

    /** Number of buckets indexed locally. */
    private int numRemoteBucketsIndexed;

    /** Number of total bukets in this vm. */
    private int numRemoteTotalBuckets;

    /**
     * Construtor for index creation response message.
     * 
     * @param ds
     *          distributed system for this member.
     * @param recipients
     *          all the member associated with the index
     */
    IndexCreationResponse(InternalDistributedSystem ds, Set recipients,
        final TXStateInterface tx) {
      super(ds, recipients, tx);
    }

    /**
     * Waits for the response from the members for index creation.
     * 
     * @return indexCreationResult result for index creation.
     * @throws CacheException
     *           indicating a cache level error
     * @throws ForceReattemptException
     *           if the peer is no longer available
     */
    public IndexCreationResult waitForResult() throws CacheException,
        ForceReattemptException
    {
      try {
        waitForCacheException();
      } catch (RuntimeException re) {
        if (re instanceof PartitionedRegionException) {
          if (re.getCause() instanceof RegionNotFoundException) {
            // Region may not be available at the receiver.
            // ignore the exception.
            // This will happen when the region on the remote end is still in
            // initialization mode and is not yet created the region ID.
          } else {
            throw re;
          }
        } else {
          throw re;
        }
      }
      return new IndexCreationResult(this.numRemoteBucketsIndexed,
          this.numRemoteTotalBuckets);
    }

    /**
     * Sets the relevant information in the response.
     * 
     * @param result
     *          true if index created properly
     * @param numBucketsIndexed
     *          number of local buckets indexed.
     * @param numTotalBuckets
     *          number of total buckets in the member.
     */
    public void setResponse(boolean result, int numBucketsIndexed,
        int numTotalBuckets)
    {
//      this.result = result;
      this.numRemoteBucketsIndexed += numBucketsIndexed;
      this.numRemoteTotalBuckets += numTotalBuckets;
    }
  }

  /**
   * Class representing index creation result.
   * @author rdubey
   *
   */
  public static class IndexCreationResult
   {
    /**
     * Number of buckets indexed.
     */
    int numBucketsIndexed;

    /**
     * Total number of bukets.
     */
    int numTotalBuckets;

    /**
     * Construtor for index creation result.
     * @param numBucketsIndexed number of buckets indexed.
     * @param numTotalBuckets number of total buckets.
     */
    IndexCreationResult(int numBucketsIndexed, int numTotalBuckets) {
      this.numBucketsIndexed = numBucketsIndexed;
      this.numTotalBuckets = numTotalBuckets;
    }

    /** 
     * Returns number of buckets indexed.
     * @return numBucketsIndexed
     */
    public int getNumBucketsIndexed()
    {
      return this.numBucketsIndexed;

    }

    /**
     * Returns num of total buckets.
     * @return numTotalBuckets.
     */

    public int getNumTotalBuckets()
    {
      return this.numTotalBuckets;
    }

  }

  /**
   * Class for index creation reply. This class has the information about sucessful
   * or unsucessful index creation.
   * @author rdubey
   *
   */
  public static final class IndexCreationReplyMsg extends ReplyMessage
   {

    /** Index created or not. */
    private boolean result;

    /** Number of buckets indexed locally. */
    private int numBucketsIndexed;

    /** Number of total bukets in this vm. */
    private int numTotalBuckets;

    /** Boolean indecating weather its a data store. */
    private boolean isDataStore;

    /** Already created an index */
    private boolean alreadyHasAnIndex;

    public IndexCreationReplyMsg() {

    }

    /**
     * Constructor for index creation reply message.
     * @param processorId processor id of the waiting processor
     * @param ex any exceptions
     * @param result ture if index created properly else false
     * @param numBucketsIndexed number of buckets indexed.
     * @param numTotalBuckets number of total buckets.
     * @param isDataStore true is the vm is a datastore esle false
     * @param alreadyHasAnIndex true if the vm already has an index of this type.
     */
    IndexCreationReplyMsg(int processorId, ReplyException ex, boolean result,
        int numBucketsIndexed, int numTotalBuckets, boolean isDataStore,
        boolean alreadyHasAnIndex) {
      super();
      super.setException(ex);
      this.result = result;
      this.numBucketsIndexed = numBucketsIndexed;
      this.numTotalBuckets = numTotalBuckets;
      this.isDataStore = isDataStore;
      this.alreadyHasAnIndex = alreadyHasAnIndex;
      setProcessorId(processorId);
    }

    @Override
    public int getDSFID() {
      return PR_INDEX_CREATION_REPLY_MSG;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.result = in.readBoolean();
      this.numBucketsIndexed = in.readInt();
      this.numTotalBuckets = in.readInt();
      this.isDataStore = in.readBoolean();
      this.alreadyHasAnIndex = in.readBoolean();

    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeBoolean(this.result);
      out.writeInt(this.numBucketsIndexed);
      out.writeInt(this.numTotalBuckets);
      out.writeBoolean(this.isDataStore);
      out.writeBoolean(this.alreadyHasAnIndex);
    }

    /**
     * Actual method sending the index creation reply message.
     * @param recipient the originator of index creation message
     * @param processorId waiting processor id
     * @param dm distribution manager
     * @param ex any exceptions
     * @param result true is index created sucessfully
     * @param numBucketsIndexed number of buckets indexed
     * @param numTotalBuckets total number of buckets
     * @param alreadyHasAnIndex true if this member had an index of similar type
     */
    public static void send(InternalDistributedMember recipient,
        int processorId, DM dm, ReplyException ex, boolean result,
        int numBucketsIndexed, int numTotalBuckets, boolean alreadyHasAnIndex)
    {
      IndexCreationReplyMsg indMsg = new IndexCreationReplyMsg(processorId, ex,
          result, numBucketsIndexed, numTotalBuckets, true, alreadyHasAnIndex);
      indMsg.setRecipient(recipient);
      dm.putOutgoing(indMsg);
    }

    /**
     * Processes the index creation result.
     * @param dm distribution manager
     */
    @Override
    public final void process(final DM dm, final ReplyProcessor21 p)
    {
      LogWriterI18n l = dm.getLoggerI18n();
      l.fine("Processor id is : "+this.processorId);
      IndexCreationResponse processor = (IndexCreationResponse)p;
      if (processor != null) {
        processor.setResponse(this.result, this.numBucketsIndexed,
            this.numTotalBuckets);
        processor.process(this);
      }
    }

  }

}
