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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.hdfs.internal.AbstractBucketRegionQueue;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.BucketRegionQueue;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;
import com.gemstone.gemfire.internal.util.concurrent.StoppableCondition;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantLock;
import com.gemstone.gnu.trove.THashSet;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public class ParallelGatewaySenderQueue implements RegionQueue {

  protected final Map<String, PartitionedRegion> userRegionNameToshadowPRMap = new ConcurrentHashMap<String, PartitionedRegion>();

  // <PartitionedRegion, Map<Integer, List<Object>>>
  private final Map regionToDispatchedKeysMap = new ConcurrentHashMap();

  protected final StoppableReentrantLock buckToDispatchLock;
  
  private final StoppableCondition regionToDispatchedKeysMapEmpty; 

  protected final StoppableReentrantLock queueEmptyLock;
  
  private volatile boolean isQueueEmpty = true;
  
  /**
   * False signal is fine on this condition.
   * As processor will loop again and find out if it was a false signal.
   * However, make sure that whatever scenario can cause an entry to be peeked
   * shoudld signal the processor to unblock.
   */
  private StoppableCondition queueEmptyCondition;
  
  protected final LogWriterI18n logger;
  
  protected final GatewaySenderStats stats;
  
  protected volatile boolean resetLastPeeked = false;
  
  
  
  /**
   * There will be one shadow pr for each of the the PartitionedRegion which has added the GatewaySender
   * Fix for Bug#45917
   * We maintain a tempQueue to queue events when buckets are not available locally.
   */
  private final ConcurrentMap<Integer, BlockingQueue<GatewaySenderEventImpl>> bucketToTempQueueMap = new ConcurrentHashMap<Integer, BlockingQueue<GatewaySenderEventImpl>>();

  /**
   * The default frequency (in milliseconds) at which a message will be sent by the
   * primary to all the secondary nodes to remove the events which have already
   * been dispatched from the queue.
   */
  public static final int DEFAULT_MESSAGE_SYNC_INTERVAL = 10;
  //TODO:REF: how to change the message sync interval ? should it be common for serial and parallel  
  protected static volatile int messageSyncInterval = DEFAULT_MESSAGE_SYNC_INTERVAL;
  //TODO:REF: name change for thread, as it appears in the log
  private BatchRemovalThread removalThread = null;

  protected final BlockingQueue<GatewaySenderEventImpl> peekedEvents =
    new LinkedBlockingQueue<GatewaySenderEventImpl>();
  
  public final ParallelGatewaySenderImpl sender ;
  
  public static final int WAIT_CYCLE_SHADOW_BUCKET_LOAD = 10;
  
  public static final String QSTRING = "_PARALLEL_QUEUE_";

  /**
   * Fixed size Thread pool for conflating the events in the queue. The size of
   * the thread pool is set to the number of processors available to the JVM.
   * There will be one thread pool per ParallelGatewaySender on a node.
   */
  private volatile ExecutorService conflationExecutor;

  private final static Random randomVar = new Random();
  
  /**
   * This class carries out the actual removal of the previousTailKey from QPR.
   * The class implements Runnable and the destroy operation is done in the run
   * method. The Runnable is executed by the one of the threads in the
   * conflation thread pool configured above.
   */
  private class ConflationHandler implements Runnable {
    Conflatable conflatableObject;

    Long previousTailKeyTobeRemoved;

    int bucketId;

    public ConflationHandler(Conflatable conflatableObject, int bId,
        Long previousTailKey) {
      this.conflatableObject = conflatableObject;
      this.previousTailKeyTobeRemoved = previousTailKey;
      this.bucketId = bId;
    }

    public void run() {
      PartitionedRegion prQ = null;
      GatewaySenderEventImpl event = (GatewaySenderEventImpl)conflatableObject;
      try {
        String regionPath = ColocationHelper.getLeaderRegion((PartitionedRegion)event.getRegion()).getFullPath();
        prQ = userRegionNameToshadowPRMap.get(regionPath);
        destroyEventFromQueue(prQ, bucketId, previousTailKeyTobeRemoved);
      } catch (EntryNotFoundException e) {
        if (logger.fineEnabled()) {
          logger.fine(this + ": Not conflating "
              + conflatableObject.getKeyToConflate()
              + "due to EntryNotFoundException ");
        }
      }
      if (logger.fineEnabled()) {
        logger.fine(this + ": " + "Conflated "
            + conflatableObject.getValueToConflate() + " for key="
            + conflatableObject.getKeyToConflate() + " in queue for region="
            + prQ.getName());
      }
    }

    private Object deserialize(Object serializedBytes) {
      Object deserializedObject = serializedBytes;
      if (serializedBytes instanceof byte[]) {
        byte[] serializedBytesCast = (byte[])serializedBytes;
        // This is a debugging method so ignore all exceptions like
        // ClassNotFoundException
        try {
          deserializedObject = EntryEventImpl.deserialize(serializedBytesCast);
        } catch (Exception e) {
        }
      }
      return deserializedObject;
    }
  }
  
  final protected int index; 
  final protected int nDispatcher;
  
  /**
   * A transient queue to maintain the eventSeqNum of the events that are to be
   * sent to remote site. It is cleared when the queue is cleared.
   */
  //private final BlockingQueue<Long> eventSeqNumQueue;  
  
  public ParallelGatewaySenderQueue(AbstractGatewaySender sender,
      Set<Region> userRegions, int idx, int nDispatcher) {
    this.index = idx;
    this.nDispatcher = nDispatcher;
	  
    this.logger = sender.getLogger();
    this.stats = sender.getStatistics();
    this.sender = (ParallelGatewaySenderImpl)sender;
    
    List<Region> listOfRegions = new ArrayList<Region>(userRegions);
    //eventSeqNumQueue = new LinkedBlockingQueue<Long>();
    Collections.sort(listOfRegions, new Comparator<Region>() {
      @Override
      public int compare(Region o1, Region o2) {
        return o1.getFullPath().compareTo(o2.getFullPath());
      }
    });
    
    for (Region userRegion : listOfRegions) {
      if(userRegion instanceof PartitionedRegion){
        addShadowPartitionedRegionForUserPR((PartitionedRegion)userRegion);  
      }else{
        addShadowPartitionedRegionForUserRR((DistributedRegion)userRegion);
      }
    }
    this.buckToDispatchLock = new StoppableReentrantLock(sender.getCancelCriterion());
    this.regionToDispatchedKeysMapEmpty = this.buckToDispatchLock.newCondition();
    queueEmptyLock = new StoppableReentrantLock(sender.getCancelCriterion());
    queueEmptyCondition = queueEmptyLock.newCondition();
    this.removalThread = new BatchRemovalThread((GemFireCacheImpl)sender.getCache());
    this.removalThread.start();
    
    //initialize the conflation thread pool if conflation is enabled
    if (sender.isBatchConflationEnabled()) {
      initializeConflationThreadPool();
    }
  }

  public void addShadowPartitionedRegionForUserRR(
      DistributedRegion userRegion) {
    this.sender.lifeCycleLock.writeLock().lock();
    PartitionedRegion prQ = null;

    if (this.logger.fineEnabled()) {
      this.logger.fine("Going to create shadowpr for userRegion "
          + userRegion.getFullPath());
    }

    try {
      String regionName = userRegion.getFullPath();

      if (this.userRegionNameToshadowPRMap.containsKey(regionName))
        return;

      GemFireCacheImpl cache = (GemFireCacheImpl)sender.getCache();
      final String prQName = sender.getId() + QSTRING + convertPathToName(userRegion.getFullPath());
      prQ = (PartitionedRegion)cache.getRegion(prQName);
      if (prQ == null) {
        // TODO:REF:Avoid deprecated apis
        AttributesFactory fact = new AttributesFactory();
        //Fix for 48621 - don't enable concurrency checks 
        //for queue buckets., event with persistence
        fact.setConcurrencyChecksEnabled(false);
        PartitionAttributesFactory pfact = new PartitionAttributesFactory();
        pfact.setTotalNumBuckets(sender.getMaxParallelismForReplicatedRegion()); 
        int localMaxMemory = userRegion.getDataPolicy().withStorage() ? sender
            .getMaximumQueueMemory() : 0;
        pfact.setLocalMaxMemory(localMaxMemory);
        pfact.setRedundantCopies(3); //TODO:Kishor : THis need to be handled nicely
        pfact.setPartitionResolver(new RREventIDResolver());
        if (sender.isPersistenceEnabled()) {
          fact.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        }
        
        fact.setDiskStoreName(sender.getDiskStoreName());
        
        // if persistence is enabled, set the diskSyncronous to whatever user
        // has set
        // else set it to false
      //optimize with above check of enable persistence
        if (sender.isPersistenceEnabled())
          fact.setDiskSynchronous(sender.isDiskSynchronous());
        else {
          fact.setDiskSynchronous(false);
        }

        // allow for no overflow directory
        EvictionAttributes ea = EvictionAttributes.createLIFOMemoryAttributes(
            sender.getMaximumQueueMemory(), EvictionAction.OVERFLOW_TO_DISK);

        fact.setEvictionAttributes(ea);
        fact.setPartitionAttributes(pfact.create());

        final RegionAttributes ra = fact.create();

        if (this.logger.fineEnabled()) {
          this.logger.fine(this + ": Attempting to create queue region: "
              + prQName);
        }

        ParallelGatewaySenderQueueMetaRegion meta = new ParallelGatewaySenderQueueMetaRegion(
            prQName, ra, null, cache, sender);

        try {
          prQ = (PartitionedRegion)cache.createVMRegion(prQName, ra,
              new InternalRegionArguments().setInternalMetaRegion(meta)
                  .setDestroyLockFlag(true).setSnapshotInputStream(null)
                  .setImageTarget(null));

          if (this.logger.fineEnabled()) {
            this.logger.fine("Region created  : " + prQ + "partition Attributes : " + prQ.getPartitionAttributes());
          }

          // Suranjan: TODO This should not be set on the PR but on the
          // GatewaySender
          prQ.enableConflation(sender.isBatchConflationEnabled());
          
          // Before going ahead, make sure all the buckets of shadowPR are
          // loaded
          // and primary nodes have been decided.
          // This is required in case of persistent PR and sender.
          if (prQ.getLocalMaxMemory() != 0) {
            Iterator<Integer> itr = prQ.getRegionAdvisor().getBucketSet()
                .iterator();
            while (itr.hasNext()) {
              itr.next();
            }
          }
       // In case of Replicated Region it may not be necessary.
          
//          if (sender.isPersistenceEnabled()) {
//            //Kishor: I need to write a test for this code.
//            Set<Integer> allBucketsClone = new HashSet<Integer>();
//            // allBucketsClone.addAll(allBuckets);*/
//            for (int i = 0; i < sender.getMaxParallelismForReplicatedRegion(); i++)
//              allBucketsClone.add(i);
//
//            while (!(allBucketsClone.size() == 0)) {
//              if (logger.fineEnabled()) {
//                logger.fine("Need to wait until partitionedRegionQueue <<"
//                    + prQ.getName() + ">> is loaded with all the buckets");
//              }
//              Iterator<Integer> itr = allBucketsClone.iterator();
//              while (itr.hasNext()) {
//                InternalDistributedMember node = prQ.getNodeForBucketWrite(
//                    itr.next(), null);
//                if (node != null) {
//                  itr.remove();
//                }
//              }
//              // after the iteration is over, sleep for sometime before trying
//              // again
//              try {
//                Thread.sleep(WAIT_CYCLE_SHADOW_BUCKET_LOAD);
//              }
//              catch (InterruptedException e) {
//                logger.error(e);
//              }
//            }
//          }
        }
        catch (IOException veryUnLikely) {
          this.logger
              .severe(
                  LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass(), veryUnLikely);
        }
        catch (ClassNotFoundException alsoUnlikely) {
          this.logger
              .severe(
                  LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass(), alsoUnlikely);
        }
        if (this.logger.fineEnabled()) {
          this.logger.fine(this + ": Created queue region: " + prQ);
        }
      }
      else {
        // in case shadowPR exists already (can be possible when sender is
        // started from stop operation)
      	if(this.index == 0) //HItesh: for first processor only
        	handleShadowPRExistsScenario(cache, prQ);
      }
      /*
       * Here, enqueTempEvents need to be invoked when a sender is already
       * running and userPR is created later. When the flow comes here through
       * start() method of sender i.e. userPR already exists and sender is
       * started later, the enqueTempEvents is done in the start() method of
       * ParallelGatewaySender
       */
      if (this.sender.isRunning()) {
        ((AbstractGatewaySender)sender).enqueTempEvents();
      }
    }
    finally {
      if (prQ != null)
        this.userRegionNameToshadowPRMap.put(userRegion.getFullPath(), prQ);
      this.sender.lifeCycleLock.writeLock().unlock();
    }
  }
  
  private String convertPathToName(String fullPath) {
    return fullPath.replaceAll("/", "_");
  }

  public void addShadowPartitionedRegionForUserPR(PartitionedRegion userPR) {
	  if (this.logger.fineEnabled()) {
          this.logger.fine(this + ": Attempting1 to create queue region: "
              + userPR.getDisplayName());
        }
    this.sender.lifeCycleLock.writeLock().lock();
    
    PartitionedRegion prQ = null;
    try {
      String regionName = userPR.getFullPath();
      // Find if there is any parent region for this userPR
      // if there is then no need to add another q for the same
      String leaderRegionName = ColocationHelper.getLeaderRegion(userPR)
          .getFullPath();
      if(!regionName.equals(leaderRegionName)) {
        //Fix for defect #50364. Allow user to attach GatewaySender to child PR (without attaching to leader PR) 
    	//though, internally, colocate the GatewaySender's shadowPR with the leader PR in colocation chain
        if (!this.userRegionNameToshadowPRMap.containsKey(leaderRegionName)) {
        	addShadowPartitionedRegionForUserPR(ColocationHelper.getLeaderRegion(userPR));
        }
        return;  
      }

      if (this.userRegionNameToshadowPRMap.containsKey(regionName))
        return;
      
      GemFireCacheImpl cache = (GemFireCacheImpl)sender.getCache();
      boolean isAccessor = (userPR.getLocalMaxMemory() == 0);
      
      final String prQName = sender.getId()
          + QSTRING + convertPathToName(userPR.getFullPath());
      prQ = (PartitionedRegion)cache.getRegion(prQName);
      if (prQ == null) {
        //TODO:REF:Avoid deprecated apis
        
        AttributesFactory fact = new AttributesFactory();
        fact.setConcurrencyChecksEnabled(false);
        PartitionAttributesFactory pfact = new PartitionAttributesFactory();
        pfact.setTotalNumBuckets(userPR.getTotalNumberOfBuckets());
        pfact.setRedundantCopies(userPR.getRedundantCopies());
        pfact.setColocatedWith(regionName);
        // EITHER set localMaxMemory to 0 for accessor node
        // OR override shadowPRs default local max memory with the sender's max
        // queue memory (Fix for bug#44254)
        int localMaxMemory = isAccessor ? 0 : sender
            .getMaximumQueueMemory();
        pfact.setLocalMaxMemory(localMaxMemory);
        
        pfact.setStartupRecoveryDelay(userPR.getPartitionAttributes().getStartupRecoveryDelay());
        pfact.setRecoveryDelay(userPR.getPartitionAttributes().getRecoveryDelay());
        
        if(sender.isPersistenceEnabled() && !isAccessor) {
          fact.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        }
        
        fact.setDiskStoreName(sender.getDiskStoreName());
        
        //if persistence is enabled, set the diskSyncronous to whatever user has set
        //else set it to false
        if (sender.isPersistenceEnabled())
          fact.setDiskSynchronous(sender.isDiskSynchronous());
        else {
          fact.setDiskSynchronous(false);
        }
        
        EvictionAttributes ea = null;
        // allow for no overflow directory 
        if (!GemFireCacheImpl.getExisting().isHadoopGfxdLonerMode()) {
          ea = EvictionAttributes.createLIFOMemoryAttributes(
          sender.getMaximumQueueMemory(), EvictionAction.OVERFLOW_TO_DISK);
        }
        else {
          ea = EvictionAttributes.createLIFOMemoryAttributes(
          sender.getMaximumQueueMemory(), EvictionAction.NONE);
        }

        fact.setEvictionAttributes(ea);
        fact.setPartitionAttributes(pfact.create());

        final RegionAttributes ra = fact.create();

        if (this.logger.fineEnabled()) {
          this.logger.fine(this + ": Attempting to create queue region: "
              + prQName);
        }

        ParallelGatewaySenderQueueMetaRegion meta = new ParallelGatewaySenderQueueMetaRegion(
            prQName, ra, null, cache, sender, isUsedForHDFS());

        try {
          prQ = (PartitionedRegion)cache
              .createVMRegion(prQName, ra, new InternalRegionArguments()
                  .setInternalMetaRegion(meta).setDestroyLockFlag(true)
                  .setSnapshotInputStream(null).setImageTarget(null)
                  .setUUID(this.sender.getUUID()));
          // at this point we should be able to assert prQ == meta; 
          
          //Suranjan: TODO This should not be set on the PR but on the GatewaySender
          prQ.enableConflation(sender
              .isBatchConflationEnabled());
          if (isAccessor)
            return; // return from here if accessor node

          // if the current node is marked uninitialized (GFXD DDL replay in
          // progress) then we cannot wait for buckets to recover, because
          // bucket creation has been disabled until DDL replay is complete.
          if(!prQ.getCache().isUnInitializedMember(prQ.getDistributionManager().getId())) {
            //Wait for buckets to be recovered.
            prQ.shadowPRWaitForBucketRecovery();
          }

        } catch (IOException veryUnLikely) {
          this.logger
              .severe(
                  LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass(), veryUnLikely);
        } catch (ClassNotFoundException alsoUnlikely) {
          this.logger
              .severe(
                  LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass(), alsoUnlikely);
        }
        if (this.logger.fineEnabled()) {
          this.logger.fine(this + ": Created queue region: "
              + prQ);
        }
      } else {
        if (isAccessor)
          return; // return from here if accessor node
        // in case shadowPR exists already (can be possible when sender is
        // started from stop operation)
        if(this.index == 0) //HItesh:for first parallelGatewaySenderQueue only 
        	handleShadowPRExistsScenario(cache, prQ);
      }

    } finally {
      if (prQ != null)
        this.userRegionNameToshadowPRMap.put(userPR.getFullPath(), prQ);
      /*
       * Here, enqueTempEvents need to be invoked when a sender is already
       * running and userPR is created later. When the flow comes here through
       * start() method of sender i.e. userPR already exists and sender is
       * started later, the enqueTempEvents is done in the start() method of
       * ParallelGatewaySender
       */
      if (this.sender.isRunning()) {
        ((AbstractGatewaySender)sender).enqueTempEvents();
      }
      afterRegionAdd(userPR);
      this.sender.lifeCycleLock.writeLock().unlock();
    }
  }

  /**
   * This will be case when the sender is started again after stop operation.
   */
  private void handleShadowPRExistsScenario(Cache cache, PartitionedRegion prQ) {
    //Note: The region will not be null if the sender is started again after stop operation
    if (logger.fineEnabled()) {
      logger.fine(this
          + ": No need to create the region as the region has been retrieved: "
          + prQ);
    }
    // now, clean up the shadowPR's buckets on this node (primary as well as
    // secondary) for a fresh start
    Set<BucketRegion> localBucketRegions = prQ.getDataStore()
        .getAllLocalBucketRegions();
    for (BucketRegion bucketRegion : localBucketRegions) {
      bucketRegion.clear();
    }
  }
  protected boolean isUsedForHDFS()
  {
    return false;
  }
  protected void afterRegionAdd (PartitionedRegion userPR) {

  }
  /**
   * Initialize the thread pool, setting the number of threads that is equal 
   * to the number of processors available to the JVM.
   */
  private void initializeConflationThreadPool() {
    final LogWriterImpl.LoggingThreadGroup loggingThreadGroup = LogWriterImpl
        .createThreadGroup("WAN Queue Conflation Logger Group", this.logger);

    final ThreadFactory threadFactory = new ThreadFactory() {
      public Thread newThread(final Runnable task) {
        final Thread thread = new Thread(loggingThreadGroup, task,
            "WAN Queue Conflation Thread");
        thread.setDaemon(true);
        return thread;
      }
    };
    conflationExecutor = Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors(), threadFactory);
  }
  
  /**
   * Cleans up the conflation thread pool. 
   * Initially, shutdown is done to avoid accepting any newly submitted tasks.
   * Wait a while for existing tasks to terminate. If the existing tasks still don't 
   * complete, cancel them by calling shutdownNow. 
   */
  private void cleanupConflationThreadPool() {
    conflationExecutor.shutdown();// Disable new tasks from being submitted
    
    try {
    if (!conflationExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
      conflationExecutor.shutdownNow(); // Cancel currently executing tasks
      // Wait a while for tasks to respond to being cancelled
      if (!conflationExecutor.awaitTermination(1, TimeUnit.SECONDS))
        logger.warning(LocalizedStrings.ParallelGatewaySenderQueue_COULD_NOT_TERMINATE_CONFLATION_THREADPOOL, this.sender);
    }
    } catch (InterruptedException e) {
      // (Re-)Cancel if current thread also interrupted
      conflationExecutor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }
  
  public void put(Object object) throws InterruptedException, CacheException {
    //Suranjan : Can this region ever be null? Should we work with regionName and not with region instance. 
    // It can't be as put is happeing on the region and its still under process
    GatewaySenderEventImpl value = (GatewaySenderEventImpl)object;
    boolean isDREvent = isDREvent(value);
    
//    if (isDREvent(value)) {
//      putInShadowPRForReplicatedRegion(object);
//      value.freeOffHeapValue();
//      return;
//    }
    Region region = value.getRegion();
    String regionPath = null;
    if (isDREvent) {
      regionPath = region.getFullPath();
    }
    else {
      regionPath = ColocationHelper.getLeaderRegion((PartitionedRegion)region)
          .getFullPath();
    }
    if (logger.fineEnabled()) {
      logger.fine(" Put is for the region " + region);
    }
    if (!this.userRegionNameToshadowPRMap.containsKey(regionPath)) {
      if (logger.fineEnabled()) {
        logger.fine("The userRegionNameToshadowPRMap is " + userRegionNameToshadowPRMap);
      }
      if (logger.warningEnabled()) {
        logger
            .warning(
                LocalizedStrings.NOT_QUEUING_AS_USERPR_IS_NOT_YET_CONFIGURED,
                value);
      }
      value.release();
      return;
    }
    
    PartitionedRegion prQ = this.userRegionNameToshadowPRMap.get(regionPath);
    int bucketId = value.getBucketId();
    Object key = null;
    if(!isDREvent){
      long k = value.getShadowKey();
      
      if (k == -1) {
        // In case of parallel we don't expect
        // the key to be not set. If it is the case then the event must be coming
        // through listener, so return.
        if (logger.fineEnabled()) {
          logger.fine("ParallelGatewaySenderOrderedQueue not putting key " + k
              + " : Value : " + value);
        }
        value.release();
        return;
      }
      key = k;
    }else{
      key = value.getEventId();
    }
    
    if (logger.fineEnabled()) {
      logger.fine("ParallelGatewaySenderOrderedQueue putting key " + key
          + ":: Value : "+value );
    }
    AbstractBucketRegionQueue brq = (AbstractBucketRegionQueue)prQ.getDataStore()
        .getLocalBucketById(bucketId);
    
    try {
      if (brq == null) {
        // Set the threadInitLevel to BEFORE_INITIAL_IMAGE.
        int oldLevel = LocalRegion
            .setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
        try {
          // Full path of the bucket:

          final String bucketFullPath = Region.SEPARATOR
              + PartitionedRegionHelper.PR_ROOT_REGION_NAME + Region.SEPARATOR
              + prQ.getBucketName(bucketId);

          brq = (AbstractBucketRegionQueue)prQ.getCache().getRegionByPath(
              bucketFullPath, false);
          if (logger.fineEnabled()) {
            logger
                .fine("ParallelGatewaySenderOrderedQueue : The bucket in the cache is "
                    + " bucketRegionName : "
                    + bucketFullPath
                    + " bucket: "
                    + brq);
          }

          if (brq != null) {
            brq.getInitializationLock().readLock().lock();
            try {
              putIntoBucketRegionQueue(brq, key, value);
            } finally {
              brq.getInitializationLock().readLock().unlock();
            }
          } else if (isDREvent) {
            // in case of DR with PGS, if shadow bucket is not found event after
            // above search then it means that bucket is not intended for this
            // node. So lets not add this event in temp queue event as we are
            // doing it for PRevent
            value.release();
          } else {
            // We have to handle the case where brq is null because the
            // colocation
            // chain is getting destroyed one by one starting from child region
            // i.e this bucket due to moveBucket operation
            // In that case we don't want to store this event.
            if (((PartitionedRegion)prQ.getColocatedWithRegion())
                .getRegionAdvisor().getBucketAdvisor(bucketId)
                .getShadowBucketDestroyed()) {
              if (logger.fineEnabled()) {
                logger
                    .fine("ParallelGatewaySenderOrderedQueue not putting key "
                        + key + " : Value : " + value
                        + " as shadowPR bucket is destroyed.");
              }
              value.release();
            } else {
              /**
               * This is to prevent data loss, in the scenario when bucket is
               * not available in the cache but we know that it will be created.
               */
              BlockingQueue tempQueue = null;
              synchronized (this.bucketToTempQueueMap) {
                tempQueue = this.bucketToTempQueueMap.get(bucketId);
                if (tempQueue == null) {
                  tempQueue = new LinkedBlockingQueue();
                  this.bucketToTempQueueMap.put(bucketId, tempQueue);
                }
              }

              synchronized (tempQueue) {
                brq = (AbstractBucketRegionQueue)prQ.getCache()
                    .getRegionByPath(bucketFullPath, false);
                if (brq != null) {
                  brq.getInitializationLock().readLock().lock();
                  try {
                    putIntoBucketRegionQueue(brq, key, value);
                  } finally {
                    brq.getInitializationLock().readLock().unlock();
                  }
                } else {
                  // tempQueue = this.bucketToTempQueueMap.get(bucketId);
                  // if (tempQueue == null) {
                  // tempQueue = new LinkedBlockingQueue();
                  // this.bucketToTempQueueMap.put(bucketId, tempQueue);
                  // }
                  tempQueue.add(value);
                  // TODO OFFHEAP is value refCount ok here?
                  // For debugging purpose.
                  if (this.logger.fineEnabled()) {
                    this.logger
                        .fine("The value "
                            + value
                            + " is enqueued to the tempQueue for the BucketRegionQueue.");
                  }
                }
              }
            }
            // }
          }

        } finally {
          LocalRegion.setThreadInitLevelRequirement(oldLevel);
        }
      } else {
        boolean thisbucketDestroyed = false;

        if (!isDREvent) {
          thisbucketDestroyed = ((PartitionedRegion)prQ
              .getColocatedWithRegion()).getRegionAdvisor()
              .getBucketAdvisor(bucketId).getShadowBucketDestroyed()
              || brq.isDestroyed();
        } else {
          thisbucketDestroyed = brq.isDestroyed();
        }

        if (!thisbucketDestroyed) {
          putIntoBucketRegionQueue(brq, key, value);
        } else {
          if (logger.fineEnabled()) {
            logger.fine("ParallelGatewaySenderOrderedQueue not putting key "
                + key + " : Value : " + value
                + " as shadowPR bucket is destroyed.");
          }
          value.release();
        }
      }
    } finally {
      notifyEventProcessorIfRequired();
    }
  }

  public void notifyEventProcessorIfRequired() {
    queueEmptyLock.lock();
    try {
      if(getLogger().fineEnabled()) {
        getLogger().fine("Going to notify, isQueueEmpty " + isQueueEmpty);
      }
      if (isQueueEmpty) {
        isQueueEmpty = false;
        queueEmptyCondition.signal();
      }
    } finally {
      if(getLogger().fineEnabled()) {
        getLogger().fine("Notified!, isQueueEmpty " + isQueueEmpty);
      }
      queueEmptyLock.unlock();
    }
  }

  private void putIntoBucketRegionQueue(AbstractBucketRegionQueue brq, Object key,
      GatewaySenderEventImpl value) {
    boolean addedValueToQueue = false;
    try {
      if (brq != null) {
        addedValueToQueue = brq.addToQueue(key, value);
      } 
    } catch (BucketNotFoundException e) {
      if (this.logger.fineEnabled()) {
        this.logger.fine("For bucket"
            + brq.getId()
            + " the current bucket redundancy is "
            + brq.getPartitionedRegion().getRegionAdvisor().getBucketAdvisor(brq.getId())
                .getBucketRedundancy());
      }
    } catch (ForceReattemptException e) {
      if (logger.fineEnabled()) {
        logger
            .fine("getInitializedBucketForId: Got ForceReattemptException for "
                + this + " for bucket = " + brq.getId());
      }
    } finally {
      if (!addedValueToQueue) {
        value.release();
      }
    }
  } 

  /**
   * This returns queueRegion if there is only one PartitionedRegion using the GatewaySender
   * Otherwise it returns null.
   */
  public Region getRegion() {
    return this.userRegionNameToshadowPRMap.size() == 1 ? (Region)this.userRegionNameToshadowPRMap
        .values().toArray()[0] : null;
  }

  public PartitionedRegion getRegion(String fullpath) {
    return this.userRegionNameToshadowPRMap.get(fullpath);
  }

  public PartitionedRegion removeShadowPR(String fullpath) {
    try {
      this.sender.lifeCycleLock.writeLock().lock();
      this.sender.setEnqueuedAllTempQueueEvents(false);
      return this.userRegionNameToshadowPRMap.remove(fullpath);
    }
    finally {
      sender.lifeCycleLock.writeLock().unlock();
    }
  }
  
  public ExecutorService getConflationExecutor() {
    return this.conflationExecutor;
  }

  /**
   * Returns the set of shadowPR backign this queue.
   */
  public Set<PartitionedRegion> getRegions() {
    return new HashSet(this.userRegionNameToshadowPRMap.values());
  }
  
  // TODO: Suranjan Find optimal way to get Random shadow pr as this will be called in each put and peek.
  protected PartitionedRegion getRandomShadowPR() {
    int size = this.userRegionNameToshadowPRMap.size();
    if (size > 0) {
      int randomIndex = randomVar.nextInt(size);
      Object[] list = this.userRegionNameToshadowPRMap.values().toArray();
      Object prq = null;
      // size of list has changed, return null this time. 
      if (this.userRegionNameToshadowPRMap.size() != size) 
        return null;
      else
        prq = list[randomIndex];
      if (prq != null)
        return (PartitionedRegion)prq;
    }
//    if (this.userPRToshadowPRMap.values().size() > 0
//        && (prQ == null)) {
//      prQ = getRandomShadowPR();
//    }
    return null;
  }
  
  private boolean isDREvent(GatewaySenderEventImpl event){
    return (event.getRegion() instanceof DistributedRegion) ? true : false;
  }
  /**
   * Take will choose a random BucketRegionQueue which is primary and will take the head element
   * from it.
   */
  @Override
  public Object take() throws CacheException, InterruptedException {
    throw new UnsupportedOperationException();
  }
  
  /**
   * TODO: Optimization needed. We are creating 1 array list for each peek!!
   * @return BucketRegionQueue
   */
  private final BucketRegionQueue getRandomBucketRegionQueue() {
    PartitionedRegion prQ = getRandomShadowPR();
    if( prQ != null) {
      final PartitionedRegionDataStore ds = prQ.getDataStore();
      final List<Integer> buckets = new ArrayList<Integer>(
          ds.getAllLocalPrimaryBucketIds());
      if (buckets.isEmpty())
        return null;
      final int index = new Random().nextInt(buckets.size());
      final int brqId = buckets.get(index);
      final BucketRegionQueue brq = (BucketRegionQueue)ds
          .getLocalBucketById(brqId);
      if (brq.isReadyForPeek()) {
        return brq;
      }
    }
    return null;
  }

  protected int getRandomPrimaryBucket(PartitionedRegion prQ) {
    if (prQ != null) {
      List<Integer> buckets = new ArrayList<Integer>(prQ.getDataStore()
          .getAllLocalPrimaryBucketIds());
      List<Integer> thisProcessorBuckets = new ArrayList<Integer>();
      for(Integer bId : buckets) {
    	if(bId % this.nDispatcher == this.index) {
    		thisProcessorBuckets.add(bId);
    	}
      }
      if (logger.fineEnabled()) {
        logger
            .fine("getRandomPrimaryBucket: total " + buckets.size() + " for this processor:" + thisProcessorBuckets.size()  );
      }
      
      // TODO:REF: instead of shuffle use random number, in this method we are
      // returning id instead we should return BRQ itself
      Collections.shuffle(thisProcessorBuckets);
      for (Integer bucketId : thisProcessorBuckets) {
        BucketRegionQueue br = (BucketRegionQueue)prQ.getDataStore()
            .getLocalBucketById(bucketId);
        
        if (br != null && br.isReadyForPeek()) {
          return br.getId();
        }
      }
    }
    return -1;
  }
  
  @Override
  public List take(int batchSize) throws CacheException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void remove() throws CacheException {
    if (!this.peekedEvents.isEmpty()) {

      GatewaySenderEventImpl event = this.peekedEvents.remove();
      try {
      // PartitionedRegion prQ = this.userPRToshadowPRMap.get(ColocationHelper
      // .getLeaderRegion((PartitionedRegion)event.getRegion()).getFullPath());
      //
      PartitionedRegion prQ = null;
      int bucketId = -1;
      Object key = null;
      if (event.getRegion() != null) {
        if (isDREvent(event)) {
          prQ = this.userRegionNameToshadowPRMap.get(event.getRegion()
              .getFullPath());
          bucketId = event.getEventId().getBucketID();
          key = event.getEventId();
        } else {
          prQ = this.userRegionNameToshadowPRMap.get(ColocationHelper
              .getLeaderRegion((PartitionedRegion)event.getRegion())
              .getFullPath());
          bucketId = event.getBucketId();
          key = event.getShadowKey();
        }
      } else {
        String regionPath = event.getRegionPath();
        GemFireCacheImpl cache = (GemFireCacheImpl)this.sender.getCache();
        Region region = (PartitionedRegion)cache.getRegion(regionPath);
        if (region != null && !region.isDestroyed()) {
          // TODO: Suranjan We have to get colocated parent region for this
          // region
          if (region instanceof DistributedRegion) {
            prQ = this.userRegionNameToshadowPRMap.get(region.getFullPath());
            event.getBucketId();
            key = event.getEventId();
          } else {
            prQ = this.userRegionNameToshadowPRMap.get(ColocationHelper
                .getLeaderRegion((PartitionedRegion)region).getFullPath());
            event.getBucketId();
            key = event.getShadowKey();
          }
        }
      }

      if (prQ != null) {        
        destroyEventFromQueue(prQ, bucketId, key);
      }
      } finally {
        event.release();
      }
    }
  }

  @Override
  public void release() {
    ConcurrentMap<Integer, BlockingQueue<GatewaySenderEventImpl>> tmpQueueMap =
        this.bucketToTempQueueMap;
    if(tmpQueueMap != null) {
      Iterator<BlockingQueue<GatewaySenderEventImpl>> iter = tmpQueueMap.values().iterator();
      while(iter.hasNext()) {
        BlockingQueue<GatewaySenderEventImpl> queue =iter.next();
        while(!queue.isEmpty()) {
          GatewaySenderEventImpl event = queue.remove();          
          event.release();
        }
      }
    }
    
  }
  
  private void destroyEventFromQueue(PartitionedRegion prQ, int bucketId,
      Object key) {
    boolean isPrimary = prQ.getRegionAdvisor().getBucketAdvisor(bucketId)
        .isPrimary();
    if (isPrimary) {
      BucketRegionQueue brq = (BucketRegionQueue)prQ.getDataStore()
          .getLocalBucketById(bucketId);
      // TODO : Kishor : Make sure we dont need to initalize a bucket
      // before destroying a key from it
      try {
        if (brq != null) {
          brq.destroyKey(key);
        }
        stats.decQueueSize();
      } catch (EntryNotFoundException e) {
        if (logger.fineEnabled()) {
          logger.fine("ParallelGatewaySenderQueue#remove: "
              + "Got EntryNotFoundException for " + this + " for bucket = "
              + bucketId);
        }
      } catch (ForceReattemptException e) {
        if (logger.fineEnabled()) {
          logger.fine("Bucket :" + bucketId + " Moved to other member");
        }
      } catch (PrimaryBucketException e) {
        if (logger.fineEnabled()) {
          logger.fine("primary bucket :" + bucketId + " Moved to other member");
        }
      }
      addRemovedEvent(prQ, bucketId, key);
    }
  }

  public void resetLastPeeked() {
    this.resetLastPeeked = true;
  }
  
  // Need to improve here.If first peek returns NULL then look in another bucket.
  @Override
  public Object peek() throws InterruptedException, CacheException {
    Object object = null;
    
    int bucketId = -1;
    PartitionedRegion prQ = getRandomShadowPR();
    if (prQ != null && prQ.getDataStore().getAllLocalBucketRegions()
        .size() > 0
        && ((bucketId = getRandomPrimaryBucket(prQ)) != -1)) {
      BucketRegionQueue brq;
      try {
        brq = ((BucketRegionQueue)prQ.getDataStore()
            .getInitializedBucketForId(null, bucketId));
        object = brq.peek();
      } catch (BucketRegionQueueUnavailableException e) {
        return object;//since this is not set, it would be null
      } catch (ForceReattemptException e) {
        if (logger.warningEnabled()) {
          logger.fine("remove: " + "Got ForceReattemptException for " + this
              + " for bucket = " + bucketId);
        }
      }
    }
    return object; // OFFHEAP: ok since only callers uses it to check for empty queue
  }
  
  // This method may need synchronization in case it is used by
  // ConcurrentParallelGatewaySender
  protected void addRemovedEvent(PartitionedRegion prQ, int bucketId, Object key) {
    buckToDispatchLock.lock();
    boolean wasEmpty = regionToDispatchedKeysMap.isEmpty();
    try {
      Map bucketIdToDispatchedKeys = (Map)regionToDispatchedKeysMap.get(prQ.getFullPath());
      if (bucketIdToDispatchedKeys == null) {
        bucketIdToDispatchedKeys = new ConcurrentHashMap();
        regionToDispatchedKeysMap.put(prQ.getFullPath(), bucketIdToDispatchedKeys);
      }
      addRemovedEventToMap(bucketIdToDispatchedKeys, bucketId, key);
      if (wasEmpty) {
        regionToDispatchedKeysMapEmpty.signal();        
      }
    }
    finally {
      buckToDispatchLock.unlock();  
    }
  }

  private void addRemovedEventToMap(Map bucketIdToDispatchedKeys, int bucketId,
      Object key) {
    List dispatchedKeys = (List)bucketIdToDispatchedKeys.get(bucketId);
    if (dispatchedKeys == null) {
      dispatchedKeys = new ArrayList<Object>();
      bucketIdToDispatchedKeys.put(bucketId, dispatchedKeys);
    }
    dispatchedKeys.add(key);
  }

  protected void addRemovedEvents(PartitionedRegion prQ, int bucketId,
      List<Object> shadowKeys) {
    buckToDispatchLock.lock(); 
    boolean wasEmpty = regionToDispatchedKeysMap.isEmpty();
    try {
      Map bucketIdToDispatchedKeys = (Map)regionToDispatchedKeysMap.get(prQ.getFullPath());
      if (bucketIdToDispatchedKeys == null) {
        bucketIdToDispatchedKeys = new ConcurrentHashMap();
        regionToDispatchedKeysMap.put(prQ.getFullPath(), bucketIdToDispatchedKeys);
      }
      addRemovedEventsToMap(bucketIdToDispatchedKeys, bucketId, shadowKeys);
      if (wasEmpty) {
        regionToDispatchedKeysMapEmpty.signal();        
      }
    }
    finally {
      buckToDispatchLock.unlock();
    }
  }

  protected void addRemovedEvents(String prQPath, int bucketId,
      List<Object> shadowKeys) {
    buckToDispatchLock.lock();
    boolean wasEmpty = regionToDispatchedKeysMap.isEmpty();
    try {
      Map bucketIdToDispatchedKeys = (Map)regionToDispatchedKeysMap.get(prQPath);
      if (bucketIdToDispatchedKeys == null) {
        bucketIdToDispatchedKeys = new ConcurrentHashMap();
        regionToDispatchedKeysMap.put(prQPath, bucketIdToDispatchedKeys);
      }
      addRemovedEventsToMap(bucketIdToDispatchedKeys, bucketId, shadowKeys);
      if (wasEmpty) {
        regionToDispatchedKeysMapEmpty.signal();        
      }
    }
    finally {
      buckToDispatchLock.unlock();
    }
  }
  
  private void addRemovedEventsToMap(Map bucketIdToDispatchedKeys,
      int bucketId, List keys) {
    List dispatchedKeys = (List)bucketIdToDispatchedKeys.get(bucketId);
    if (dispatchedKeys == null) {
      dispatchedKeys = keys == null ? new ArrayList<Object>() : keys;
    } else {
      dispatchedKeys.addAll(keys);
    }
    bucketIdToDispatchedKeys.put(bucketId, dispatchedKeys);
  }
  
  public List peek(int batchSize) throws InterruptedException, CacheException {
    throw new UnsupportedOperationException();
  }

  public List peek(int batchSize, int timeToWait) throws InterruptedException,
      CacheException {
    PartitionedRegion prQ = getRandomShadowPR();
    List batch = new ArrayList();
    if (prQ == null || prQ.getLocalMaxMemory() == 0) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      blockProcesorThreadIfRequired();
      return batch;
    }
    
    long start = System.currentTimeMillis();
    long end = start + timeToWait;

    if (this.resetLastPeeked) {
      batch.addAll(peekedEvents);
      this.resetLastPeeked = false;
      if (this.logger.fineEnabled()) {
        StringBuilder buffer = new StringBuilder();
        for (GatewaySenderEventImpl ge : peekedEvents) {
          buffer.append("event :");
          buffer.append(ge);
        }
        this.logger.fine("Adding already peeked events to the batch "
            + buffer.toString());
      }
    }
    
    int bId = -1;
    while (batch.size() < batchSize) {
      if (areLocalBucketQueueRegionsPresent()
          && ((bId = getRandomPrimaryBucket(prQ)) != -1)) {
        GatewaySenderEventImpl object = (GatewaySenderEventImpl) peekAhead(prQ, bId);
        if (object != null) {
          GatewaySenderEventImpl copy = object.makeHeapCopyIfOffHeap();
          if (copy == null) {
            continue;
          }
          object = copy;
        }
        // Conflate here
        if (object != null) {
          if (logger.fineEnabled()) {
            logger.fine("The gatewayEventImpl in peek is " + object);
          }
          batch.add(object);
          peekedEvents.add(object);
          BucketRegionQueue brq = ((BucketRegionQueue)prQ
              .getDataStore().getLocalBucketById(bId));
          
          //brq.doLockForPrimary(false);
          
        } else {
          // If time to wait is -1 (don't wait) or time interval has elapsed
          long currentTime = System.currentTimeMillis();
          if (logger.fineEnabled()) {
            logger.fine(this + ": Peeked object was null. Peek current time: " + currentTime);
          }
          if (timeToWait == -1 || (end <= currentTime)) {
            if (logger.fineEnabled()) {
              logger.fine(this + ": Peeked object was null.. Peek breaking");
            }
            break;
          }
          if (logger.fineEnabled()) {
            logger.fine(this + ": Peeked object was null. Peek continuing");
          }
          // Sleep a bit before trying again.
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          continue;
        }
      } else {
        // If time to wait is -1 (don't wait) or time interval has elapsed
        long currentTime = System.currentTimeMillis();
        if (logger.fineEnabled()) {
          logger.fine(this + ": Peek current time: " + currentTime);
        }
        if (timeToWait == -1 || (end <= currentTime)) {
          if (logger.fineEnabled()) {
            logger.fine(this + ": Peek breaking");
          }
          break;
        }
        if (logger.fineEnabled()) {
          logger.fine(this + ": Peek continuing");
        }
        // Sleep a bit before trying again.
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        continue;
      }
    }
    if (logger.fineEnabled()) {
      logger.fine(this + ": Peeked a batch of " + batch.size() + " entries"
          + " the size of queue is " + size() + "localSize is : " + localSize());
    }
    if (batch.size() == 0) {
      blockProcesorThreadIfRequired();
    }
    return batch;
  }

  /**
   * Should take care of scenario where some one wants to pause/stop the processor.
   * So the while condition should also have paused/stop check.
   * pause/stop should notify this thread.
   * Also no need to wait indefinitely in while loop. We just wait for 1 sec,
   * and if not the again let the processor try to peek check for pause/stop/queueEmpty
   * and come back.
   * This should avoid threads being blocked for pause/stop when processor
   * is waiting here and also avoid the hot loop.
   */
  protected void blockProcesorThreadIfRequired() throws InterruptedException {
    queueEmptyLock.lock();
    try {
      //while (isQueueEmpty) {
      if(isQueueEmpty) {
        if (getLogger().finerEnabled()) {
          getLogger().finer("Going to wait, till notified.");
        }
        queueEmptyCondition.await(1000,TimeUnit.MILLISECONDS);
        //isQueueEmpty = this.localSize() == 0;
      }
      // update the flag so that next time when we come we will block.
      isQueueEmpty = this.localSize() == 0;
    } finally {
      if (getLogger().finerEnabled()) {
        getLogger().finer("Going to unblock. isQueueEmpty " + isQueueEmpty);
      }
      queueEmptyLock.unlock();
    }
  }

  private boolean areLocalBucketQueueRegionsPresent() {
    boolean bucketsAvailable = false;
    for (PartitionedRegion prQ : this.userRegionNameToshadowPRMap.values()) {
      if (prQ.getDataStore().getAllLocalBucketRegions().size() > 0)
        return true;
    }
    return false;
  }


  protected Object peekAhead(PartitionedRegion prQ, int bucketId) throws CacheException {
    Object object = null;
    BucketRegionQueue brq = ((BucketRegionQueue)prQ
        .getDataStore().getLocalBucketById(bucketId));

    if (logger.finerEnabled()) {
      logger.finer(this + ": Peekahead for the bucket " + bucketId);
    }
    try {
      object = brq.peek();
    } catch (BucketRegionQueueUnavailableException e) {
      //BucketRegionQueue unavailable. Can be due to the BucketRegionQueue being destroyed.
      return object;//this will be null
    }
    if (logger.fineEnabled()  &&  object != null) {
      logger.fine(this + ": Peeked object from bucket " + bucketId
          + " object: " + object);
    }

    if (object == null) {
      if (this.stats != null) {
        this.stats.incEventsNotQueuedConflated();
      }
    }
    return object; // OFFHEAP: ok since callers are careful to do destroys on region queue after finished with peeked object.
  }
  
  
  public int localSize() {
    int size = 0;
    for (PartitionedRegion prQ : this.userRegionNameToshadowPRMap.values()) {
      if(((PartitionedRegion)prQ.getRegion()).getDataStore() != null) {
        size += ((PartitionedRegion)prQ.getRegion()).getDataStore()
            .getSizeOfLocalPrimaryBuckets();  
      }
      if (logger.fineEnabled()  &&  size > 0) {
        logger.fine("The name of the queue region is " + prQ.getFullPath()
            + " and the size is " + size);
      }
    }
    return size /*+ sender.getTmpQueuedEventSize()*/;
  }
  
  @Override
  public int size() {
    int size = 0;
    for (PartitionedRegion prQ : this.userRegionNameToshadowPRMap.values()) {
      if (logger.finerEnabled()) {
        logger.finer("The name of the queue region is " + prQ.getName()
            + " and the size is " + prQ.size() + " keyset size is "
            + prQ.keys().size());
      }
      size += prQ.size();
    }
    
    return size + sender.getTmpQueuedEventSize();
  }
  
  @Override
  public void addCacheListener(CacheListener listener) {
    for(PartitionedRegion prQ: this.userRegionNameToshadowPRMap.values()) {
      AttributesMutator mutator = prQ.getAttributesMutator();
      mutator.addCacheListener(listener);
    }
  }

  @Override
  public void removeCacheListener() {
    throw new UnsupportedOperationException();
  }


  @Override
  public void remove(int batchSize) throws CacheException {
    for (int i = 0; i < batchSize; i++) {
      remove();
    }
  }
  
  public void conflateEvent(Conflatable conflatableObject, int bucketId, Long tailKey) {
    ConflationHandler conflationHandler = new ConflationHandler(
        conflatableObject, bucketId, tailKey);
    conflationExecutor.execute(conflationHandler);
  }
  
  public long getNumEntriesOverflowOnDiskTestOnly() {
    long numEntriesOnDisk = 0;
    for(PartitionedRegion prQ: this.userRegionNameToshadowPRMap.values()) {
      DiskRegionStats diskStats = prQ.getDiskRegionStats();
      if (diskStats == null) {
        if (logger.fineEnabled()) {
          logger
              .fine(this
                  + "DiskRegionStats for shadow PR is null. Returning the numEntriesOverflowOnDisk as 0");
        }
        return 0;
      }
      if (logger.fineEnabled()) {
        logger
            .fine(this
                + "DiskRegionStats for shadow PR is NOT null. Returning the numEntriesOverflowOnDisk obtained from DiskRegionStats");
      }
      numEntriesOnDisk += diskStats.getNumOverflowOnDisk();  
    }
    return numEntriesOnDisk;
  }
  
  public long getNumEntriesInVMTestOnly() {
    long numEntriesInVM = 0;
    for (PartitionedRegion prQ : this.userRegionNameToshadowPRMap.values()) {
      DiskRegionStats diskStats = prQ.getDiskRegionStats();
      if (diskStats == null) {
        if (logger.fineEnabled()) {
          logger
              .fine(this
                  + "DiskRegionStats for shadow PR is null. Returning the numEntriesInVM as 0");
        }
        return 0;
      }
      if (logger.fineEnabled()) {
        logger
            .fine(this
                + "DiskRegionStats for shadow PR is NOT null. Returning the numEntriesInVM obtained from DiskRegionStats");
      }
      numEntriesInVM += diskStats.getNumEntriesInVM();
    }
    return numEntriesInVM;
  }
  
  /**
   * This method does the cleanup of any threads, sockets, connection that are held up
   * by the queue. Note that this cleanup doesn't clean the data held by the queue.
   */
  public void cleanUp() {
    if (this.removalThread != null) {
      this.removalThread.shutdown();
    }
    if (conflationExecutor != null) {
      cleanupConflationThreadPool();
    }
  }
  
  @Override
  public void close() {
    // Because of bug 49060 do not close the regions of a parallel queue
//    for (Region r: getRegions()) {
//      if (r != null && !r.isDestroyed()) {
//        try {
//          r.close();
//        } catch (RegionDestroyedException e) {
//        }
//      }
//    }
  }

  /**
   * @return the bucketToTempQueueMap
   */
  public Map<Integer, BlockingQueue<GatewaySenderEventImpl>> getBucketToTempQueueMap() {
    return this.bucketToTempQueueMap;
  }
  
  //TODO:REF: Name for this class should be appropriate?
  private class BatchRemovalThread extends Thread {
    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final GemFireCacheImpl cache;

    /**
     * Constructor : Creates and initializes the thread
     */
    public BatchRemovalThread(GemFireCacheImpl c) {
      super("BatchRemovalThread");
      //TODO:REF: Name for this thread ?
      this.setDaemon(true);
      this.cache = c;
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }
      if (cache.getCancelCriterion().cancelInProgress() != null) {
        return true;
      }
      return false;
    }

    @Override
    public void run() {
      try {
        InternalDistributedSystem ids = cache.getDistributedSystem();
        DM dm = ids.getDistributionManager();
        for (;;) {
          try { // be somewhat tolerant of failures
            if (checkCancelled()) {
              break;
            }

            // TODO : make the thread running time configurable
            boolean interrupted = Thread.interrupted();
            try {
              synchronized (this) {
                this.wait(messageSyncInterval);
              }
            } catch (InterruptedException e) {
              interrupted = true;
              if (checkCancelled()) {
                break;
              }
              break; // desperation...we must be trying to shut
              // down...?
            } finally {
              // Not particularly important since we're exiting
              // the thread,
              // but following the pattern is still good
              // practice...
              if (interrupted)
                Thread.currentThread().interrupt();
            }

            if (logger.fineEnabled()) {
              buckToDispatchLock.lock();
              try {
                logger
                    .fine("BatchRemovalThread about to query the batch removal map "
                        + regionToDispatchedKeysMap);
              }
              finally {
                buckToDispatchLock.unlock();  
              }
            }

            final HashMap temp = new HashMap();
            buckToDispatchLock.lock();
            try {
              boolean wasEmpty = regionToDispatchedKeysMap.isEmpty();
              while (regionToDispatchedKeysMap.isEmpty()) {
                regionToDispatchedKeysMapEmpty.await();
              }
              if (wasEmpty) continue;
              // TODO: This should be optimized.
              temp.putAll(regionToDispatchedKeysMap);
              regionToDispatchedKeysMap.clear();
            }
            finally {
              buckToDispatchLock.unlock();
            }
            // Get all the data-stores wherever userPRs are present
            Set recipients = getAllRecipients(cache, temp);
            ParallelQueueRemovalMessage pqrm = new ParallelQueueRemovalMessage(temp);
            pqrm.setRecipients(recipients);
            dm.putOutgoing(pqrm);
          } // be somewhat tolerant of failures
          catch (CancelException e) {
            if (logger.fineEnabled()) {
              logger.fine("BatchRemovalThread is exiting due to cancellation");
            }
            break;
          } catch (Throwable t) {
            Error err;
            if (t instanceof Error
                && SystemFailure.isJVMFailureError(err = (Error)t)) {
              SystemFailure.initiateFailure(err);
              // If this ever returns, rethrow the error. We're
              // poisoned now, so don't let this thread continue.
              throw err;
            }
            // Whenever you catch Error or Throwable, you must also
            // check for fatal JVM error (see above). However, there
            // is _still_ a possibility that you are dealing with a
            // cascading error condition, so you also need to check to see if
            // the JVM is still usable:
            SystemFailure.checkFailure();
            if (checkCancelled()) {
              break;
            }
            if (logger.fineEnabled()) {
              logger.fine("BatchRemovalThread: ignoring exception", t);
            }
          }
        } // for
      } // ensure exit message is printed
      catch (CancelException e) {
        if (logger.fineEnabled()) {
          logger.fine("BatchRemovalThread exiting due to cancellation: " + e);
        }
      } finally {
        logger
            .info(LocalizedStrings.HARegionQueue_THE_QUEUEREMOVALTHREAD_IS_DONE);
      }
    }

    private Set<InternalDistributedMember> getAllRecipients(
        GemFireCacheImpl cache, Map map) {
      Set recipients = new THashSet();
      for (Object pr : map.keySet()) {
        recipients.addAll(((PartitionedRegion)(cache.getRegion((String)pr)))
            .getRegionAdvisor().adviseDataStore());
      }
      return recipients;
    }

    /**
     * shutdown this thread and the caller thread will join this thread
     */
    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
      boolean interrupted = Thread.interrupted();
      try {
        this.join(15 * 1000);
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (this.isAlive()) {
        logger
            .warning(LocalizedStrings.HARegionQueue_QUEUEREMOVALTHREAD_IGNORED_CANCELLATION);
      }
    }
  }

  protected class ParallelGatewaySenderQueueMetaRegion extends
      PartitionedRegion {
    
    ParallelGatewaySenderImpl sender = null;
    public ParallelGatewaySenderQueueMetaRegion(String regionName,
        RegionAttributes attrs, LocalRegion parentRegion,
        GemFireCacheImpl cache, AbstractGatewaySender pgSender) {
      this( regionName, attrs, parentRegion, cache, pgSender, false);
    }
    public ParallelGatewaySenderQueueMetaRegion(String regionName,
        RegionAttributes attrs, LocalRegion parentRegion,
        GemFireCacheImpl cache, AbstractGatewaySender pgSender, boolean isUsedForHDFS) {
      super(regionName, attrs, parentRegion, cache,
          new InternalRegionArguments().setDestroyLockFlag(true)
              .setRecreateFlag(false).setSnapshotInputStream(null)
              .setImageTarget(null)
              .setIsUsedForParallelGatewaySenderQueue(true)
              .setParallelGatewaySender((ParallelGatewaySenderImpl)pgSender)
              .setIsUsedForHDFSParallelGatewaySenderQueue(isUsedForHDFS)
              .setUUID(pgSender.getUUID()));
      this.sender = (ParallelGatewaySenderImpl)pgSender;
      
    }

    @Override
    protected boolean isCopyOnRead() {
      return false;
    }

    // Prevent this region from participating in a TX, bug 38709
    @Override
    final public boolean isSecret() {
      return true;
    }
    
    //Prevent this region from using concurrency checks
    @Override
    final public boolean supportsConcurrencyChecks() {
      return false;
    }

    @Override
    final protected boolean shouldNotifyBridgeClients() {
      return false;
    }

    @Override
    final public boolean generateEventID() {
      return false;
    }
    
    final public boolean isUsedForParallelGatewaySenderQueue() {
      return true;
    }
    
    @Override
    final public ParallelGatewaySenderImpl getParallelGatewaySender(){
      return this.sender;
    }
  }

  public LogWriterI18n getLogger() {
    return this.logger;
  }
  
  public long estimateMemoryFootprint(SingleObjectSizer sizer) {
    return sizer.sizeof(this) + sizer.sizeof(regionToDispatchedKeysMap)
        + sizer.sizeof(userRegionNameToshadowPRMap)
        + sizer.sizeof(bucketToTempQueueMap) + sizer.sizeof(peekedEvents)
        + sizer.sizeof(conflationExecutor);
  }
  
  public void clear(PartitionedRegion pr, int bucketId) {
  	throw new RuntimeException("This method(clear)is not supported by ParallelGatewaySenderQueue");
  }
  
  public int size(PartitionedRegion pr, int bucketId) throws ForceReattemptException{
  	throw new RuntimeException("This method(size)is not supported by ParallelGatewaySenderQueue");
  }
}
