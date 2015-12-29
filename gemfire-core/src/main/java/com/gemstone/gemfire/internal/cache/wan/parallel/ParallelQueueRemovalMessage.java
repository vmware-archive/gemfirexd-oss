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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.internal.AbstractBucketRegionQueue;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Removes a batch of events from the remote secondary queues  
 * @author Suranjan Kumar
 * @since 7.5
 */

public class ParallelQueueRemovalMessage extends PooledDistributionMessage {

  private HashMap regionToDispatchedKeysMap; 

  public ParallelQueueRemovalMessage() {
  }

  public ParallelQueueRemovalMessage(HashMap rgnToDispatchedKeysMap) {
    this.regionToDispatchedKeysMap = rgnToDispatchedKeysMap;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_REMOVAL_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    final GemFireCacheImpl cache;
    cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      final LogWriterI18n logger = cache.getLoggerI18n();
      int oldLevel = LocalRegion
          .setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        for (Object name : regionToDispatchedKeysMap.keySet()) {
          final String regionName = (String)name;
          final PartitionedRegion region = (PartitionedRegion)cache
              .getRegion(regionName);
          if (region == null) {
            continue;
          }
          else {
            AbstractGatewaySender abstractSender = region.getParallelGatewaySender(); 
            // Find the map: bucketId to dispatchedKeys
            // Find the bucket
            // Destroy the keys
            Map bucketIdToDispatchedKeys = (Map)this.regionToDispatchedKeysMap
                .get(regionName);
            for (Object bId : bucketIdToDispatchedKeys.keySet()) {
              final String bucketFullPath = Region.SEPARATOR
                  + PartitionedRegionHelper.PR_ROOT_REGION_NAME
                  + Region.SEPARATOR + region.getBucketName((Integer)bId);
              AbstractBucketRegionQueue brq = (AbstractBucketRegionQueue)cache
                  .getRegionByPath(bucketFullPath, false);
              if (logger.fineEnabled()) {
                logger
                    .fine("ParallelQueueRemovalMessage : The bucket in the cache is "
                        + " bucketRegionName : "
                        + bucketFullPath
                        + " bucket: "
                        + brq);
              }

              List dispatchedKeys = (List)bucketIdToDispatchedKeys
                  .get((Integer)bId);
              if (dispatchedKeys != null) {
                for (Object key : dispatchedKeys) {
                  //First, clear the Event from tempQueueEvents at AbstractGatewaySender level, if exists
                  //synchronize on AbstractGatewaySender.queuedEventsSync while doing so
                  abstractSender.removeFromTempQueueEvents(key);
                  
                  if (brq != null) {
                    if (brq.isInitialized()) {
                      if (logger.fineEnabled()) {
                        logger.fine("ParallelQueueRemovalMessage : The bucket "
                            + bucketFullPath
                            + " is initialized. Destroying the key " + key
                            + " from BucketRegionQueue.");
                      }
                      destroyKeyFromBucketQueue(brq, key, region);
                    }
                    else {
                      if (logger.fineEnabled()) {
                        logger.fine("ParallelQueueRemovalMessage : The bucket "
                            + bucketFullPath + " is not yet initialized.");
                      }
                      brq.getInitializationLock().readLock().lock();
                      try {
                        if (brq.containsKey(key)) {
                          destroyKeyFromBucketQueue(brq, key, region);
                        }
                        else {
                          // if BucketRegionQueue does not have the key, it
                          // should be in tempQueue
                          // remove it from there..defect #49196
                          destroyFromTempQueue(brq.getPartitionedRegion(),
                              (Integer) bId,
                              key);
                        }
                      }
                      finally {
                        brq.getInitializationLock().readLock().unlock();
                      }
                    }
                  }
                  else {// brq is null. Destroy the event from tempQueue. Defect #49196
                    destroyFromTempQueue(region, (Integer) bId, key);
                  }
                }
              }
            }
          }
        } //for loop regionToDispatchedKeysMap.keySet()
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    } // cache != null
  }

  private void destroyKeyFromBucketQueue(AbstractBucketRegionQueue brq,
      Object key, PartitionedRegion prQ) {
    try {
      brq.destroyKey(key);
      if (prQ.getLogWriterI18n().fineEnabled()) {
        prQ.getLogWriterI18n().fine(
            "Destroyed the key " + key + " for shadowPR " + prQ.getName()
                + " for bucket " + brq.getId());
      }
    } catch (EntryNotFoundException e) {
      if (prQ.getLogWriterI18n().fineEnabled()) {
        prQ.getLogWriterI18n().fine(
            "Got EntryNotFoundException while destroying the key " + key
                + " for bucket " + brq.getId());
      }
      //add the key to failedBatchRemovalMessageQueue. 
      //This is to handle the last scenario in #49196
      brq.addToFailedBatchRemovalMessageKeys(key);
      
    } catch (ForceReattemptException fe) {
      if (prQ.getLogWriterI18n().fineEnabled()) {
        prQ.getLogWriterI18n().fine(
            "Got ForceReattemptException while getting bucket " + brq.getId()
                + " to destroyLocally the keys.");
      }
    } catch (CancelException e) {
      return; // cache or DS is closing
    } catch (CacheException e) {
      if (prQ.getLogWriterI18n().errorEnabled()) {
        prQ.getLogWriterI18n()
            .error(
                LocalizedStrings.ParallelQueueRemovalMessage_QUEUEREMOVALMESSAGEPROCESSEXCEPTION_IN_PROCESSING_THE_LAST_DISPTACHED_KEY_FOR_A_SHADOWPR_THE_PROBLEM_IS_WITH_KEY__0_FOR_SHADOWPR_WITH_NAME_1,
                new Object[] { key, prQ.getName() }, e);
      }
    }
  }
  
  private void destroyFromTempQueue(PartitionedRegion qPR, int bId, Object key) {
    Set queues = qPR.getParallelGatewaySender().getQueues();
    if (queues != null) {
      ConcurrentParallelGatewaySenderQueue prq = (ConcurrentParallelGatewaySenderQueue)queues
          .toArray()[0];
      BlockingQueue<GatewaySenderEventImpl> tempQueue = prq
    		  .getBucketTmpQueue(bId);
      if (tempQueue != null && key instanceof Long) {
        final Long k = (Long)key;
        Iterator<GatewaySenderEventImpl> itr = tempQueue.iterator();
        while (itr.hasNext()) {
          GatewaySenderEventImpl ge = itr.next();
          if (k.equals(ge.getShadowKey())) {
            try {
              itr.remove();
            }finally {
              ge.release();
            }
          }
        }
      }
    }
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeHashMap(this.regionToDispatchedKeysMap, out);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionToDispatchedKeysMap = DataSerializer.readHashMap(in);
  }
}
