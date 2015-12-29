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

package com.gemstone.gemfire.internal.cache.control;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionRebalanceOp;

/**
 * Implements <code>RebalanceOperation</code> for rebalancing Cache resources.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("synthetic-access")
public class RebalanceOperationImpl implements RebalanceOperation {

  private final boolean simulation;
  private final GemFireCacheImpl cache;
  private Future<RebalanceResults> future;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final Object futureLock = new Object();
  private RegionFilter filter;
  
  RebalanceOperationImpl(GemFireCacheImpl cache, boolean simulation,
      RegionFilter filter) {
    this.simulation = simulation;
    this.cache = cache;
    this.filter = filter;
  }
    
  public void start() {
    final InternalResourceManager manager = this.cache.getResourceManager();
    ScheduledExecutorService ex = manager.getExecutor();
    synchronized (this.futureLock) {
      manager.addInProgressRebalance(this);
      future = ex.submit(new Callable<RebalanceResults>() {
        public RebalanceResults call() {
          SystemFailure.checkFailure();
          cache.getCancelCriterion().checkCancelInProgress(null);
          try {
            return RebalanceOperationImpl.this.call();
          }
          catch (RuntimeException e) {
            cache.getLogger().fine(
                "Unexpected exception in rebalancing", e);
            throw e;
          } finally {
            manager.removeInProgressRebalance(RebalanceOperationImpl.this);
          }
        }
      });
    }
  }
  
  private RebalanceResults call() {
    RebalanceResultsImpl results = new RebalanceResultsImpl();
    ResourceManagerStats stats = cache.getResourceManager().getStats();
    long start = stats.startRebalance();
    try {
    for(PartitionedRegion region: cache.getPartitionedRegions()) {
      if(cancelled.get()) {
        break;
      }
      try {
        //Colocated regions will be rebalanced as part of rebalancing their leader
          if (region.getColocatedWith() == null && filter.include(region)) {
            
            Set<PartitionRebalanceInfo> detailSet = null;
            if (region.isFixedPartitionedRegion()) {
              if (Boolean.getBoolean("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP")) {
                PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                    region, simulation, false, false, true, true, true, cancelled,
                    stats);
                detailSet = prOp.execute();
              } else {
                continue;
              }
            } else {
              PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                  region, simulation, true, true, true, true, true, cancelled,
                  stats);
              detailSet = prOp.execute();
            }
            for (PartitionRebalanceInfo details : detailSet) {
              results.addDetails(details);
            }
          }
      } catch(RegionDestroyedException e) {
        //ignore, go on to the next region
      }
    }
    } finally {
      stats.endRebalance(start);
    }
    return results;
  }
  
  private Future<RebalanceResults> getFuture() {
    synchronized (this.futureLock) {
      return this.future;
    }
  }
  
  public boolean cancel() {
    cancelled.set(true);
    if(getFuture().cancel(false)) {
      cache.getResourceManager().removeInProgressRebalance(this);
    }
    return true;
  }

  public RebalanceResults getResults() throws CancellationException, InterruptedException {
      try {
        return getFuture().get();
      } catch (ExecutionException e) {
        if(e.getCause() instanceof GemFireException) {
          throw (GemFireException) e.getCause();
        } else if(e.getCause() instanceof InternalGemFireError) {
          throw (InternalGemFireError) e.getCause();
        } else {
          throw new InternalGemFireError(e.getCause());
        }
      }
  }

  public RebalanceResults getResults(long timeout, TimeUnit unit)
      throws CancellationException, TimeoutException, InterruptedException {
    try {
      return getFuture().get(timeout, unit);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof GemFireException) {
        throw (GemFireException) e.getCause();
      } else if(e.getCause() instanceof InternalGemFireError) {
        throw (InternalGemFireError) e.getCause();
      } else {
        throw new InternalGemFireError(e.getCause());
      }
    }
  }

  public boolean isCancelled() {
    return this.cancelled.get();
  }

  public boolean isDone() {
    return this.cancelled.get() || getFuture().isDone();
  }
  
  /**
   * Returns true if this is a simulation.
   * 
   * @return true if this is a simulation
   */
  boolean isSimulation() {
    return this.simulation;
  }
  
  private static class RebalanceResultsImpl implements RebalanceResults, Serializable {
    private Set<PartitionRebalanceInfo> detailSet = new TreeSet<PartitionRebalanceInfo>();
    private long totalBucketCreateBytes;
    private long totalBucketCreateTime;
    private int totalBucketCreatesCompleted;
    private long totalBucketTransferBytes;
    private long totalBucketTransferTime;
    private int totalBucketTransfersCompleted;
    private long totalPrimaryTransferTime;
    private int totalPrimaryTransfersCompleted;
    private long totalTime;
    
    public void addDetails(PartitionRebalanceInfo details) {
      this.detailSet.add(details);
      totalBucketCreateBytes += details.getBucketCreateBytes();
      totalBucketCreateTime += details.getBucketCreateTime();
      totalBucketCreatesCompleted += details.getBucketCreatesCompleted();
      totalBucketTransferBytes += details.getBucketTransferBytes();
      totalBucketTransferTime += details.getBucketTransferTime();
      totalBucketTransfersCompleted += details.getBucketTransfersCompleted();
      totalPrimaryTransferTime += details.getPrimaryTransferTime();
      totalPrimaryTransfersCompleted += details.getPrimaryTransfersCompleted();
      totalTime += details.getTime();
    }

    public Set<PartitionRebalanceInfo> getPartitionRebalanceDetails() {
      return detailSet;
    }

    public long getTotalBucketCreateBytes() {
      return this.totalBucketCreateBytes;
    }

    public long getTotalBucketCreateTime() {
      return this.totalBucketCreateTime;
    }

    public int getTotalBucketCreatesCompleted() {
      return this.totalBucketCreatesCompleted;
    }

    public long getTotalBucketTransferBytes() {
      return this.totalBucketTransferBytes;
    }

    public long getTotalBucketTransferTime() {
      return this.totalBucketTransferTime;
    }

    public int getTotalBucketTransfersCompleted() {
      return this.totalBucketTransfersCompleted;
    }

    public long getTotalPrimaryTransferTime() {
      return this.totalPrimaryTransferTime;
    }

    public int getTotalPrimaryTransfersCompleted() {
      return this.totalPrimaryTransfersCompleted;
    }

    public long getTotalTime() {
      return this.totalTime;
    }
  }
}
