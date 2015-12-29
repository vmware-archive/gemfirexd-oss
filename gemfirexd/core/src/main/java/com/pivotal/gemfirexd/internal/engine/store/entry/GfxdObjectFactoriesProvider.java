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

package com.pivotal.gemfirexd.internal.engine.store.entry;

import com.gemstone.gemfire.internal.HostStatSampler.StatsSamplerCallback;
import com.gemstone.gemfire.internal.cache.AbstractRegionEntry;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl.StaticSystemCallbacks;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.internal.cache.RegionEntryFactory;
import com.gemstone.gemfire.internal.cache.TXEntryStateFactory;
import com.gemstone.gemfire.internal.cache.TXStateProxyFactory;
import com.gemstone.gemfire.internal.concurrent.CustomEntryConcurrentHashMap;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkFactory;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.GemFireChunk;
import com.pivotal.gemfirexd.internal.engine.access.GfxdTXStateProxy;
import com.pivotal.gemfirexd.internal.engine.distributed.message.ProjectionRow.ProjectionRowFactory;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.RegionEntryUtils;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapDelta;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapDeltas;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRow;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRowWithLobs;

/**
 * 
 * @author Asif, swale
 */
public final class GfxdObjectFactoriesProvider {

  public static final CustomEntryConcurrentHashMap.HashEntryCreator<Object,
      Object> getHashEntryCreator() {

    return new AbstractRegionEntry.HashRegionEntryCreator() {

      /**
       * @see CustomEntryConcurrentHashMap.HashEntryCreator#keyHashCode(Object,
       *      boolean)
       */
      @Override
      public final int keyHashCode(final Object key, boolean compareValues) {
        // This override ensures that the hash stored as the HashEntry's hash
        // matches that stored when the same RegionEntry is used as the key
        // (or compares against the hash of CCRK).
        // Distribution of hashes should not be a problem since we already
        // ensure that for the serialized hashes, while DVDs have all proper
        // hash implementations.
        // Alternative would be to maintain two different hash fields but
        // that is a per entry overhead.
        return key.hashCode();
      }
    };
  }
  
  public static ChunkFactory getChunkFactory() {
    final ChunkType[] ordinalToChunkType = new ChunkType[] {
      OffHeapRow.TYPE, // SRC_TYPE_NO_LOB_NO_DELTA
      OffHeapRowWithLobs.TYPE, // SRC_TYPE_WITH_LOBS
      OffHeapDelta.TYPE, // SRC_TYPE_WITH_SINGLE_DELTA
      OffHeapDeltas.TYPE, // SRC_TYPE_WITH_MULTIPLE_DELTAS
      //OffHeapLob.TYPE, // SRC_TYPE_IS_LOB
      GemFireChunk.TYPE // SRC_TYPE_GFE
    };

    return new ChunkFactory() {
      @Override
      public Chunk newChunk(long address) {
        return getChunkTypeForAddress(address).newChunk(address);
      }
      
      @Override
      public Chunk newChunk(long address, int chunkSize, ChunkType chunkType) {
        return chunkType.newChunk(address, chunkSize);
      }

      @Override
      public Chunk newChunk(long address, ChunkType chunkType) {
        return chunkType.newChunk(address);
      }

      @Override
      public ChunkType getChunkTypeForAddress(long address) {
        return ordinalToChunkType[Chunk.getSrcTypeOrdinal(address)];
      }

      @Override
      public ChunkType getChunkTypeForRawBits(int bits) {
        return ordinalToChunkType[Chunk.getSrcTypeOrdinalFromRawBits(bits)];
      }
    };
  }

  public static RegionEntryFactory getRegionEntryFactory(boolean statsEnabled,
      boolean isLRU, boolean isDisk, boolean withVersioning, Object owner,
      InternalRegionArguments internalArgs) {
    RegionEntryFactory factory = null;
    GemFireContainer container = null;
    boolean offheapEnabled = false;
    // Try getting Container from Internal Args
    if (internalArgs != null) {
      container = (GemFireContainer)internalArgs.getUserAttribute();
    }
    // If container is null try getting from UserAttribute
    if (owner instanceof LocalRegion) {
      if (container == null) {
        container = (GemFireContainer)((LocalRegion)owner).getUserAttribute();
      }
      offheapEnabled = ((LocalRegion)owner).getEnableOffHeapMemory();
    }

    boolean bucketEntryNeeded = false;
    if (container != null) {
      bucketEntryNeeded = (!container.isGlobalIndex() && container
          .isPartitioned());
    }
    else if (owner instanceof PlaceHolderDiskRegion) {
      // Only application tables can be PR with disk persistence & so we need to
      // check for redundancy or overflow
      PlaceHolderDiskRegion phd = (PlaceHolderDiskRegion)owner;
      offheapEnabled = phd.getEnableOffHeapMemory();
      if (phd.isBucket()) {
        // TODO: SW: this will cause global index regions to be created with
        // bucket disk entries on recovery increasing its overhead!
        bucketEntryNeeded = true;
      }
    }

    if (statsEnabled) {
      if (isLRU) {
        if (isDisk) {
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsDiskLRURegionEntryOffHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationStatsDiskLRURegionEntryOffHeap
                      .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsDiskLRURegionEntryOffHeap
                  .getEntryFactory()
                  : VMLocalRowLocationStatsDiskLRURegionEntryOffHeap
                      .getEntryFactory();
            }
          }
          else { // offheapEnabled false
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsDiskLRURegionEntryHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationStatsDiskLRURegionEntryHeap
                      .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsDiskLRURegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationStatsDiskLRURegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
        else { // isDisk false
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsLRURegionEntryOffHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationStatsLRURegionEntryOffHeap
                      .getEntryFactory();
              
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsLRURegionEntryOffHeap
                  .getEntryFactory()
                  : VMLocalRowLocationStatsLRURegionEntryOffHeap
                      .getEntryFactory();
            }
          }
          else {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsLRURegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationStatsLRURegionEntryHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsLRURegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationStatsLRURegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
      }
      else { // isLRU false
        if (isDisk) {
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsDiskRegionEntryOffHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationStatsDiskRegionEntryOffHeap
                      .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsDiskRegionEntryOffHeap
                  .getEntryFactory()
                  : VMLocalRowLocationStatsDiskRegionEntryOffHeap
                      .getEntryFactory();
            }
          }
          else { // offheapEnabled false
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsDiskRegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationStatsDiskRegionEntryHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsDiskRegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationStatsDiskRegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
        else { // isDisk false
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsRegionEntryOffHeap
                  .getEntryFactory() : VersionedLocalRowLocationStatsRegionEntryOffHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsRegionEntryOffHeap
                  .getEntryFactory() : VMLocalRowLocationStatsRegionEntryOffHeap
                  .getEntryFactory();
            }
          }
          else {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationStatsRegionEntryHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationStatsRegionEntryHeap.getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationStatsRegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationStatsRegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
      }
    }
    else {
      if (isLRU) {
        if (isDisk) { 
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinDiskLRURegionEntryOffHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinDiskLRURegionEntryOffHeap
                  .getEntryFactory();
              
            } else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinDiskLRURegionEntryOffHeap
                  .getEntryFactory() : VMLocalRowLocationThinDiskLRURegionEntryOffHeap
                  .getEntryFactory();
            }
          } else { //offheapEnabled false
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinDiskLRURegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinDiskLRURegionEntryHeap
                  .getEntryFactory();
            } else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinDiskLRURegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationThinDiskLRURegionEntryHeap
                  .getEntryFactory();
            }
          }
          
      } // isDisk false
        else {
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinLRURegionEntryOffHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinLRURegionEntryOffHeap
                  .getEntryFactory();
            } else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinLRURegionEntryOffHeap
                  .getEntryFactory() : VMLocalRowLocationThinLRURegionEntryOffHeap
                  .getEntryFactory();
            }
          } else {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinLRURegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinLRURegionEntryHeap
                  .getEntryFactory();
            } else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinLRURegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationThinLRURegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
      }
      else { // isLRU false
        if (isDisk) {
          if (offheapEnabled) {
            if (withVersioning) {           
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinDiskRegionEntryOffHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinDiskRegionEntryOffHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinDiskRegionEntryOffHeap
                  .getEntryFactory() : VMLocalRowLocationThinDiskRegionEntryOffHeap
                  .getEntryFactory();
            }
          }
          else { // offheapEnabled false
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinDiskRegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinDiskRegionEntryHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinDiskRegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationThinDiskRegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
        else { // isDisk false
          if (offheapEnabled) {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinRegionEntryOffHeap
                  .getEntryFactory()
                  : VersionedLocalRowLocationThinRegionEntryOffHeap.getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinRegionEntryOffHeap
                  .getEntryFactory()
                  : VMLocalRowLocationThinRegionEntryOffHeap.getEntryFactory();
            }
          }
          else {
            if (withVersioning) {
              factory = bucketEntryNeeded ? VersionedBucketRowLocationThinRegionEntryHeap
                  .getEntryFactory() : VersionedLocalRowLocationThinRegionEntryHeap
                  .getEntryFactory();
            }
            else {
              factory = bucketEntryNeeded ? VMBucketRowLocationThinRegionEntryHeap
                  .getEntryFactory() : VMLocalRowLocationThinRegionEntryHeap
                  .getEntryFactory();
            }
          }
        }
      }
    }
    return factory;
  }

  public static StaticSystemCallbacks getSystemCallbacksImpl() {
    return RegionEntryUtils.gfxdSystemCallbacks;
  }

  public static TXStateProxyFactory getTXStateProxyFactory() {
    return GfxdTXStateProxy.getGfxdFactory();
  }

  public static TXEntryStateFactory getTXEntryStateFactory() {
    return GfxdTXEntryState.getGfxdFactory();
  }

  public static ProjectionRowFactory getRawValueFactory() {
    return ProjectionRowFactory.getFactory();
  }

  public static StatsSamplerCallback getStatsSamplerCallbackImpl() {
    return GemFireStore.GfxdStatisticsSampleCollector.getInstance();
  }
  
  // to force inclusion by classlister
  public static void dummy() {
  }
}
