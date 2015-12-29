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
package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.MemoryAllocator;

/**
 * Triggers centralized eviction(asynchronously) when the ResourceManager sends
 * an eviction event for off-heap regions. This is registered with the ResourceManager.
 *
 * @author rholmes
 * @since 7.5
 */
public class OffHeapEvictor extends HeapEvictor {
  private static final String EVICTOR_THREAD_GROUP_NAME = "OffHeapEvictorThreadGroup";
  
  private static final String EVICTOR_THREAD_NAME = "OffHeapEvictorThread";
  
  private long bytesToEvictWithEachBurst;
  
  public OffHeapEvictor(Cache gemFireCache) {
    super(gemFireCache);    
    calculateEvictionBurst();
  }

  private void calculateEvictionBurst() {
    float evictionBurstPercentage = Float.parseFloat(System.getProperty(ResourceManager.EVICTION_BURST_PERCENT_PROP, "0.4"));
    
    MemoryAllocator allocator = ((GemFireCacheImpl) this.cache).getOffHeapStore();
    
    /*
     * Bail if there is no off-heap memory to evict.
     */
    if(null == allocator) {
      throw new IllegalStateException(LocalizedStrings.MEMSCALE_EVICTION_INIT_FAIL.toLocalizedString());
    }
    
    bytesToEvictWithEachBurst = (long)(allocator.getTotalMemory() * 0.01 * evictionBurstPercentage);       
  }
  
  protected int getEvictionLoopDelayTime() {
    if (numEvictionLoopsCompleted < Math.max(3, numFastLoops)) {
      return 250;
    }
    
    return 1000;
  }
  
  protected boolean includePartitionedRegion(PartitionedRegion region) {
    return (region.getEvictionAttributes().getAlgorithm().isLRUHeap() 
        && (region.getDataStore() != null) 
        && region.getAttributes().getEnableOffHeapMemory());
  }
  
  protected boolean includeLocalRegion(LocalRegion region) {
    return (region.getEvictionAttributes().getAlgorithm().isLRUHeap() 
        && region.getAttributes().getEnableOffHeapMemory());
  }
  
  protected String getEvictorThreadGroupName() {
    return OffHeapEvictor.EVICTOR_THREAD_GROUP_NAME;
  }
  
  protected String getEvictorThreadName() {
    return OffHeapEvictor.EVICTOR_THREAD_NAME;
  }

  public long getTotalBytesToEvict() {
    return bytesToEvictWithEachBurst;
  }

  @Override
  protected ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }  
}