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

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.internal.offheap.MemoryBlock;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.RefCountChangeInfo;

public class OffHeapTestUtil {

  public static void checkOrphans() {
    SimpleMemoryAllocatorImpl allocator = SimpleMemoryAllocatorImpl.getAllocator();
    long end = System.currentTimeMillis() + 30000;
    List<MemoryBlock> orphans = allocator.getOrphans();
    
    //Wait for the orphans to go away (may be in HDFS queue that needs to drain)
    while(orphans != null && !orphans.isEmpty() && System.currentTimeMillis() < end) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      orphans = allocator.getOrphans();
    }
    
    if(orphans != null && ! orphans.isEmpty()) {
      List<RefCountChangeInfo> info = allocator.getRefCountInfo(orphans.get(0).getMemoryAddress());
      System.out.println("FOUND ORPHAN!!");
      System.out.println("Sample orphan: " + orphans.get(0));
      System.out.println("Orpan info: " + info);
    }
    Assert.assertEquals(Collections.emptyList(), orphans);
  }

}
