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
package com.pivotal.gemfirexd.internal.engine.store.offheap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;

/**
 * An unbounded address cache. As this can be used by BulkTableScanResultset for
 * adding batch of retained addresses, so to make the removal of an address
 * possible, the elements are added at the start of the list instead of end. So
 * to release a byte source at a given position, its position is given relative
 * to the end of the batch.
 * 
 * @author asifs
 * 
 */
public final class LinkedListOHAddressCache extends LinkedList<Long> implements
    CollectionBasedOHAddressCache {
  /**
   * 
   */
  private static final long serialVersionUID = 787656788765l;

  @Override
  public void put(long address) {
    if (address != 0) {
      this.add(0, Long.valueOf(address));
    } else {
      // TODO:ASIF: Identify a cleaner way to avoid these blanks
      this.add(0, null);
    }
  }

  @Override
  public void releaseByteSource(int positionFromEnd) {

    Long address = this.remove(positionFromEnd);
    if (address != null) {
      Chunk.release(address.longValue(), true);
    }

  }

  @Override
  public void release() {
    Iterator<Long> iter = this.iterator();
    while (iter.hasNext()) {
      Long address = iter.next();
      if (address != null) {
        Chunk.release(address.longValue(), true);
      }
    }
    this.clear();
  }

  @Override
  public int testHook_getSize() {
    return size();
  }

  @Override
  public List<Long> testHook_copyToList() {
    return new ArrayList<Long>(this);
  }
}
