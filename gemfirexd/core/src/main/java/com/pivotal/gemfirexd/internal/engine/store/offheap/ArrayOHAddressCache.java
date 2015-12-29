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
import java.util.List;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;

/**
 * Implements OHAddressCache with a  list of ArrayTrackerElement. Each element
 * has a long array of size 512 so the average amount of memory used for each long
 * added to this collection is 8.
 * Keep the code in this class in sync with OffHeapOHAddressCache.
 * The both should have the exact same logic.
 * 
 * @author darrel
 *
 */
public final class ArrayOHAddressCache implements CollectionBasedOHAddressCache {
  private ArrayTrackerElement head = new ArrayTrackerElement(null);

  @Override
  public void release() {
    ArrayTrackerElement e = this.head;
    while (e != null) {
      e.release();
      e = e.down;
    }
    this.head = null;
  }

  @Override
  public void put(long addr) {
    if (this.head == null || this.head.isFull()) {
      this.head = new ArrayTrackerElement(this.head);
    }
    this.head.addRef(addr);
  }

  @Override
  public void releaseByteSource(int idx) {
    if (this.head == null) {
      throw new IndexOutOfBoundsException("list was empty");
    }
    ArrayTrackerElement e = this.head.removeRef(idx);
    if (e.isEmpty()) {
      if (e == this.head) {
        if (e.down != null) {
          this.head = e.down;
          e.down.up = null;
        } else {
          this.head = null;
        }
      } else {
        if (e.down != null) {
          e.down.up = e.up;
        }
        // since e was not the head we know e.up is not null
        e.up.down = e.down;
      }
    }
  }
  
  private static class ArrayTrackerElement {
    private static final long NULL = 0;
    private static final long REMOVED = 1;
    private final long[] refs = new long[512];
    /**
     * The number of refs that have been added or that have been removed by setting the slot to REMOVED.
     * Note this does not get decd until a REMOVED slot is the last one in the element.
     */
    private short addCount = 0;
    /**
     * The number of REMOVED tokens in refs.
     */
    private short removedCount = 0;
    private ArrayTrackerElement down;
    private ArrayTrackerElement up;
    
    public ArrayTrackerElement(ArrayTrackerElement oldHead) {
      this.down = oldHead;
      this.up = null;
      if (oldHead != null) {
        oldHead.up = this;
      }
    }
    public void release() {
      for (int i=0; i < this.addCount; i++) {
        long addr = this.refs[i];
        if (addr != NULL && addr != REMOVED) {
          Chunk.release(addr, true);
        }
      }
    }
    /**
     * Returns the element that the ref was removed from
     */
    public ArrayTrackerElement removeRef(int idx) {
      return removeRef(this, idx);
    }
    /**
     * Returns the element that the ref was removed from
     */
    private static ArrayTrackerElement removeRef(ArrayTrackerElement e, int idx) {
      if (idx == 0) {
        while (e.isEmpty()) {
          if (e.down != null) {
            e = e.down;
          } else {
            throw new IndexOutOfBoundsException("list was empty");
          }
        }
        e.addCount--; // This dec removes the zeroth element
        long addr = e.refs[e.addCount];
        // note that the slot at addCount should never equal REMOVED
        if (addr != NULL) {
          Chunk.release(addr, true);
        }
      } else {
        boolean onLastTarget = false;
        int targetIdx = e.addCount;
        int liveCount = targetIdx - e.removedCount;
        if (liveCount <= idx) {
          idx -= liveCount;
          if (idx == 0) {
            onLastTarget = true;
          }
          targetIdx = 0; // force us down the stack
        }
        boolean doNext = false;
        do {
          targetIdx--;
          while (targetIdx < 0) {
            if (e.down != null) {
              e = e.down;
              targetIdx = e.addCount;
              liveCount = targetIdx - e.removedCount;
              if (liveCount <= idx) {
                idx -= liveCount;
                if (idx == 0) {
                  onLastTarget = true;
                }
                targetIdx = 0; // force us down the stack
              }
              targetIdx--;
            } else {
              throw new IndexOutOfBoundsException("removeRef idx did not exist");
            }
          }
          if (e.refs[targetIdx] != REMOVED) {
            if (onLastTarget) {
              doNext = false;
            } else {
              doNext = true;
              idx--;
              if (idx == 0) {
                onLastTarget = true;
              }
            }
          } else {
            // Since it is a REMOVED token skip it
            doNext = true;
          }
        } while (doNext);
        long addr = e.refs[targetIdx];
        if (addr != NULL) {
          Chunk.release(addr, true);
        }
        // Just set the slot to the REMOVED token
        e.refs[targetIdx] = REMOVED;
        e.removedCount++;
      }
      // clean up any sequence of REMOVED tokens starting at e.addCount-1
      while (e.addCount > 0 && e.refs[e.addCount-1] == REMOVED) {
        e.removedCount--;
        e.addCount--;
      }
      return e;
    }
    
    public void addRef(long refAddr) {
      this.refs[this.addCount] = refAddr;
      this.addCount++;
    }
    public boolean isEmpty() {
      return this.addCount - this.removedCount <= 0;
    }
    public boolean isFull() {
      return this.addCount >= this.refs.length;
    }
    public int size() {
      return this.addCount - this.removedCount;
    }
    public void copyToList(ArrayList<Long> result) {
      for (int i=this.addCount-1; i >= 0; i--) {
        long addr = this.refs[i];
        if (addr == NULL) {
          result.add(null);
        } else if (addr != REMOVED) {
          result.add(addr);
        }
      }
    }
  }

  @Override
  public int testHook_getSize() {
    int result = 0;
    ArrayTrackerElement e = this.head;
    while (e != null) {
      result += e.size();
      e = e.down;
    }
    return result;
  }

  @Override
  public List<Long> testHook_copyToList() {
    ArrayList<Long> result = new ArrayList<Long>(testHook_getSize());
    ArrayTrackerElement e = this.head;
    while (e != null) {
      e.copyToList(result);
      e = e.down;
    }
    return result;
  }
}

