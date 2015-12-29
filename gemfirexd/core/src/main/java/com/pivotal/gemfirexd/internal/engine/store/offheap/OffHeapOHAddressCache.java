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

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;

/**
 * Implementation of OHAddressCache that uses off-heap memory to store the addresses.
 * A linked list of off-heap blocks (whose size is BLOCK_SIZE+8) is used to store the addresses
 * so the average amount of off-heap memory used for each address is 8 bytes.
 * Keep the code in this class in sync with OffHeapOHAddressCache.
 * The both should have the exact same logic.
 * <p>
 * This data structure is basically a stack. Adds are always done at the top. Removes
 * can be done at the top (idx 0) or at any other index. If a remove is done at an index
 * that is greater than 0 then a REMOVED token is placed in the slot in memory that contained
 * the removed address. This allows the remove to not need to memcpy down all the items above the remove. Once all
 * the items at the end of a block are REMOVED tokens they are all removed.
 * A block in this class corresponds to ArrayOHAddressCache.ArrayTrackerElement.
 * Each block is a Chunk of OffHeapMemory. Any time the block becomes emtpy it is released back to the off-heap memory manager.
 * <p>A good reason for using this impl instead of ArrayOHAddressCache is that the memory allocated
 * by this collection can live for a while so the heap based collection can get promoted to the old gen
 * in which case it will not be gced until a more expensive gc is done.
 * 
 * @author darrel
 *
 */
public final class OffHeapOHAddressCache implements CollectionBasedOHAddressCache {

  private long head = NULL;

  private static final long NULL = 0;
  private static final long REMOVED = 1;
  private static final int ADD_COUNT_OFFSET = Chunk.OFF_HEAP_HEADER_SIZE; // int (could be short)
  private static final int REMOVE_COUNT_OFFSET = ADD_COUNT_OFFSET+4; // int (could be short)
  private static final int DOWN_OFFSET = REMOVE_COUNT_OFFSET+4; // long
  private static final int UP_OFFSET = DOWN_OFFSET+8; // long
  private static final int REFS_OFFSET = UP_OFFSET+8; // long[]
  private static final int BLOCK_SIZE = 4096-8;
  private static final int REFS_LENGTH = (BLOCK_SIZE - REFS_OFFSET) / 8; // long[].length
  
  private static long allocateBlock(long oldHead) {
    Chunk chunk = (Chunk) SimpleMemoryAllocatorImpl.getAllocator().allocate(BLOCK_SIZE, null);
    long result = chunk.getMemoryAddress();
    setAddCount(result, 0);
    setRemoveCount(result, 0);
    setDown(result, oldHead);
    setUp(result, NULL);
    if (oldHead != NULL) {
      setUp(oldHead, result);
    }
    return result;
  }
  private static void setRefs(long addr, int idx, long v) {
    UnsafeMemoryChunk.writeAbsoluteLong(addr + REFS_OFFSET + (idx * 8L), v);
  }
  private static void setUp(long addr, long v) {
    UnsafeMemoryChunk.writeAbsoluteLong(addr + UP_OFFSET, v);
  }
  private static void setDown(long addr, long v) {
    UnsafeMemoryChunk.writeAbsoluteLong(addr + DOWN_OFFSET, v);
  }
  private static void setRemoveCount(long addr, int v) {
    UnsafeMemoryChunk.writeAbsoluteInt(addr + REMOVE_COUNT_OFFSET, v);
  }
  private static void setAddCount(long addr, int v) {
    UnsafeMemoryChunk.writeAbsoluteInt(addr + ADD_COUNT_OFFSET, v);
  }
  private static long getRefs(long addr, int idx) {
    return UnsafeMemoryChunk.readAbsoluteLong(addr + REFS_OFFSET + (idx * 8L));
  }
  private static long getUp(long addr) {
    return UnsafeMemoryChunk.readAbsoluteLong(addr + UP_OFFSET);
  }
  private static long getDown(long addr) {
    return UnsafeMemoryChunk.readAbsoluteLong(addr + DOWN_OFFSET);
  }
  private static int getRemoveCount(long addr) {
    return UnsafeMemoryChunk.readAbsoluteInt(addr + REMOVE_COUNT_OFFSET);
  }
  private static int getAddCount(long addr) {
    return UnsafeMemoryChunk.readAbsoluteInt(addr + ADD_COUNT_OFFSET);
  }
  @Override
  public void release() {
    long e = this.head;
    while (e != NULL) {
      releaseBlock(e);
      long releaseAddr = e;
      e = getDown(e);
      Chunk.release(releaseAddr, true);
    }
    this.head = NULL;
  }

  private static void releaseBlock(long addr) {
    final int addCount = getAddCount(addr);
    for (int i=0; i < addCount; i++) {
      long ref = getRefs(addr, i);
      if (ref != NULL && ref != REMOVED) {
        Chunk.release(ref, true);
      }
    }
  }
  @Override
  public void put(long addr) {
    if (isBlockFull(this.head)) {
      this.head = allocateBlock(this.head);
    }
    addRefToBlock(this.head, addr);
  }

  private void addRefToBlock(long addr, long refAddr) {
    int addCount = getAddCount(addr);
    setRefs(addr, addCount, refAddr);
    setAddCount(addr, addCount+1);
  }
  
  @Override
  public void releaseByteSource(int idx) {
    long e = removeRefFromBlock(this.head, idx);
    if (isBlockEmpty(e)) {
      if (e == this.head) {
        long eDown = getDown(e);
        if (eDown != NULL) {
          this.head = eDown;
          setUp(eDown, NULL);
        } else {
          this.head = NULL;
        }
        Chunk.release(e, true);
      } else {
        long eDown = getDown(e);
        long eUp = getUp(e);
        if (eDown != NULL) {
          setUp(eDown, eUp);
        }
        // since e was not the head we know eUp is not null
        setDown(eUp, eDown);
        Chunk.release(e, true);
      }
    }
  }
  /**
   * Returns the element that the ref was removed from
   */
  private static long removeRefFromBlock(long e, int idx) {
    if (e == NULL) {
      throw new IndexOutOfBoundsException("list was empty");
    }
    if (idx == 0) {
      while (isBlockEmpty(e)) {
        long eDown = getDown(e);
        if (eDown != NULL) {
          e = eDown;
        } else {
          throw new IndexOutOfBoundsException("list was empty");
        }
      }
      int eAddCount = getAddCount(e)-1;
      setAddCount(e, eAddCount); // This dec removes the zeroth element
      long ref = getRefs(e, eAddCount);
      if (ref != NULL) {
        Chunk.release(ref, true);
      }
    } else {
      boolean onLastTarget = false;
      int targetIdx = getAddCount(e);
      int liveCount = targetIdx - getRemoveCount(e);
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
          long eDown = getDown(e);
          if (eDown != NULL) {
            e = eDown;
            targetIdx = getAddCount(e);
            liveCount = targetIdx - getRemoveCount(e);
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
        if (getRefs(e, targetIdx) != REMOVED) {
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
      long ref = getRefs(e, targetIdx);
      if (ref != NULL) {
        Chunk.release(ref, true);
      }
      // Just set the slot to the REMOVED token
      setRefs(e, targetIdx, REMOVED);
      setRemoveCount(e, getRemoveCount(e)+1);
    }
    { // clean up any sequence of REMOVED tokens starting at e.addCount-1
      int eAddCount = getAddCount(e);
      while (eAddCount > 0 && getRefs(e, eAddCount-1) == REMOVED) {
        setRemoveCount(e, getRemoveCount(e)-1);
        eAddCount--;
        setAddCount(e, eAddCount);
      }
    }
    return e;
  }
  
  private static boolean isBlockEmpty(long addr) {
    return getAddCount(addr) - getRemoveCount(addr) <= 0;
  }
  private static boolean isBlockFull(long addr) {
    return addr == NULL || getAddCount(addr) >= REFS_LENGTH;
  }
  private static int sizeBlock(long addr) {
    return getAddCount(addr) - getRemoveCount(addr);
  }
  private static void copyBlockToList(long addr, ArrayList<Long> list) {
    for (int i=getAddCount(addr)-1; i >= 0; i--) {
      long ref = getRefs(addr, i);
      if (ref == NULL) {
        list.add(null);
      } else if (ref != REMOVED) {
        list.add(ref);
      }
    }
  }
  
  @Override
  public int testHook_getSize() {
    int result = 0;
    long e = this.head;
    while (e != NULL) {
      result += sizeBlock(e);
      e = getDown(e);
    }
    return result;
  }
  @Override
  public List<Long> testHook_copyToList() {
    ArrayList<Long> result = new ArrayList<Long>(testHook_getSize());
    long e = this.head;
    while (e != NULL) {
      copyBlockToList(e, result);
      e = getDown(e);
    }
    return result;
  }
}
