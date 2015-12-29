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
// DO NOT modify this class. It was generated from LeafRegionEntry.cpp




import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import com.gemstone.gemfire.internal.concurrent.AtomicUpdaterFactory;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.lru.LRUClockNode;
import com.gemstone.gemfire.internal.concurrent.CustomEntryConcurrentHashMap.HashEntry;
// macros whose definition changes this class:
// disk: DISK
// lru: LRU
// stats: STATS
// versioned: VERSIONED
// offheap: OFFHEAP
// rowlocation: ROWLOCATION
// local: LOCAL
// bucket: BUCKET
// package: PKG
/**
 * Do not modify this class. It was generated.
 * Instead modify LeafRegionEntry.cpp and then run
 * bin/generateRegionEntryClasses.sh from the directory
 * that contains your build.xml.
 */
@SuppressWarnings("serial")
public class VMThinLRURegionEntryHeap extends VMThinLRURegionEntry
{
  public VMThinLRURegionEntryHeap (RegionEntryContext context, Object key,
    Object value
      ) {
    super(context,
          value
        );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinLRURegionEntryHeap> lastModifiedUpdater
    = AtomicUpdaterFactory.newLongFieldUpdater(VMThinLRURegionEntryHeap.class, "lastModified");
  protected long getlastModifiedField() {
    return lastModifiedUpdater.get(this);
  }
  protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
    return lastModifiedUpdater.compareAndSet(this, expectedValue, newValue);
  }
  /**
   * @see HashEntry#getEntryHash()
   */
  @Override
  public final int getEntryHash() {
    return this.hash;
  }
  @Override
  protected void setEntryHash(int v) {
    this.hash = v;
  }
  /**
   * @see HashEntry#getNextEntry()
   */
  @Override
  public final HashEntry<Object, Object> getNextEntry() {
    return this.next;
  }
  /**
   * @see HashEntry#setNextEntry
   */
  @Override
  public final void setNextEntry(final HashEntry<Object, Object> n) {
    this.next = n;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // lru code
  @Override
  public void setDelayedDiskId(LocalRegion r) {
  // nothing needed for LRUs with no disk
  }
  public final synchronized int updateEntrySize(EnableLRU capacityController) {
    return updateEntrySize(capacityController, _getValue()); // OFHEAP: _getValue ok w/o incing refcount because we are synced and only getting the size
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public final synchronized int updateEntrySize(EnableLRU capacityController,
                                                Object value) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize(getRawKey(), value);
  //   GemFireCacheImpl.getInstance().getLoggerI18n().info("DEBUG updateEntrySize: oldSize=" + oldSize
  //                                               + " newSize=" + newSize);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
  //   if ( debug ) log( "updateEntrySize key=" + getRawKey()
  //                     + (_getValue() == Token.INVALID ? " invalid" :
  //                        (_getValue() == Token.LOCAL_INVALID ? "local_invalid" :
  //                         (_getValue()==null ? " evicted" : " valid")))
  //                     + " oldSize=" + oldSize
  //                     + " newSize=" + this.size );
    return delta;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private LRUClockNode nextLRU;
  private LRUClockNode prevLRU;
  //private int refCount;
  private int size;
  public final void setNextLRUNode( LRUClockNode next ) {
    this.nextLRU = next;
  }
  public final LRUClockNode nextLRUNode() {
    return this.nextLRU;
  }
  public final void setPrevLRUNode( LRUClockNode prev ) {
    this.prevLRU = prev;
  }
  public final LRUClockNode prevLRUNode() {
    return this.prevLRU;
  }
  public final int getEntrySize() {
    return this.size;
  }
  protected final void setEntrySize(int size) {
    this.size = size;
  }
  /*
  public final synchronized int getRefCount() {
    return this.refCount;
  }
  public final synchronized void incRefCount() {
    this.refCount++;
    // removal from the LruList is performed as part
    // of the eviction process (getHeadEntry())
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  
  public final synchronized void decRefCount(NewLRUClockHand lruList) {
    if (this.refCount > 0) {
      this.refCount--;
      if (this.refCount == 0) {
        // No more transactions, place in lru list
        lruList.appendEntry(this);
      }
    }
  }
  public final synchronized void resetRefCount(NewLRUClockHand lruList) {
    if (this.refCount > 0) {
      this.refCount = 0;
      lruList.appendEntry(this);
    }
  }
  */
//@Override
//public StringBuilder appendFieldsToString(final StringBuilder sb) {
//  StringBuilder result = super.appendFieldsToString(sb);
//  result.append("; prev=").append(this.prevLRU==null?"null":"not null");
//  result.append("; next=").append(this.nextLRU==null?"null":"not null");
//  return result;
//}
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  private Object key;
  @Override
  public final Object getRawKey() {
    return this.key;
  }
  @Override
  protected void _setRawKey(Object key) {
    this.key = key;
  }
  private volatile Object value;
  @Override
  protected Object getValueField() {
    return this.value;
  }
  @Override
  protected void setValueField(Object v) {
    this.value = v;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private static RegionEntryFactory factory = new RegionEntryFactory() {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      return new VMThinLRURegionEntryHeap(context, key, value);
    }
    public final Class<?> getEntryClass() {
      return VMThinLRURegionEntryHeap.class;
    }
    public RegionEntryFactory makeVersioned() {
      return VMThinLRURegionEntryHeap.getEntryFactory();
    }
    @Override
    public RegionEntryFactory makeOnHeap() {
      return this;
    }
  };
  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
