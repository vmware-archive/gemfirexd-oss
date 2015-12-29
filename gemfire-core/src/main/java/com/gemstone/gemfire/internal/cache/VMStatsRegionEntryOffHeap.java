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


import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import com.gemstone.gemfire.internal.concurrent.AtomicUpdaterFactory;

import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.internal.InternalStatisticsDisabledException;
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
public class VMStatsRegionEntryOffHeap extends VMStatsRegionEntry
    implements OffHeapRegionEntry
{
  public VMStatsRegionEntryOffHeap (RegionEntryContext context, Object key,
    @Retained
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
  private static final AtomicLongFieldUpdater<VMStatsRegionEntryOffHeap> lastModifiedUpdater
    = AtomicUpdaterFactory.newLongFieldUpdater(VMStatsRegionEntryOffHeap.class, "lastModified");
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
  // stats code
  @Override
  public final void updateStatsForGet(boolean hit, long time)
  {
    setLastAccessed(time);
    if (hit) {
      incrementHitCount();
    } else {
      incrementMissCount();
    }
  }
  @Override
  public final void setLastModified(long lastModified) {
    _setLastModified(lastModified);
    if (!DISABLE_ACCESS_TIME_UPDATE_ON_PUT) {
      setLastAccessed(lastModified);
    }
  }
  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryOffHeap> hitCountUpdater
    = AtomicUpdaterFactory.newIntegerFieldUpdater(VMStatsRegionEntryOffHeap.class, "hitCount");
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryOffHeap> missCountUpdater
    = AtomicUpdaterFactory.newIntegerFieldUpdater(VMStatsRegionEntryOffHeap.class, "missCount");
  @Override
  public final long getLastAccessed() throws InternalStatisticsDisabledException {
    return this.lastAccessed;
  }
  private void setLastAccessed(long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }
  @Override
  public final long getHitCount() throws InternalStatisticsDisabledException {
    return this.hitCount & 0xFFFFFFFFL;
  }
  @Override
  public final long getMissCount() throws InternalStatisticsDisabledException {
    return this.missCount & 0xFFFFFFFFL;
  }
  private void incrementHitCount() {
    hitCountUpdater.incrementAndGet(this);
  }
  private void incrementMissCount() {
    missCountUpdater.incrementAndGet(this);
  }
  @Override
  public final void resetCounts() throws InternalStatisticsDisabledException {
    hitCountUpdater.set(this,0);
    missCountUpdater.set(this,0);
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public final void txDidDestroy(long currTime) {
    setLastModified(currTime);
    setLastAccessed(currTime);
    this.hitCount = 0;
    this.missCount = 0;
  }
  @Override
  public boolean hasStats() {
    return true;
  }
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
  /**
   * All access done using ohAddrUpdater so it is used even though the compiler can not tell it is.
   */
  @Retained @Released private volatile long ohAddress;
  /**
   * I needed to add this because I wanted clear to call setValue which normally can only be called while the re is synced.
   * But if I sync in that code it causes a lock ordering deadlock with the disk regions because they also get a rw lock in clear.
   * Some hardware platforms do not support CAS on a long. If gemfire is run on one of those the AtomicLongFieldUpdater does a sync
   * on the re and we will once again be deadlocked.
   * I don't know if we support any of the hardware platforms that do not have a 64bit CAS. If we do then we can expect deadlocks
   * on disk regions.
   */
  private final static AtomicLongFieldUpdater<VMStatsRegionEntryOffHeap> ohAddrUpdater =
      AtomicUpdaterFactory.newLongFieldUpdater(VMStatsRegionEntryOffHeap.class, "ohAddress");
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Token getValueAsToken() {
    return OffHeapRegionEntryHelper.getValueAsToken(this);
  }
  @Override
  @Unretained
  protected Object getValueField() {
    return OffHeapRegionEntryHelper._getValue(this);
  }
  @Override
  protected void setValueField(@Unretained Object v) {
    OffHeapRegionEntryHelper.setValue(this, v);
  }
  @Override
  @Retained
  public Object _getValueRetain(RegionEntryContext context, boolean decompress) {
    return OffHeapRegionEntryHelper._getValueRetain(this, decompress);
  }
  @Override
  public long getAddress() {
    return ohAddrUpdater.get(this);
  }
  @Override
  public boolean setAddress(long expectedAddr, long newAddr) {
    return ohAddrUpdater.compareAndSet(this, expectedAddr, newAddr);
  }
  @Override
  @Released
  public void release() {
    OffHeapRegionEntryHelper.releaseEntry(this);
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private static RegionEntryFactory factory = new RegionEntryFactory() {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      return new VMStatsRegionEntryOffHeap(context, key, value);
    }
    public final Class<?> getEntryClass() {
      return VMStatsRegionEntryOffHeap.class;
    }
    public RegionEntryFactory makeVersioned() {
      return VMStatsRegionEntryOffHeap.getEntryFactory();
    }
    @Override
    public RegionEntryFactory makeOnHeap() {
      return VMStatsRegionEntryHeap.getEntryFactory();
    }
  };
  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
