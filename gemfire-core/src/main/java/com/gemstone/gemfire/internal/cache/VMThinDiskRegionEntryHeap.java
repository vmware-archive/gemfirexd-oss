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
import com.gemstone.gemfire.internal.cache.persistence.DiskRecoveryStore;
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
public class VMThinDiskRegionEntryHeap extends VMThinDiskRegionEntry
{
  public VMThinDiskRegionEntryHeap (RegionEntryContext context, Object key,
    Object value
      ) {
    super(context,
          (value instanceof RecoveredEntry ? null : value)
        );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
    this.key = key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinDiskRegionEntryHeap> lastModifiedUpdater
    = AtomicUpdaterFactory.newLongFieldUpdater(VMThinDiskRegionEntryHeap.class, "lastModified");
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
  // disk code
  protected void initialize(RegionEntryContext context, Object value) {
    diskInitialize(context, value);
  }
  @Override
  public int updateAsyncEntrySize(EnableLRU capacityController) {
    throw new IllegalStateException("should never be called");
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private void diskInitialize(RegionEntryContext context, Object value) {
    DiskRecoveryStore drs = (DiskRecoveryStore)context;
    DiskStoreImpl ds = drs.getDiskStore();
    long maxOplogSize = ds.getMaxOplogSize();
    //get appropriate instance of DiskId implementation based on maxOplogSize
    this.id = DiskId.createDiskId(maxOplogSize, true/* is persistence */, ds.needsLinkedList());
    Helper.initialize(this, drs, value);
  }
  /**
   * DiskId
   * 
   * @since 5.1
   */
  protected DiskId id;//= new DiskId();
  public DiskId getDiskId() {
    return this.id;
  }
  @Override
  public void setDiskId(RegionEntry old) {
    this.id = ((AbstractDiskRegionEntry)old).getDiskId();
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
      return new VMThinDiskRegionEntryHeap(context, key, value);
    }
    public final Class<?> getEntryClass() {
      return VMThinDiskRegionEntryHeap.class;
    }
    public RegionEntryFactory makeVersioned() {
      return VMThinDiskRegionEntryHeap.getEntryFactory();
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
