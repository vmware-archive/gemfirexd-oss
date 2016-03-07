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

package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ObjectEqualsHashingStrategy;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholdListener;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.gemstone.gemfire.internal.util.ArraySortedCollection;
import com.gemstone.gemfire.internal.util.ArraySortedCollectionWithOverflow;
import com.gemstone.gemfire.internal.util.SortDuplicateObserver;
import com.gemstone.gemfire.internal.util.TIntObjectHashMapWithDups;
import com.gemstone.gnu.trove.TIntHashSet;

/**
 * Test sort and overflow handling by {@link ArraySortedCollection} and
 * {@link ArraySortedCollectionWithOverflow}.
 */
public class ArraySortedCollectionJUnitTest extends TestCase {

  File overflowDir;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    this.overflowDir = new File(".","backup");
    this.overflowDir.mkdir();

    Properties props = new Properties();
    props.setProperty("log-level", "warning");
    new CacheFactory(props).create();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    if (this.overflowDir != null) {
      this.overflowDir.delete();
    }

    InternalDistributedSystem ids = InternalDistributedSystem
        .getConnectedInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  static int numCmpCalls;
  static final class DefaultComparator implements Comparator<Object> {

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object o1, Object o2) {
      numCmpCalls++;
      return ((Comparable<Object>)o1).compareTo(o2);
    }
  };
  static final DefaultComparator defaultComparator = new DefaultComparator();

  /** will set isEviction/isCritical at regular intervals */
  static final MemoryThresholdListener dummyListener =
      new MemoryThresholdListener() {

    private static final int NUM_CALLS_BEFORE_EVICT = 40;
    private static final int NUM_CALLS_BEFORE_CRITICAL = 60;
    private int numCalls;

    @Override
    public void onEvent(MemoryEvent event) {
    }

    @Override
    public boolean isEviction() {
      this.numCalls++;
      if (this.numCalls >= NUM_CALLS_BEFORE_EVICT
          && ((this.numCalls % NUM_CALLS_BEFORE_EVICT) == 0
          || (PartitionedRegion.rand.nextBoolean()
              && (this.numCalls % NUM_CALLS_BEFORE_EVICT) == 1))) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isCritical() {
      if (this.numCalls >= NUM_CALLS_BEFORE_CRITICAL
          && ((this.numCalls % NUM_CALLS_BEFORE_CRITICAL) == 0
          || (PartitionedRegion.rand.nextBoolean()
              && (this.numCalls % NUM_CALLS_BEFORE_CRITICAL) == 1))) {
        return true;
      }
      return false;
    }
  };

  private static final int numItems = 2000000;

  private void checkArraySorting(ArraySortedCollection sortedSet,
      String testName) {
    for (int c = 1; c <= 4; c++) {
      // insert elements in set and check if sorting is proper with overflow
      TIntHashSet randInts = new TIntHashSet(numItems);
      final String[] values = new String[numItems];
      for (int i = 0; i < numItems; i++) {
        int rnd = PartitionedRegion.rand.nextInt(numItems);
        randInts.add(rnd);
        values[i] = "strkey-" + rnd;
      }
      numCmpCalls = 0;
      long start = NanoTimer
          .nativeNanoTime(NativeCalls.CLOCKID_THREAD_CPUTIME_ID, true);
      int n;
      for (int i = 0; i < numItems; i++) {
        sortedSet.add(values[i]);
      }
      if ((n = sortedSet.size()) != numItems) {
        fail("inserted " + numItems + " but got " + n);
      }
      // now check if properly sorted
      String prev = "";
      n = 0;
      for (Object s : sortedSet) {
        String str = (String)s;
        if ((c % 2) == 1) {
          if (str.compareTo(prev) < 0) {
            fail("incorrect ordering for '" + str + "', prev='" + prev + "'");
          }
          int suffix = Integer.parseInt(str.substring("strkey-".length()));
          randInts.remove(suffix);
        }
        prev = str;
        n++;
      }
      if (n != numItems) {
        fail("inserted " + numItems + " but got " + n);
      }
      if ((c % 2) == 1 && !randInts.isEmpty()) {
        fail("did not get all elements in result for ints: "
            + randInts.toString());
      }
      if (sortedSet instanceof ArraySortedCollectionWithOverflow) {
        System.out.println("Num overflowed elements = "
            + ((ArraySortedCollectionWithOverflow)sortedSet)
                .numOverflowedElements() + " for total = " + numItems);
      }
      long end = NanoTimer.nativeNanoTime(NativeCalls.CLOCKID_THREAD_CPUTIME_ID, true);
      System.out.println("Time taken for " + testName + ": "
          + ((end - start) / 1000000) + " millis, cmp calls = " + numCmpCalls);

      sortedSet.clear();
      numCmpCalls = 0;
    }
  }

  public void testArraySortedData() throws Exception {
    ArraySortedCollection sort = new ArraySortedCollection(defaultComparator,
        null, null, 1000, 0);
    checkArraySorting(sort, "testArraySortedData");
  }

  public void DEBUG_testArraySorting() {
    for (int c = 1; c <= 4; c++) {
      // insert elements in set and check if sorting is proper with overflow
      TIntHashSet randInts = new TIntHashSet(numItems);
      final String[] values = new String[numItems];
      for (int i = 0; i < numItems; i++) {
        int rnd = PartitionedRegion.rand.nextInt(numItems);
        randInts.add(rnd);
        values[i] = "strkey-" + rnd;
      }
      numCmpCalls = 0;
      final String[] sortedSet = new String[numItems];
      long start = NanoTimer
          .nativeNanoTime(NativeCalls.CLOCKID_THREAD_CPUTIME_ID, true);
      for (int i = 0; i < numItems; i++) {
        sortedSet[i] = values[i];
      }
      Arrays.sort(sortedSet, defaultComparator);
      // now check if properly sorted
      String prev = "";
      int n = 0;
      for (Object s : sortedSet) {
        String str = (String)s;
        if ((c % 2) == 1) {
          if (str.compareTo(prev) < 0) {
            fail("incorrect ordering for '" + str + "', prev='" + prev + "'");
          }
          int suffix = Integer.parseInt(str.substring("strkey-".length()));
          randInts.remove(suffix);
        }
        prev = str;
        n++;
      }
      if (n != numItems) {
        fail("inserted " + numItems + " but got " + n);
      }
      if ((c % 2) == 1 && !randInts.isEmpty()) {
        fail("did not get all elements in result for ints: "
            + randInts.toString());
      }
      long end = NanoTimer.nativeNanoTime(NativeCalls.CLOCKID_THREAD_CPUTIME_ID, true);
      System.out.println("Time taken for testArraySorting: "
          + ((end - start) / 1000000) + " millis, cmp calls = " + numCmpCalls);

      numCmpCalls = 0;
    }
  }

  public void testOverflowSortedData() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getExisting();
    ArraySortedCollectionWithOverflow sort = ArraySortedCollectionWithOverflow
        .create(defaultComparator, null, null, ObjectSizer.SIZE_CLASS_ONCE,
            1000, 0, dummyListener, this.overflowDir.getAbsolutePath(), cache);

    checkArraySorting(sort, "testOverflowSortedData");
    sort.close();
  }

  public void testDupsMap() throws Exception {
    // insert duplicate entries into the map and check for duplicate elimination
    SortDuplicateObserver duplicateObserver = new SortDuplicateObserver() {

      @Override
      public boolean canSkipDuplicate() {
        return true;
      }

      @Override
      public boolean eliminateDuplicate(Object insertObject,
          Object existingObject) {
        return true;
      }
    };
    TIntObjectHashMapWithDups dupsMap = new TIntObjectHashMapWithDups(
        duplicateObserver, ObjectEqualsHashingStrategy.getInstance());
    TIntHashSet vals = new TIntHashSet();

    for (int i = 0; i < 300; i += 3) {
      Object existing = dupsMap.put(i % 10, i % 50);
      if (vals.contains(i % 50)) {
        assertEquals(i % 50, existing);
      }
      else {
        assertNull(existing);
        vals.add(i % 50);
      }
    }
    assertEquals(vals.size(), dupsMap.size());
  }
}
