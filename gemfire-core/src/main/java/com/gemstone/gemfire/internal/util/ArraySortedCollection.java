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

package com.gemstone.gemfire.internal.util;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * A sorted collection of elements that minimizes memory overhead by having the
 * elements in arrays and sorting the arrays at the end with given comparator.
 * The arrays themselves will be fixed size ones as provided, and the sorter
 * will merge all the arrays to give a final sorted collection. It does not
 * support remove operation but allows clearing the entire collection.
 * <p>
 * Note that the iterator of this class will do the merging on the fly, so it is
 * recommended to create an iterator only once.
 * <p>
 * This class is NOT THREAD-SAFE.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class ArraySortedCollection extends AbstractCollection<Object> {

  protected Object[] elements;

  protected final Comparator<Object> comparator;

  /**
   * If this is > 0 then indicates that sort has to return only from among these
   * many lower elements, so sorting can optimize by discarding large elements
   * where possible.
   */
  protected final int maxSortLimit;

  protected final int initArraySize;

  protected final int maxUnsortedArraySize;

  protected TimSort<Object> sorter;

  protected transient Object[] currentArray;

  protected transient int currentArrayIndex;

  protected transient int currentArrayPos;

  protected int currentArrayCheckSize;

  protected transient boolean currentArraySorted;

  /**
   * hashCode to Object map for efficient duplicate elimination if required
   */
  protected TIntObjectHashMapWithDups hashCodeToObjectMapWithDups;

  /**
   * hashing to use for fast duplicate determination when
   * {@link #hashCodeToObjectMapWithDups} is set
   */
  protected final TObjectHashingStrategy hashingStrategy;

  /**
   * the initial size of unsorted array which will be expanded to
   * <code>maxArraySize</code> if provided
   */
  protected static final int INIT_ARRAY_SIZE = 1024;

  public ArraySortedCollection(final Comparator<Object> comparator,
      final SortDuplicateObserver duplicateObserver,
      final TObjectHashingStrategy hashingStrategy,
      int maxUnsortedArraySize, final long maxSortLimit) {
    if (maxUnsortedArraySize <= 1 || maxUnsortedArraySize == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("maxArraySize=" + maxUnsortedArraySize);
    }
    this.comparator = comparator;
    if (maxUnsortedArraySize < INIT_ARRAY_SIZE) {
      maxUnsortedArraySize = INIT_ARRAY_SIZE;
    }
    // for maxSortedLimit sort 2*maxSortedLimit elements and then discard
    // the higher maxSortedLimit elements; if this exceeds maxArraySize then
    // ignore maxSortedLimit and only merge phase will do the discard
    this.maxSortLimit = (maxSortLimit > 0 && (maxSortLimit <
        (maxUnsortedArraySize >> 1))) ? (int)maxSortLimit : 0;
    this.initArraySize = this.maxSortLimit <= 0 ? Math.min(INIT_ARRAY_SIZE,
        maxUnsortedArraySize) : (this.maxSortLimit << 1);
    this.maxUnsortedArraySize = maxUnsortedArraySize;
    if (duplicateObserver != null && duplicateObserver.canSkipDuplicate()) {
      this.hashCodeToObjectMapWithDups = new TIntObjectHashMapWithDups(
          duplicateObserver, hashingStrategy);
      this.hashingStrategy = hashingStrategy;
    }
    else {
      this.hashCodeToObjectMapWithDups = null;
      this.hashingStrategy = null;
    }
    this.elements = new Object[4];
    // initialize the first array
    initUnsortedArray();
  }

  protected final void initUnsortedArray() {
    this.elements[0] = this.currentArray = new Object[this.initArraySize];
    this.currentArrayCheckSize = this.initArraySize;
  }

  public final Comparator<Object> getComparator() {
    return this.comparator;
  }

  @Override
  public boolean add(final Object v) {
    // first do duplicate elimination if required
    int vh = 0;
    TIntObjectHashMapWithDups dups = this.hashCodeToObjectMapWithDups;
    if (dups != null) {
      if (dups.isMarkedForReuse()) {
        this.hashCodeToObjectMapWithDups = dups = new TIntObjectHashMapWithDups(
            dups.getDuplicateObserver(), this.hashingStrategy);
      }
      if (dups.put((vh = this.hashingStrategy.computeHashCode(v)), v) != null) {
        // new object subsumed into existing object
        return false;
      }
    }
    // if new element can fit into current array, then add it there
    if (this.currentArrayPos < this.currentArrayCheckSize) {
      final Object[] currentArray = this.currentArray;
      if (currentArray != null) {
        currentArray[this.currentArrayPos++] = v;
      }
      else {
        // case of invocation after a clear()
        assert this.currentArrayIndex == 0: this.currentArrayIndex;
        assert this.currentArrayPos == 0: this.currentArrayPos;

        initUnsortedArray();
        this.currentArray[0] = v;
        this.currentArrayPos = 1;
      }
    }
    else {
      if (createNewArray(true)) {
        // insert into the cleared dups array
        if (dups != null && dups.put(vh, v) != null) {
          // should never happen after dups has been cleared
          throw new AssertionError("dups map still has entries after a clear? "
              + dups.toString());
        }
      }
      this.currentArray[this.currentArrayPos++] = v;
    }
    if (!this.currentArraySorted) {
      return true;
    }
    else {
      this.currentArraySorted = false;
      return true;
    }
  }

  protected boolean createNewArray(final boolean allowMemExpansion) {
    // first check if we hit the maxSortedLimit*2 where we will sort and keep
    // discarding higher maxSortedLimit elements; we do this only if we have
    // enough number of elements to discard which is determined as some fraction
    // of maxSortedLimit (may not happen if we are setup for overflow to disk
    // and hit evictionUp/criticalUp limits)
    final TIntObjectHashMapWithDups dups = this.hashCodeToObjectMapWithDups;
    if (this.maxSortLimit > 0
        && ((this.currentArrayPos * 3) > (this.maxSortLimit * 4))) {
      // we will just sort the elements without regard to possibly earlier list
      // of sorted elements in [0, maxSortedLimit) range since TimSort already
      // takes care of efficiently sorting/merging partially sorted arrays
      this.sorter = TimSort.<Object> sort(this.currentArray, 0,
          this.currentArrayPos, this.comparator, this.sorter);
      // clear the higher elements to reduce memory
      final int currentArrayPos = this.currentArrayPos;
      final Object[] currentArray = this.currentArray;
      if (dups == null || dups.isMarkedForReuse()) {
        for (int i = this.maxSortLimit; i < currentArrayPos; i++) {
          currentArray[i] = null;
        }
      }
      else {
        // also remove from dups map to reduce its size else it can grow
        // indefinitely
        final TObjectHashingStrategy hashingStrategy = this.hashingStrategy;
        int i = this.maxSortLimit;
        Object prev = currentArray[i - 1];
        for (; i < currentArrayPos; i++) {
          Object v = currentArray[i];
          if (prev != null) {
            // don't remove from dups map if this is a dup still in array
            if (!hashingStrategy.equals(prev, v)) {
              prev = null;
              dups.remove(hashingStrategy.computeHashCode(v), v);
            }
          }
          else {
            dups.remove(hashingStrategy.computeHashCode(v), v);
          }
          currentArray[i] = null;
        }
      }
      this.currentArrayPos = this.maxSortLimit;
      return false;
    }
    // then check if we can expand the unsorted array
    if (allowMemExpansion
        && this.currentArrayCheckSize < this.maxUnsortedArraySize) {
      this.elements[this.currentArrayIndex] = this.currentArray = Arrays
          .copyOf(this.currentArray, this.maxUnsortedArraySize);
      this.currentArrayCheckSize = this.maxUnsortedArraySize;
      return false;
    }
    // sort the current array and create a new one
    this.sorter = TimSort.<Object> sort(this.currentArray, 0,
        this.currentArrayPos, this.comparator, this.sorter);
    this.currentArrayIndex++;
    final int len = this.elements.length;
    if (len <= this.currentArrayIndex) {
      final Object[] newElements = new Object[len + 4];
      System.arraycopy(this.elements, 0, newElements, 0, len);
      this.elements = newElements;
    }
    this.elements[this.currentArrayIndex] = this.currentArray =
        new Object[this.initArraySize];
    this.currentArrayCheckSize = this.initArraySize;
    this.currentArrayPos = 0;

    // Clear the duplicate elimination map, if present, to minimize memory
    // growth. In the worst case the duplicates will be eliminated only for each
    // sorted array but not across (which will be handled when merging them).
    if (dups != null && !dups.isEmpty() && !dups.isMarkedForReuse()) {
      dups.clear();
    }
   return true;
  }

  @Override
  public boolean remove(Object v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    final Object[] elements = this.elements;
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] != null) {
        elements[i] = null;
      }
      else {
        break;
      }
    }
    this.currentArray = null;
    this.currentArrayIndex = 0;
    this.currentArrayPos = 0;
    this.currentArraySorted = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int size() {
    final long size = longSize();
    return size < Integer.MAX_VALUE ? (int)size : Integer.MAX_VALUE;
  }

  public long longSize() {
    long sz = 0;
    for (int i = 0; i < this.currentArrayIndex; i++) {
      sz += ((Object[])this.elements[i]).length;
    }
    return (sz + this.currentArrayPos);
  }

  /**
   * Returns an {@link Enumerator} over the sorted collection of elements.
   */
  public Enumerator enumerator() {
    // first sort the last array if required
    if (!this.currentArraySorted) {
      if (this.currentArrayPos > 0) {
        this.sorter = TimSort.<Object> sort(this.currentArray, 0,
            this.currentArrayPos, this.comparator, this.sorter);
      }
      else if (this.currentArray == null) {
        assert this.currentArrayIndex == 0: this.currentArrayIndex;
        assert this.currentArrayPos == 0: this.currentArrayPos;

        initUnsortedArray();
      }
      this.currentArraySorted = true;
    }
    // now create wrapper iterators for all arrays and a MergeSortedSources
    // around it
    if (this.currentArrayIndex > 0) {
      Enumerator[] itrs = new Enumerator[this.currentArrayIndex + 1];
      for (int i = 0; i < this.currentArrayIndex; i++) {
        itrs[i] = createIteratorForElementArray(i);
      }
      itrs[this.currentArrayIndex] = new ArrayItr(this.currentArray,
          this.currentArrayPos);
      TIntObjectHashMapWithDups dups = this.hashCodeToObjectMapWithDups;
      if (dups == null) {
        return new MergeSortedSources(itrs, this.comparator,
            this.currentArrayIndex, null, null);
      }
      else {
        // reuse the duplicates map
        if (dups.isMarkedForReuse()) {
          // already being reused, so create new one
          dups = new TIntObjectHashMapWithDups(dups.getDuplicateObserver(),
              this.hashingStrategy);
        }
        else if (!dups.isEmpty()) {
          dups.clear();
          dups.markForReuse();
        }
        return new MergeSortedSources(itrs, this.comparator,
            this.currentArrayIndex, dups, this.hashingStrategy);
      }
    }
    else {
      return new ArrayItr(this.currentArray, this.currentArrayPos);
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Object> iterator() {
    return (Iterator<Object>)enumerator();
  }

  protected Enumerator createIteratorForElementArray(int index) {
    Object[] a = (Object[])this.elements[index];
    return new ArrayItr(a, a.length);
  }

  /**
   * An iterator for an array of elements.
   */
  protected static final class ArrayItr implements Enumerator,
      Iterator<Object> {

    private final Object[] a;

    private final int size;

    private int currentPosition;

    protected ArrayItr(Object[] array, int size) {
      this.a = array;
      this.size = size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
      return this.currentPosition < this.size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object next() {
      try {
        return this.a[this.currentPosition++];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new NoSuchElementException();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object nextElement() {
      if (this.currentPosition < this.size) {
        return this.a[this.currentPosition++];
      }
      else {
        return null;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
