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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * Merge n sorted sources to provide an iterator that is sorted across all
 * sources.
 * 
 * @author swale
 * @since 7.5
 */
public final class MergeSortedSources implements Enumerator, Iterator<Object> {

  private final Enumerator[] sources;
  private final Enumerator lastSource;
  private final PriorityQueue<ArraySortedElement> currentSortedBuffer;
  private final TIntObjectHashMapWithDups hashCodeToObjectMapWithDups;
  private final TObjectHashingStrategy hashingStrategy;
  private Object currentElement;

  /**
   * Used by {@link ArraySortedCollection} for elements enqueued for sorting.
   */
  private static final class ArraySortedElement {
    final int arrayIndex;
    Object element;
    int hash;

    ArraySortedElement(int arrayIndex) {
      this.arrayIndex = arrayIndex;
    }

    static final class ASECompare implements Comparator<ArraySortedElement> {
      final Comparator<Object> comparator;

      ASECompare(Comparator<Object> comparator) {
        this.comparator = comparator;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public int compare(ArraySortedElement o1, ArraySortedElement o2) {
        return this.comparator.compare(o1.element, o2.element);
      }
    }
  }

  /**
   * Created a sorted merged output source given multiple individual sorted
   * source iterators.
   * 
   * @param sources
   *          the sorted input sources
   * @param comparator
   *          the comparator to use
   * @param lastSourceIndex
   *          the index of the last source in "sources" argument -- allows it to
   *          be something other than (sources.length - 1) if required
   * @param dupsMap
   *          a map used for duplicate elimination used to determine whether to
   *          eliminate a duplicate or return it in the sorted result
   * @param hashingStrategy
   *          used for hashCode and equals in <code>dupsMap</code>
   */
  public MergeSortedSources(final Enumerator[] sources,
      final Comparator<Object> comparator, final int lastSourceIndex,
      final TIntObjectHashMapWithDups dupsMap,
      final TObjectHashingStrategy hashingStrategy) {
    if (lastSourceIndex > 0) {
      this.sources = sources;
      this.lastSource = null;
      final PriorityQueue<ArraySortedElement> buffer = this.currentSortedBuffer
          = new PriorityQueue<ArraySortedElement>(lastSourceIndex + 1,
              new ArraySortedElement.ASECompare(comparator));
      // enqueue the head from each array
      Enumerator itr;
      Object o;
      if (dupsMap == null) {
        for (int i = 0; i <= lastSourceIndex; i++) {
          itr = sources[i];
          if ((o = itr.nextElement()) != null) {
            ArraySortedElement a = new ArraySortedElement(i);
            a.element = o;
            buffer.add(a);
          }
        }
        this.hashingStrategy = null;
        this.hashCodeToObjectMapWithDups = null;
      }
      else {
        int hash;
        for (int i = 0; i <= lastSourceIndex; i++) {
          itr = sources[i];
          while ((o = itr.nextElement()) != null) {
            hash = hashingStrategy.computeHashCode(o);
            if (dupsMap.put(hash, o) != null) {
              // new object subsumed into existing object
              continue;
            }
            ArraySortedElement a = new ArraySortedElement(i);
            a.element = o;
            a.hash = hash;
            buffer.add(a);
            break;
          }
        }
        this.hashingStrategy = hashingStrategy;
        this.hashCodeToObjectMapWithDups = dupsMap;
      }
    }
    else {
      // for the special case of single array, just iterate over the last one
      this.sources = null;
      this.lastSource = sources[lastSourceIndex];
      this.currentSortedBuffer = null;
      this.hashingStrategy = null;
      this.hashCodeToObjectMapWithDups = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    final Enumerator lastSrc = this.lastSource;
    if (lastSrc != null) {
      if (this.currentElement == null) {
        return ((this.currentElement = lastSrc.nextElement()) != null);
      }
      else {
        return true;
      }
    }
    else if (this.currentElement == null) {
      return (this.currentElement = moveNext()) != null;
    }
    else {
      return true;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object next() {
    Object v = this.currentElement;
    if (v != null) {
      this.currentElement = null;
      return v;
    }

    final Enumerator lastSrc = this.lastSource;
    if (lastSrc != null) {
      if ((v = lastSrc.nextElement()) != null) {
        return v;
      }
    }
    else if ((v = moveNext()) != null) {
      return v;
    }
    throw new NoSuchElementException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object nextElement() {
    final Enumerator lastSrc = this.lastSource;
    if (lastSrc != null) {
      return lastSrc.nextElement();
    }
    else {
      return moveNext();
    }
  }

  private final Object moveNext() {
    final PriorityQueue<ArraySortedElement> buffer = this.currentSortedBuffer;
    if (buffer != null) {
      // dequeue from front and enqueue a new element from the array whose
      // element got dequeued
      final ArraySortedElement a = buffer.poll();
      if (a != null) {
        final Object v = a.element;
        final int arrayIndex = a.arrayIndex;
        final Enumerator source = this.sources[arrayIndex];
        final TIntObjectHashMapWithDups dupsMap =
            this.hashCodeToObjectMapWithDups;
        if (dupsMap == null) {
          final Object o;
          if ((o = source.nextElement()) != null) {
            // more elements available in this array
            a.element = o;
            buffer.add(a);
          }
        }
        else {
          final TObjectHashingStrategy hashingStrategy = this.hashingStrategy;
          // remove the object being returned from duplicate map to keep the
          // size of map small
          dupsMap.remove(a.hash, v);

          Object o;
          int hash;
          while ((o = source.nextElement()) != null) {
            hash = hashingStrategy.computeHashCode(o);
            if (dupsMap.put(hash, o) != null) {
              // new object subsumed into existing object
              continue;
            }
            // more elements available in this array
            a.element = o;
            a.hash = hash;
            buffer.add(a);
            break;
          }
        }
        return v;
      }
      else {
        return null;
      }
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
