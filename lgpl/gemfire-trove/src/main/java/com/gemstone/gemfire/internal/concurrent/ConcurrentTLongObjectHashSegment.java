/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
/*
 * Contains code from GNU Trove having the license below.
 *
 * Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.gemstone.gemfire.internal.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gnu.trove.HashFunctions;
import com.gemstone.gnu.trove.HashingStats;
import com.gemstone.gnu.trove.PrimeFinder;
import com.gemstone.gnu.trove.THashSet;

/**
 * Base class for a segment of concurrent hash maps with long keys and
 * Object values allowing for appropriate locking to make it thread-safe.
 *
 * Adapted from Trove's <tt>TLongObjectHashMap</tt>.
 *
 * @author swale
 * @since gfxd 1.0
 *
 * @param <V>
 *          the type of objects in the segment
 */
@SuppressWarnings("serial")
final class ConcurrentTLongObjectHashSegment<V> extends ReentrantReadWriteLock {

  static final Object REMOVED = THashSet.REMOVED;

  /** the set of keys */
  long[] keys;

  /** the set of values */
  Object[] values;

  /** the current number of occupied slots in the hash. */
  int size;

  /** the current number of free slots in the hash. */
  int free;

  /**
   * The maximum number of elements allowed without allocating more space.
   */
  int maxSize;

  /** parameters used for hashing */
  final THashParameters params;

  /** tracks the total size of the top-level ConcurrentTLongHash structure */
  final AtomicLong totalSize;

  ConcurrentTLongObjectHashSegment(int initialCapacity, THashParameters params,
      AtomicLong totalSize) {
    int capacity = PrimeFinder.nextPrime((int)Math.ceil(initialCapacity
        / params.loadFactor));
    computeMaxSize(capacity, params);
    this.keys = new long[capacity];
    this.values = new Object[capacity];
    this.params = params;
    this.totalSize = totalSize;
  }

  private void computeMaxSize(int capacity, THashParameters params) {
    // need at least one free slot for open addressing
    this.maxSize = Math.min(capacity - 1,
        (int)Math.floor(capacity * params.loadFactor));
    this.free = capacity - this.size; // reset the free element count
  }

  public Object get(long key, final int hash) {
    super.readLock().lock();
    try {
      int index = index(key, hash);
      if (index >= 0) {
        Object val = this.values[index];
        return val != REMOVED ? val : null;
      }
      else {
        return null;
      }
    } finally {
      super.readLock().unlock();
    }
  }

  public boolean contains(long key, final int hash) {
    super.readLock().lock();
    try {
      return index(key, hash) >= 0;
    } finally {
      super.readLock().unlock();
    }
  }

  public boolean containsValue(Object val) {
    super.readLock().lock();
    try {
      Object[] vals = this.values;
      if (val != null) {
        for (Object v : vals) {
          if (v != null && v != REMOVED && (v == val || val.equals(v))) {
            return true;
          }
        }
      }
      else {
        // special case null values so that we don't have to
        // perform null checks before every call to equals()
        for (Object v : vals) {
          if (v == null) {
            return true;
          }
        }
      }
      return false;
    } finally {
      super.readLock().unlock();
    }
  }

  /**
   * Inserts a key/value pair into the map.
   *
   * @param key
   *          an <code>long</code> key
   * @param v
   *          an <code>Object</code> value
   * @return the previous value associated with <tt>key</tt>, or null if none
   *         was found.
   */
  public Object put(long key, V v, final int hash) {
    super.writeLock().lock();
    try {
      return putP(key, v, hash);
    } finally {
      super.writeLock().unlock();
    }
  }

  final Object putP(long key, V v, final int hash) {
    final long[] keys = this.keys;
    final Object[] vals = this.values;
    Object previous = null;
    int index = insertionIndex(key, hash, keys, vals);
    boolean isNewMapping = true;
    if (index < 0) {
      index = -index - 1;
      previous = vals[index];
      isNewMapping = false;
    }
    keys[index] = key;
    vals[index] = v;
    if (isNewMapping) {
      postInsertHook(previous == null);
    }

    return previous != REMOVED ? previous : null;
  }

  /**
   * Inserts a key/value pair into the map if key is not already present.
   *
   * @param key
   *          an <code>long</code> key
   * @param v
   *          an <code>Object</code> value
   * @return the previous value associated with <tt>key</tt>, or null if none
   *         was found.
   */
  public Object putIfAbsent(long key, V v, final int hash) {
    super.writeLock().lock();
    try {
      final long[] keys = this.keys;
      final Object[] vals = this.values;
      Object previous = null;
      int index = insertionIndex(key, hash, keys, vals);
      boolean isNewMapping = true;
      if (index < 0) {
        index = -index - 1;
        previous = vals[index];
        if (previous != null && previous != REMOVED) {
          return previous;
        }
        isNewMapping = false;
      }
      keys[index] = key;
      vals[index] = v;
      if (isNewMapping) {
        postInsertHook(previous == null);
      }

      return null;
    } finally {
      super.writeLock().unlock();
    }
  }

  /**
   * Deletes a key/value pair from the map.
   *
   * @param key
   *          a <code>long</code> key
   * @return an <code>Object</code> value, or null if no mapping for key
   *         exists
   */
  public Object remove(long key, final int hash) {
    super.writeLock().lock();
    try {
      return removeP(key, hash);
    } finally {
      super.writeLock().unlock();
    }
  }

  final Object removeP(long key, final int hash) {
    Object prev = null;
    int index = index(key, hash);
    if (index >= 0) {
      prev = this.values[index];
      removeAt(index); // clear key,state; adjust size
    }
    return prev != REMOVED ? prev : null;
  }

  public void clear() {
    super.writeLock().lock();
    try {
      final long[] keys = this.keys;
      final Object[] vals = this.values;
      final int size = vals.length;
      for (int i = 0; i < size; i++) {
        keys[i] = 0;
        vals[i] = null;
      }
      this.totalSize.addAndGet(-this.size);
      this.size = 0;
      this.free = size;
    } finally {
      super.writeLock().unlock();
    }
  }

  /**
   * Locates the index of <tt>key</tt>.
   *
   * @param key
   *          a <code>long</code> value
   * @return the index of <tt>val</tt> or -1 if it isn't in the set.
   */
  protected int index(long key, final int hash) {
    int probe, index, length;
    long[] keys;
    Object[] values;
    Object cur;
    final HashingStats stats;

    keys = this.keys;
    values = this.values;
    length = keys.length;
    stats = this.params.stats;
    index = hash % length;
    cur = values[index];

    if (cur != null && (cur == REMOVED || keys[index] != key)) {
      long start = -1L;
      if (stats != null) {
        start = stats.getNanoTime();
      }
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
        cur = values[index];
      } while (cur != null && (cur == REMOVED || keys[index] != key));
      if (stats != null) {
        stats.endQueryResultsHashCollisionProbe(start);
      }
    }

    return cur == null ? -1 : index;
  }

  /**
   * Locates the index at which <tt>key</tt> can be inserted. if there is
   * already a value equal()ing <tt>key</tt> in the set, returns that value as
   * a negative integer.
   *
   * @param key
   *          a <code>long</code> value
   * @return an <code>int</code> value
   */
  final int insertionIndex(long key, final int hash, final long[] keys,
      final Object[] values) {
    int probe, index, length;
    Object cur;
    final HashingStats stats;

    length = keys.length;
    stats = this.params.stats;
    index = hash % length;
    cur = values[index];

    if (cur == null) {
      return index; // empty, all done
    }
    else if (cur != REMOVED && keys[index] == key) {
      return -index - 1; // already stored
    }
    else { // already FULL or REMOVED, must probe
      long start = -1L;
      if (stats != null) {
        start = stats.getNanoTime();
        stats.incQueryResultsHashCollisions();
      }
      // compute the double hash
      probe = 1 + (hash % (length - 2));

      // if the slot we landed on is FULL (but not removed), probe
      // until we find an empty slot, a REMOVED slot, or an element
      // equal to the one we are trying to insert.
      // finding an empty slot means that the value is not present
      // and that we should use that slot as the insertion point;
      // finding a REMOVED slot means that we need to keep searching,
      // however we want to remember the offset of that REMOVED slot
      // so we can reuse it in case a "new" insertion (i.e. not an update)
      // is possible.
      // finding a matching value means that we've found that our desired
      // key is already in the table

      if (cur != REMOVED) {
        // starting at the natural offset, probe until we find an
        // offset that isn't full.
        do {
          index -= probe;
          if (index < 0) {
            index += length;
          }
          cur = values[index];
        } while (cur != null && cur != REMOVED && keys[index] != key);
      }

      // if the index we found was removed: continue probing until we
      // locate a free location or an element which equal()s the
      // one we have.
      if (cur == REMOVED) {
        int firstRemoved = index;
        while (cur != null && (cur == REMOVED || keys[index] != key)) {
          index -= probe;
          if (index < 0) {
            index += length;
          }
          cur = values[index];
        }
        if (stats != null) {
          stats.endQueryResultsHashCollisionProbe(start);
        }
        return (cur != null && cur != REMOVED) ? -index - 1 : firstRemoved;
      }
      // if it's full, the key is already stored
      if (stats != null) {
        stats.endQueryResultsHashCollisionProbe(start);
      }
      return (cur != null && cur != REMOVED) ? -index - 1 : index;
    }
  }

  /**
   * After an insert, this hook is called to adjust the size/free values of
   * the set and to perform rehashing if necessary.
   */
  private void postInsertHook(boolean usedFreeSlot) {
    if (usedFreeSlot) {
      free--;
    }

    this.totalSize.incrementAndGet();
    // rehash whenever we exhaust the available space in the table
    if (++size > maxSize || free == 0) {
      // choose a new capacity suited to the new state of the table
      // if we've grown beyond our maximum size, increase capacity;
      // if we've exhausted the free spots, rehash to the same capacity,
      // which will free up any stale removed slots for reuse.
      final int capacity = capacity();
      double expansionFactor = Math.min(2.0,
          (3.0 - (2.0 * this.params.loadFactor)));
      int newCapacity = size > maxSize ? PrimeFinder
          .nextPrime((int)((double)capacity * expansionFactor)) : capacity;
      rehash(newCapacity);
      computeMaxSize(capacity(), this.params);
    }
  }

  private void rehash(int newCapacity) {
    long[] oldKeys = this.keys;
    int oldCapacity = oldKeys.length;
    Object[] oldVals = this.values;
    long[] newKeys = new long[newCapacity];
    Object[] newVals = new Object[newCapacity];

    for (int i = oldCapacity; i-- > 0;) {
      if (oldVals[i] != null && oldVals[i] != REMOVED) {
        long o = oldKeys[i];
        int hash = computeHashCode(o);
        int index = insertionIndex(o, hash, newKeys, newVals);
        if (index < 0) {
          throwObjectContractViolation(newKeys[(-index - 1)], o);
        }
        newKeys[index] = o;
        newVals[index] = oldVals[i];
      }
    }
    this.keys = newKeys;
    this.values = newVals;
  }

  private int capacity() {
    return this.values.length;
  }

  static int computeHashCode(final long k) {
    return HashFunctions.hash(k) & 0x7fffffff;
  }

  private void removeAt(int index) {
    this.totalSize.decrementAndGet();
    this.size--;
    this.keys[index] = 0;
    this.values[index] = REMOVED;
  }

  /**
   * Convenience methods to use in throwing exceptions about badly behaved
   * user objects employed as keys. We have to throw an
   * IllegalArgumentException with a rather verbose message telling the user
   * that they need to fix their object implementation to conform to the
   * general contract for java.lang.Object.
   *
   * @param o1
   *          the first of the equal elements with unequal hash codes.
   * @param o2
   *          the second of the equal elements with unequal hash codes.
   * @exception IllegalArgumentException
   *              the whole point of this method.
   */
  private void throwObjectContractViolation(long o1, long o2)
      throws IllegalArgumentException {
    throw new IllegalArgumentException(
        "Equal objects must have equal hashcodes. "
            + "During rehashing, map discovered that "
            + "the following two objects claim to be "
            + "equal (as in java.lang.Object.equals()) "
            + "but their hashCodes (or those calculated by "
            + "your TObjectHashingStrategy) are not equal."
            + "This violates the general contract of "
            + "java.lang.Object.hashCode(). See bullet point two "
            + "in that method's documentation. object #1 =" + o1
            + "; object #2 =" + o2);
  }
}
