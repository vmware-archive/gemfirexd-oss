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
package com.gemstone.gemfire.internal.util;

import com.gemstone.gnu.trove.TIntHashingStrategy;
import com.gemstone.gnu.trove.TIntObjectHashMap;
import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * Difference from {@link TIntObjectHashMap} is that this map allows for
 * duplicates to be inserted into the map.
 * 
 * @author swale
 * @since gfxd 2.0
 */
public final class TIntObjectHashMapWithDups extends TIntObjectHashMap {

  private static final long serialVersionUID = -9063763171316658251L;

  private final transient SortDuplicateObserver duplicateObserver;

  private final TObjectHashingStrategy objectHasher;

  private boolean markedForReuse;

  /**
   * Creates a new <code>TIntObjectHashMapWithDups</code> instance with the
   * default capacity and load factor.
   */
  public TIntObjectHashMapWithDups() {
    super();
    this.duplicateObserver = null;
    this.objectHasher = null;
  }

  /**
   * Creates a new <code>TIntObjectHashMapWithDups</code> instance with given
   * {@link SortDuplicateObserver}.
   * 
   * @param duplicateObserver
   *          used to determine whether to eliminate a duplicate or put it into
   *          the map
   * @param objectHasher
   *          the object equals strategy to use for duplicate determination and
   *          elimination by <code>duplicateObserver</code>
   */
  public TIntObjectHashMapWithDups(SortDuplicateObserver duplicateObserver,
      TObjectHashingStrategy objectHasher) {
    super();
    this.duplicateObserver = duplicateObserver;
    this.objectHasher = objectHasher;
  }

  /**
   * Creates a new <code>TIntObjectHashMapWithDups</code> instance with a prime
   * value at or near the specified capacity and load factor.
   * 
   * @param initialCapacity
   *          used to find a prime capacity for the table.
   * @param loadFactor
   *          used to calculate the threshold over which rehashing takes place.
   * @param strategy
   *          used to compute hash codes and to compare keys.
   * @param duplicateObserver
   *          used to determine whether to eliminate a duplicate or put it into
   *          the map
   * @param objectHasher
   *          the object equals strategy to use for duplicate determination and
   *          elimination by <code>duplicateObserver</code>
   */
  public TIntObjectHashMapWithDups(int initialCapacity, float loadFactor,
      TIntHashingStrategy strategy, SortDuplicateObserver duplicateObserver,
      TObjectHashingStrategy objectHasher) {
    super(initialCapacity, loadFactor, strategy);
    this.duplicateObserver = duplicateObserver;
    this.objectHasher = objectHasher;
  }

  /**
   * Inserts a key/value pair into the map. If there is an existing key in the
   * map, the value will either be subsumed into the existing value as per
   * {@link SortDuplicateObserver#eliminateDuplicate(Object, Object)}, or a
   * duplicate will be inserted into the map.
   * 
   * @param key
   *          an <code>int</code> value
   * @param val
   *          an <code>Object</code> value
   * @return the previous value associated with <tt>key</tt>, or null if none
   *         was found.
   */
  @Override
  public final Object put(final int key, final Object val) {
    final byte previousState;
    final int index = insertionIndex(key, val);
    // nothing to do if it already exists
    if (index >= 0) {
      previousState = _states[index];
      _set[index] = key;
      _states[index] = FULL;
      _values[index] = val;
      postInsertHook(previousState == FREE);
      return null;
    }
    else {
      // indicates that value was subsumed in the existing one
      return _values[-index - 1];
    }
  }

  /**
   * Deletes a key/value pair from the map only if both key and value match.
   * This will remove only the first occurence of the key, value pair if there
   * are multiple of them in the map.
   * 
   * @param key
   *          an <code>int</code> value
   * @param val
   *          an <code>Object</code> value
   * @return true if a mapping for key+value exists, and false otherwise
   */
  public final boolean remove(final int key, final Object val) {
    final int index = index(key, val);
    if (index >= 0) {
      removeAt(index); // clear key,state; adjust size
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public final int capacity() {
    return _states.length;
  }

  /** Empties the map. */
  @Override
  public final void clear() {
    _size = 0;
    final byte[] states = _states;
    final Object[] vals = _values;
    final int capacity = _free = states.length;

    // no need to reset the keys; vals are nulled to allow them to be GCed
    for (int i = 0; i < capacity; i++) {
      if (states[i] != FREE) {
        states[i] = FREE;
        vals[i] = null;
      }
    }
  }

  public final void markForReuse() {
    this.markedForReuse = true;
  }

  public final boolean isMarkedForReuse() {
    return this.markedForReuse;
  }

  public final SortDuplicateObserver getDuplicateObserver() {
    return this.duplicateObserver;
  }

  /**
   * Locates the index at which <tt>val</tt> can be inserted. if there is
   * already a value equal()ing <tt>val</tt> in the set and which will be
   * subsumed as part of duplicate elimination by
   * {@link SortDuplicateObserver#eliminateDuplicate(Object, Object)}, then
   * return that index as a negative integer.
   * 
   * @param key
   *          an <code>int</code> key value
   * @param val
   *          an <code>Object</code> value
   * 
   * @return an <code>int</code> value
   */
  protected final int insertionIndex(final int key, final Object val) {
    final int hash, length;
    final byte[] states;
    final int[] set;
    final Object[] values;
    final SortDuplicateObserver duplicateObserver;
    final TObjectHashingStrategy objectHasher;
    int probe, index;
    Object o;

    states = _states;
    set = _set;
    values = _values;
    length = states.length;
    hash = _hashingStrategy.computeHashCode(key) & 0x7fffffff;
    duplicateObserver = this.duplicateObserver;
    objectHasher = this.objectHasher;
    index = hash % length;

    if (states[index] == FREE) {
      return index; // empty, all done
    }
    else if (states[index] == FULL && set[index] == key
        && duplicateObserver != null
        && objectHasher.equals((o = values[index]), val)
        && duplicateObserver.eliminateDuplicate(val, o)) {
      return -index - 1; // already stored
    }
    else { // already FULL or REMOVED, must probe
      // compute the double hash
      probe = 1 + (hash % (length - 2));

      // if the slot we landed on is FULL (but not removed), probe
      // until we find an empty slot, a REMOVED slot, or an element
      // equal to the one we are trying to insert that can subsume this.
      // finding an empty slot means that the value is not present
      // and that we should use that slot as the insertion point;
      // finding a REMOVED slot means that we need to keep searching,
      // however we want to remember the offset of that REMOVED slot
      // so we can reuse it in case a "new" insertion (i.e. not an update)
      // is possible.
      // finding a matching value means that we've found that our desired
      // key is already in the table

      if (states[index] != REMOVED) {
        // starting at the natural offset, probe until we find an
        // offset that isn't full.
        do {
          index -= probe;
          if (index < 0) {
            index += length;
          }
        } while (states[index] == FULL
            && (set[index] != key || duplicateObserver == null
                || !objectHasher.equals((o = values[index]), val)
                || !duplicateObserver.eliminateDuplicate(val, o)));
      }

      // if the index we found was removed: continue probing until we
      // locate a free location or an element which equal()s the
      // one we have.
      if (states[index] == REMOVED) {
        int firstRemoved = index;
        while (states[index] != FREE
            && (states[index] == REMOVED || (set[index] != key
                || duplicateObserver == null
                || !objectHasher.equals((o = values[index]), val)
                || !duplicateObserver.eliminateDuplicate(val, o)))) {
          index -= probe;
          if (index < 0) {
            index += length;
          }
        }
        return states[index] == FULL ? -index - 1 : firstRemoved;
      }
      // if it's full, the key is already stored
      return states[index] == FULL ? -index - 1 : index;
    }
  }

  /**
   * Locates the first index of <tt>key</tt>, <tt>value</tt> pair.
   * 
   * @param key
   *          an <code>int</code> value
   * @param val
   *          an <code>Object</code> value
   * @return the index of <tt>key</tt> + <tt>val</tt> pair, or -1 if it isn't in
   *         the map.
   */
  protected final int index(final int key, final Object val) {
    final int hash, length;
    final int[] set;
    final byte[] states;
    final Object[] vals;
    int probe, index;

    states = _states;
    set = _set;
    vals = _values;
    length = states.length;
    hash = _hashingStrategy.computeHashCode(key) & 0x7fffffff;
    index = hash % length;

    if (states[index] != FREE && (states[index] == REMOVED
        || set[index] != key || vals[index] != val)) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
      } while (states[index] != FREE && (states[index] == REMOVED
          || set[index] != key || vals[index] != val));
    }

    return states[index] == FREE ? -1 : index;
  }

  /**
   * rehashes the map to the new capacity.
   * 
   * @param newCapacity
   *          an <code>int</code> value
   */
  @Override
  protected final void rehash(final int newCapacity) {
    final int[] oldKeys = _set;
    final int oldCapacity = oldKeys.length;
    final Object[] oldVals = _values;
    final byte[] oldStates = _states;

    final int[] newKeys = _set = new int[newCapacity];
    final Object[] newVals = _values = new Object[newCapacity];
    final byte[] newStates = _states = new byte[newCapacity];

    for (int i = oldCapacity; i-- > 0;) {
      if (oldStates[i] == FULL) {
        final int k = oldKeys[i];
        final Object o = oldVals[i];
        final int index = insertionIndex(k, o);
        if (index >= 0) {
          newKeys[index] = k;
          newVals[index] = o;
          newStates[index] = FULL;
        }
      }
    }
  }
}
