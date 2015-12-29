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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gnu.trove.HashingStats;
import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * A specialized extension of {@link THashMap} that allows for having a pair
 * of values as a key instead of a single object key. This avoids having to
 * create an object for every pair in the key and instead stores the second
 * key column in its own array much like the first column, thus reducing the
 * memory overhead significantly.
 * 
 * @author swale
 * @since 7.0
 */
public final class THashMapWithKeyPair extends THashMap {

  private static final long serialVersionUID = 6832841924272527019L;

  /**
   * Factory to create value object for
   * {@link THashMapWithKeyPair#create(Object, Object, ValueCreator)}.
   */
  public interface ValueCreator {
    Object create(Object key1, Object key2);
    Object update(Object key1, Object key2, Object oldValue);
  }

  /** the set of part2 of the keys */
  protected transient Object[] _set2;

  /**
   * Creates a new <code>THashMapWithKeyPair</code> instance with the default
   * capacity and load factor.
   */
  public THashMapWithKeyPair() {
    super();
  }

  /**
   * Creates a new <code>THashMapWithKeyPair</code> instance with the default
   * capacity and load factor.
   * 
   * @param strategy
   *          used to compute hash codes and to compare objects.
   */
  public THashMapWithKeyPair(TObjectHashingStrategy strategy,
      HashingStats stats) {
    super(strategy);
    setHashingStats(stats);
  }

  /**
   * Creates a new <code>THashMapWithKeyPair</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt> and with the
   * specified load factor.
   * 
   * @param initialCapacity
   *          an <code>int</code> value
   * @param loadFactor
   *          a <code>float</code> value
   * @param strategy
   *          used to compute hash codes and to compare objects.
   */
  public THashMapWithKeyPair(int initialCapacity, float loadFactor,
      TObjectHashingStrategy strategy, HashingStats stats) {
    super(initialCapacity, loadFactor, strategy);
    setHashingStats(stats);
  }

  /**
   * retrieves the value for <tt>key1,key2</tt>
   * 
   * @param key1
   *          part1 of the key to be inserted
   * @param key2
   *          part2 of the key to be inserted
   * 
   * @return the value of <tt>key1,key2</tt> or null if no such mapping
   *         exists.
   */
  public Object get(final Object key1, final Object key2) {
    int index = index(key1, key2);
    return index >= 0 ? _values[index] : null;
  }

  /**
   * Inserts a key1+key2/value pair into the map.
   * 
   * @param key1
   *          part1 of the key to be inserted
   * @param key2
   *          part2 of the key to be inserted
   * @param value
   *          an <code>Object</code> value
   * 
   * @return the previous value associated with <tt>key1,key2</tt>, or null if
   *         none was found.
   */
  public final Object put(final Object key1, final Object key2,
      final Object value) {
    if (null != key1 && null != key2) {
      Object previous = null;
      Object oldKey;
      int index = insertionIndex(key1, key2);
      if (index < 0) {
        index = -index - 1;
        previous = _values[index];
      }
      oldKey = _set[index];
      _set[index] = key1;
      _set2[index] = key2;
      _values[index] = value;
      if (null == previous) {
        postInsertHook(oldKey == null);
      }

      return previous;
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Inserts a key1+key2/value pair into the map if the key does not already
   * exist creating the value using the provided factory only in that case.
   * 
   * @param key1
   *          part1 of the key to be inserted
   * @param key2
   *          part2 of the key to be inserted
   * @param valueCreator
   *          an {@link ValueCreator} instance
   * 
   * @return the previous value associated with <tt>key</tt>, or the newly
   *         created one
   */
  public Object create(final Object key1, final Object key2,
      ValueCreator valueCreator) {
    if (null != key1 && null != key2) {
      final Object oldKey, value, oldValue;
      final int index = insertionIndex(key1, key2);
      if (index >= 0) {
        oldKey = _set[index];
        _set[index] = key1;
        _set2[index] = key2;
        value = valueCreator.create(key1, key2);
        _values[index] = value;
        postInsertHook(oldKey == null);
        return value;
      }
      else {
        oldValue = _values[-index - 1];
        value = valueCreator.update(key1, key2, oldValue);
        if (value != oldValue) {
          _values[-index - 1] = value;
        }
        return value;
      }
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Inserts a key1+key2/value pair into the map, if the key does not already
   * exist returning null else returns the existing value.
   * 
   * @param key1
   *          part1 of the key to be inserted
   * @param key2
   *          part2 of the key to be inserted
   * @param value
   *          an <code>Object</code> value
   * 
   * @return the previous value associated with <tt>key</tt>, or null if the
   *         given value was inserted
   */
  public final Object putIfAbsent(final Object key1, final Object key2,
      final Object value) {
    if (null != key1 && null != key2) {
      return putIfAbsent(key1, key2, value, insertionIndex(key1, key2));
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Inserts a key1+key2/value pair into the map at given index determined by
   * {@link #insertionIndex(Object, Object)}, if the key does not already exist
   * returning null else returns the existing value.
   * 
   * @param key1
   *          part1 of the key to be inserted
   * @param key2
   *          part2 of the key to be inserted
   * @param value
   *          an <code>Object</code> value
   * @param index
   *          the result of {@link #insertionIndex(Object, Object)}
   * 
   * @return the previous value associated with <tt>key</tt>, or null if the
   *         given value was inserted
   */
  public final Object putIfAbsent(final Object key1, final Object key2,
      final Object value, final int index) {
    if (index >= 0) {
      final Object oldKey = _set[index];
      _set[index] = key1;
      _set2[index] = key2;
      _values[index] = value;
      postInsertHook(oldKey == null);
      return null;
    }
    else {
      return _values[-index - 1];
    }
  }

  /**
   * checks for the presence of <tt>key1, key2</tt> in the keys of the map.
   * 
   * @param key1
   *          part1 of the key object
   * @param key2
   *          part2 of the key object
   * 
   * @return a <code>boolean</code> value
   */
  public boolean containsKey(final Object key1, final Object key2) {
    return index(key1, key2) >= 0;
  }

  /**
   * Deletes a key1+key2/value pair from the map.
   * 
   * @param key1
   *          part1 of the key object
   * @param key2
   *          part2 of the key object
   *          
   * @return an <code>Object</code> value
   */
  public Object removeKeyPair(final Object key1, final Object key2) {
    if (null != key1 && null != key2) {
      Object prev = null;
      final int index = index(key1, key2);
      if (index >= 0) {
        prev = _values[index];
        removeAt(index); // clear key,state; adjust size
      }
      return prev;
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Executes <tt>procedure</tt> for each key1+key2/value entry in the map.
   * 
   * @param procedure
   *          a <code>TObjectObjectObjectProcedure</code> value
   * @return false if the loop over the entries terminated because the
   *         procedure returned false for some entry.
   */
  public boolean forEachEntry(final TObjectObjectObjectProcedure procedure) {
    final Object[] keys1 = _set;
    final Object[] keys2 = _set2;
    final Object[] values = _values;
    for (int i = keys1.length; i-- > 0;) {
      if (keys1[i] != null && keys1[i] != REMOVED && keys2[i] != null
          && !procedure.execute(keys1[i], keys2[i], values[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return a shallow clone of this collection
   */
  @Override
  public Object clone() {
    THashMapWithKeyPair m = (THashMapWithKeyPair)super.clone();
    m._set2 = this._set2.clone();
    return m;
  }

  /**
   * initializes the keys and values of this hash table.
   * 
   * @param initialCapacity
   *          an <code>int</code> value
   * @return an <code>int</code> value
   */
  @Override
  protected int setUp(int initialCapacity) {
    final int capacity = super.setUp(initialCapacity);
    _set2 = new Object[capacity];
    return capacity;
  }

  /**
   * rehashes the map to the new capacity.
   * 
   * @param newCapacity
   *          an <code>int</code> value
   */
  @Override
  // GemStoneAddition
  protected void rehash(int newCapacity) {
    final int oldCapacity = _set.length;
    final Object[] oldKeys1 = _set;
    final Object[] oldKeys2 = _set2;
    final Object[] oldVals = _values;

    final Object[] newKeys1 = _set = new Object[newCapacity];
    final Object[] newKeys2 = _set2 = new Object[newCapacity];
    final Object[] newVals = _values = new Object[newCapacity];

    for (int i = oldCapacity; i-- > 0;) {
      if (oldKeys1[i] != null && oldKeys1[i] != REMOVED) {
        final Object k1 = oldKeys1[i];
        final Object k2 = oldKeys2[i];
        int index = insertionIndex(k1, k2);
        if (index < 0) {
          throwObjectContractViolation(newKeys1[(-index - 1)], k1);
        }
        newKeys1[index] = k1;
        newKeys2[index] = k2;
        newVals[index] = oldVals[i];
      }
    }
  }

  @Override
  protected final int index(Object key) {
    throw new AssertionError("not expected to be invoked");
  }

  @Override
  protected final int insertionIndex(Object key) {
    throw new AssertionError("not expected to be invoked");
  }

  /**
   * Locates the index of <tt>key1, key2</tt>.
   * 
   * @param key1
   *          part1 of the key object
   * @param key2
   *          part2 of the key object
   * 
   * @return the index of <tt>key1, key2</tt> or -1 if it isn't in the set.
   */
  protected final int index(final Object key1, final Object key2) {
    int hash, probe, index;
    Object cur1, cur2;

    final Object[] set1 = _set;
    final Object[] set2 = _set2;
    final int length = set1.length;
    final TObjectHashingStrategy hashingStrategy = _hashingStrategy;
    hash = hashingStrategy.computeHashCode(key1) & 0x7fffffff;
    hash ^= hashingStrategy.computeHashCode(key2) & 0x7fffffff;
    index = hash % length;
    cur1 = set1[index];
    cur2 = set2[index];

    if ((cur1 != null
        && (cur1 == REMOVED || !hashingStrategy.equals(cur1, key1)))
        || (cur2 != null && !hashingStrategy.equals(cur2, key2))) {
      long start = -1L;
      if (this.stats != null) {
        start = this.stats.getNanoTime();
      }

      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
        cur1 = set1[index];
        cur2 = set2[index];
      } while ((cur1 != null
          && (cur1 == REMOVED || !hashingStrategy.equals(cur1, key1)))
          || (cur2 != null && !hashingStrategy.equals(cur2, key2)));
      if (this.stats != null) {
        this.stats.endQueryResultsHashCollisionProbe(start);
      }
    }

    return cur1 == null ? -1 : index;
  }

  /**
   * Locates the index at which <tt>key1, key2</tt> can be inserted. if there
   * is already a value equal()ing <tt>key1, key2</tt> in the set, returns
   * that value's index as <tt>-index - 1</tt>.
   * 
   * @param key1
   *          part1 of the key object
   * @param key2
   *          part2 of the key object
   * 
   * @return the index of a FREE slot at which key pair can be inserted or, if
   *         key pair is already stored in the hash, the negative value of
   *         that index, minus 1: -index -1.
   */
  public int insertionIndex(final Object key1, final Object key2) {
    int hash, probe, index;
    Object cur1, cur2;

    final Object[] set1 = _set;
    final Object[] set2 = _set2;
    final int length = set1.length;
    final TObjectHashingStrategy hashingStrategy = _hashingStrategy;
    hash = hashingStrategy.computeHashCode(key1) & 0x7fffffff;
    hash ^= hashingStrategy.computeHashCode(key2) & 0x7fffffff;
    index = hash % length;
    cur1 = set1[index];
    cur2 = set2[index];

    if (cur1 == null && cur2 == null) {
      return index; // empty, all done
    }
    else if (hashingStrategy.equals(cur1, key1)
        && hashingStrategy.equals(cur2, key2)) {
      return -index - 1; // already stored
    }
    else { // already FULL or REMOVED, must probe
      long start = -1L;
      if (this.stats != null) {
        start = this.stats.getNanoTime();
        this.stats.incQueryResultsHashCollisions();
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
      if (cur1 != REMOVED) {
        // starting at the natural offset, probe until we find an
        // offset that isn't full.
        do {
          index -= probe;
          if (index < 0) {
            index += length;
          }
          cur1 = set1[index];
          cur2 = set2[index];
        } while ((cur1 != null && cur1 != REMOVED
            && !hashingStrategy.equals(cur1, key1))
            || (cur2 != null && !hashingStrategy.equals(cur2, key2)));
      }

      // if the index we found was removed: continue probing until we
      // locate a free location or an element which equal()s the
      // one we have.
      if (cur1 == REMOVED) {
        int firstRemoved = index;
        while ((cur1 != null
            && (cur1 == REMOVED || !hashingStrategy.equals(cur1, key1)))
            || (cur2 != null && !hashingStrategy.equals(cur2, key2))) {
          index -= probe;
          if (index < 0) {
            index += length;
          }
          cur1 = set1[index];
          cur2 = set2[index];
        }
        if (this.stats != null) {
          this.stats.endQueryResultsHashCollisionProbe(start);
        }
        return (cur1 != null && cur1 != REMOVED) ? -index - 1 : firstRemoved;
      }
      if (this.stats != null) {
        this.stats.endQueryResultsHashCollisionProbe(start);
      }
      // if it's full, the key is already stored
      return (cur1 != null && cur1 != REMOVED) ? -index - 1 : index;
    }
  }

  /**
   * removes the mapping at <tt>index</tt> from the map.
   * 
   * @param index
   *          an <code>int</code> value
   */
  @Override
  protected void removeAt(int index) {
    super.removeAt(index); // clear key, value, state; adjust size
    _set2[index] = null;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    toString(sb);
    return sb.toString();
  }

  public void toString(final StringBuilder sb) {
    sb.append('{');
    final Object[] keys1 = _set;
    final Object[] keys2 = _set2;
    final Object[] values = _values;
    int index = keys1.length;
    boolean notFirst = false;
    while (--index > 0) {
      Object key1 = keys1[index];
      Object key2 = keys2[index];
      Object value = values[index];
      if (key1 != null && key1 != REMOVED) {
        if (notFirst) {
          sb.append(", ");
        }
        else {
          notFirst = true;
        }
        sb.append(key1 != this ? key1 : "(this Map)");
        sb.append(':');
        sb.append(key2 != this ? key2 : "(this Map)");
        sb.append('=');
        sb.append(value != this ? value : "(this Map)");
      }
    }
    sb.append('}');
  }
}
