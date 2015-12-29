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

import com.gemstone.gnu.trove.*;
import java.util.Arrays;

/**
 * An open addressed set implementation for long primitives.
 *
 * @author darrel
 */
public class TStatelessLongHashSet extends TStatelessLongHash {
  /**
   * Creates a new <code>TStatelessLongHashSet</code> instance with the default
   * capacity and load factor.
   */
  public TStatelessLongHashSet(long freeValue) {
    super(freeValue);
  }

  /**
   * Creates a new <code>TStatelessLongHashSet</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt> and
   * with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public TStatelessLongHashSet(long freeValue, int initialCapacity) {
    super(freeValue, initialCapacity);
  }

  /**
   * Creates a new <code>TStatelessLongHashSet</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt> and
   * with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public TStatelessLongHashSet(long freeValue, int initialCapacity, float loadFactor) {
    super(freeValue, initialCapacity, loadFactor);
  }

  /**
   * Creates a new <code>TStatelessLongHashSet</code> instance containing the
   * elements of <tt>array</tt>.
   *
   * @param array an array of <code>long</code> primitives
   */
  public TStatelessLongHashSet(long freeValue, long[] array) {
    this(freeValue, array.length);
    addAll(array);
  }

  /**
   * Creates a new <code>TStatelessLongHash</code> instance with the default
   * capacity and load factor.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongHashSet(long freeValue, TLongHashingStrategy strategy) {
    super(freeValue, strategy);
  }

  /**
   * Creates a new <code>TStatelessLongHash</code> instance whose capacity
   * is the next highest prime above <tt>initialCapacity + 1</tt>
   * unless that value is already prime.
   *
   * @param initialCapacity an <code>int</code> value
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongHashSet(long freeValue, int initialCapacity, TLongHashingStrategy strategy) {
    super(freeValue, initialCapacity, strategy);
  }

  /**
   * Creates a new <code>TStatelessLongHash</code> instance with a prime
   * value at or near the specified capacity and load factor.
   *
   * @param initialCapacity used to find a prime capacity for the table.
   * @param loadFactor used to calculate the threshold over which
   * rehashing takes place.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongHashSet(long freeValue, int initialCapacity, float loadFactor, TLongHashingStrategy strategy) {
    super(freeValue, initialCapacity, loadFactor, strategy);
  }

  /**
   * Creates a new <code>TStatelessLongHashSet</code> instance containing the
   * elements of <tt>array</tt>.
   *
   * @param array an array of <code>long</code> primitives
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongHashSet(long freeValue, long[] array, TLongHashingStrategy strategy) {
    this(freeValue, array.length, strategy);
    addAll(array);
  }

  /**
   * @return a TLongIterator with access to the values in this set
   */
  public TStatelessLongIterator iterator() {
    return new TStatelessLongIterator(this);
  }

  /**
   * Inserts a value into the set.
   *
   * @param val an <code>long</code> value
   * @return true if the set was modified by the add operation
   */
  public boolean add(long val) {
    int index = insertionIndex(val);

    if (index < 0) {
      return false;       // already present in set, nothing to add
    }

    long previousState = _set[index];
    _set[index] = val;
    postInsertHook(previousState == this._FREE);
        
    return true;            // yes, we added something
  }

  /**
   * Expands the set to accomodate new values.
   *
   * @param newCapacity an <code>int</code> value
   */
  @Override 
    protected void rehash(int newCapacity) {
    int oldCapacity = _set.length;
    long oldSet[] = _set;

    _set = new long[newCapacity];
    if (this._FREE != 0) {
      Arrays.fill(_set, this._FREE);
    }

    for (int i = oldCapacity; i-- > 0;) {
      long o = oldSet[i];
      if (o != this._FREE) {
        int index = insertionIndex(o);
        _set[index] = o;
      }
    }
  }

  /**
   * Returns a new array containing the values in the set.
   *
   * @return an <code>long[]</code> value
   */
  public long[] toArray() {
    long[] result = new long[size()];
    long[] set = _set;
        
    for (int i = set.length, j = 0; i-- > 0;) {
      long o = set[i];
      if (o != this._FREE) {
        result[j++] = o;
      }
    }
    return result;
  }

  /**
   * Empties the set.
   */
  @Override 
    public void clear() {
    super.clear();
    java.util.Arrays.fill(_set, this._FREE);
  }

  /**
   * Compares this set with another set for equality of their stored
   * entries.
   *
   * @param other an <code>Object</code> value
   * @return a <code>boolean</code> value
   */
  @Override 
    public boolean equals(Object other) {
    if (! (other instanceof TStatelessLongHashSet)) {
      return false;
    }
    final TStatelessLongHashSet that = (TStatelessLongHashSet)other;
    if (that.size() != this.size()) {
      return false;
    }
    return forEach(new TLongProcedure() {
        public final boolean execute(long value) {
          return that.contains(value);
        }
      });
  }

  @Override 
    public int hashCode() {
    HashProcedure p = new HashProcedure();
    forEach(p);
    return p.getHashCode();
  }

  protected final class HashProcedure implements TLongProcedure {
    private int h = 0;
        
    public int getHashCode() {
      return h;
    }
        
    public final boolean execute(long key) {
      h += _hashingStrategy.computeHashCode(key);
      return true;
    }
  }

//   /**
//    * Removes <tt>val</tt> from the set.
//    *
//    * @param val an <code>long</code> value
//    * @return true if the set was modified by the remove operation.
//    */
//   public boolean remove(long val) {
//     int index = index(val);
//     if (index >= 0) {
//       removeAt(index);
//       return true;
//     }
//     return false;
//   }

  /**
   * Tests the set to determine if all of the elements in
   * <tt>array</tt> are present.
   *
   * @param array an <code>array</code> of long primitives.
   * @return true if all elements were present in the set.
   */
  public boolean containsAll(long[] array) {
    for (int i = array.length; i-- > 0;) {
      if (! contains(array[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds all of the elements in <tt>array</tt> to the set.
   *
   * @param array an <code>array</code> of long primitives.
   * @return true if the set was modified by the add all operation.
   */
  public boolean addAll(long[] array) {
    boolean changed = false;
    for (int i = array.length; i-- > 0;) {
      if (add(array[i])) {
        changed = true;
      }
    }
    return changed;
  }

//   /**
//    * Removes all of the elements in <tt>array</tt> from the set.
//    *
//    * @param array an <code>array</code> of long primitives.
//    * @return true if the set was modified by the remove all operation.
//    */
//   public boolean removeAll(long[] array) {
//     boolean changed = false;
//     for (int i = array.length; i-- > 0;) {
//       if (remove(array[i])) {
//         changed = true;
//       }
//     }
//     return changed;
//   }

//   /**
//    * Removes any values in the set which are not contained in
//    * <tt>array</tt>.
//    *
//    * @param array an <code>array</code> of long primitives.
//    * @return true if the set was modified by the retain all operation
//    */
//   public boolean retainAll(long[] array) {
//     boolean changed = false;
//     Arrays.sort(array);
//     long[] set = _set;

//     for (int i = set.length; i-- > 0;) {
//       if (set[i] != this._FREE && (Arrays.binarySearch(array,set[i]) < 0)) {
//         remove(set[i]);
//         changed = true;
//       }
//     }
//     return changed;
//   }

} // TStatelessLongHashSet
