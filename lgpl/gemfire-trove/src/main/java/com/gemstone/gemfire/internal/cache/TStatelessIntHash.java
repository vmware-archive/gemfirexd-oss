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
 * An open addressed hashing implementation for int primitives.
 *
 * @author darrel
 */

abstract public class TStatelessIntHash extends TStatelessPrimitiveHash
  implements TIntHashingStrategy {
  /** the set of ints */
  protected transient int[] _set;

  protected /*final*/ int _FREE;
  
  /** strategy used to hash values in this collection */
  protected TIntHashingStrategy _hashingStrategy;

  /**
   * Creates a new <code>TStatelessIntHash</code> instance with the default
   * capacity and load factor.
   */
  public TStatelessIntHash(int freeValue) {
    super();
    this._hashingStrategy = this;
    this._FREE = freeValue;
  }

  /**
   * Creates a new <code>TStatelessIntHash</code> instance whose capacity
   * is the next highest prime above <tt>initialCapacity + 1</tt>
   * unless that value is already prime.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public TStatelessIntHash(int freeValue, int initialCapacity) {
    super(initialCapacity);
    this._hashingStrategy = this;
    this._FREE = freeValue;
  }

  /**
   * Creates a new <code>TStatelessIntHash</code> instance with a prime
   * value at or near the specified capacity and load factor.
   *
   * @param initialCapacity used to find a prime capacity for the table.
   * @param loadFactor used to calculate the threshold over which
   * rehashing takes place.
   */
  public TStatelessIntHash(int freeValue, int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
    this._hashingStrategy = this;
    this._FREE = freeValue;
  }

  /**
   * Creates a new <code>TStatelessIntHash</code> instance with the default
   * capacity and load factor.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessIntHash(int freeValue, TIntHashingStrategy strategy) {
    super();
    this._hashingStrategy = strategy;
    this._FREE = freeValue;
  }

  /**
   * Creates a new <code>TStatelessIntHash</code> instance whose capacity
   * is the next highest prime above <tt>initialCapacity + 1</tt>
   * unless that value is already prime.
   *
   * @param initialCapacity an <code>int</code> value
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessIntHash(int freeValue, int initialCapacity, TIntHashingStrategy strategy) {
    super(initialCapacity);
    this._hashingStrategy = strategy;
    this._FREE = freeValue;
  }

  /**
   * Creates a new <code>TStatelessIntHash</code> instance with a prime
   * value at or near the specified capacity and load factor.
   *
   * @param initialCapacity used to find a prime capacity for the table.
   * @param loadFactor used to calculate the threshold over which
   * rehashing takes place.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessIntHash(int freeValue, int initialCapacity, float loadFactor, TIntHashingStrategy strategy) {
    super(initialCapacity, loadFactor);
    this._hashingStrategy = strategy;
    this._FREE = freeValue;
  }

  public int getFreeValue() {
    return this._FREE;
  }
  
  /**
   * @return a deep clone of this collection
   */
  @Override
    public Object clone() {
    TStatelessIntHash h = (TStatelessIntHash)super.clone();
    h._set = this._set.clone();
    h._FREE = this._FREE;
    return h;
  }

  /**
   * Returns the capacity of the hash table.  This is the true
   * physical capacity, without adjusting for the load factor.
   *
   * @return the physical capacity of the hash table.
   */
  @Override
    protected int capacity() {
    return _set.length;
  }

  /**
   * initializes the hashtable to a prime capacity which is at least
   * <tt>initialCapacity + 1</tt>.  
   *
   * @param initialCapacity an <code>int</code> value
   * @return the actual capacity chosen
   */
  @Override
    protected int setUp(int initialCapacity) {
    int capacity;

    capacity = super.setUp(initialCapacity);
    _set = new int[capacity];
    if (this._FREE != 0) {
      Arrays.fill(_set, this._FREE);
    }
    return capacity;
  }
    
  /**
   * Searches the set for <tt>val</tt>
   *
   * @param val an <code>int</code> value
   * @return a <code>boolean</code> value
   */
  public boolean contains(int val) {
    return index(val) >= 0;
  }

  /**
   * Executes <tt>procedure</tt> for each element in the set.
   *
   * @param procedure a <code>TObjectProcedure</code> value
   * @return false if the loop over the set terminated because
   * the procedure returned false for some value.
   */
  public boolean forEach(TIntProcedure procedure) {
    int[] set = _set;
    for (int i = set.length; i-- > 0;) {
      if (set[i] != this._FREE && ! procedure.execute(set[i])) {
        return false;
      }
    }
    return true;
  }

//   /**
//    * Releases the element currently stored at <tt>index</tt>.
//    *
//    * @param index an <code>int</code> value
//    */
//   @Override
//     protected void removeAt(int index) {
//     super.removeAt(index);
//     _set[index] = this._FREE;
//   }

  /**
   * Locates the index of <tt>val</tt>.
   *
   * @param val an <code>int</code> value
   * @return the index of <tt>val</tt> or -1 if it isn't in the set.
   */
  protected int index(int val) {
    int hash, probe, index, length;
    int[] set;

    if (val == this._FREE) {
      return -1;
    }
    set = _set;
    length = set.length;
    hash = _hashingStrategy.computeHashCode(val) & 0x7fffffff;
    index = hash % length;

    if (set[index] != this._FREE && set[index] != val) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
      } while (set[index] != this._FREE && set[index] != val);
    }

    return set[index] == this._FREE ? -1 : index;
  }

  /**
   * Locates the index at which <tt>val</tt> can be inserted.  if
   * there is already a value equal()ing <tt>val</tt> in the set,
   * returns that value as a negative integer.
   *
   * @param val an <code>int</code> value
   * @return an <code>int</code> value
   */
  protected int insertionIndex(int val) {
    int hash, probe, index, length;
    int[] set;

    if (val == this._FREE) {
      throw new IllegalArgumentException("can not add the value " + val);
    }
    set = _set;
    length = set.length;
    hash = _hashingStrategy.computeHashCode(val) & 0x7fffffff;
    index = hash % length;

    if (set[index] == this._FREE) {
      return index;       // empty, all done
    } else if (set[index] == val) {
      return -index -1;   // already stored
    } else {                // already FULL or REMOVED, must probe
      // compute the double hash
      probe = 1 + (hash % (length - 2));

      // if the slot we landed on is not FREE, probe
      // until we find an empty slot or an element
      // equal to the one we are trying to insert.       
      // finding an empty slot means that the value is not present
      // and that we should use that slot as the insertion point;
      // finding a matching value means that we've found that our desired
      // key is already in the table

      // starting at the natural offset, probe until we find an
      // offset that isn't full.
      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
      } while (set[index] != this._FREE && set[index] != val);

      // if it's not free, the key is already stored
      return set[index] != this._FREE ? -index -1 : index;
    }
  }

  /**
   * Default implementation of TIntHashingStrategy:
   * delegates hashing to HashFunctions.hash(int).
   *
   * @param val the value to hash
   * @return the hashcode.
   */
  public final int computeHashCode(int val) {
    return HashFunctions.hash(val);
  }
} // TStatelessIntHash
