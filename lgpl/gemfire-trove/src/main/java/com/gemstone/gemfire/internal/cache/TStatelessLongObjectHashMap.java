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
 * An open addressed Map implementation for long keys and Object values.
 *
 * @author darrel
 */
public class TStatelessLongObjectHashMap extends TStatelessLongHash {
  /** the values of the map */
  protected transient Object[] _values;

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance with the default
   * capacity and load factor.
   */
  public TStatelessLongObjectHashMap(long freeValue) {
    super(freeValue);
  }

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt> and
   * with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public TStatelessLongObjectHashMap(long freeValue, int initialCapacity) {
    super(freeValue, initialCapacity);
  }

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt> and
   * with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public TStatelessLongObjectHashMap(long freeValue, int initialCapacity, float loadFactor) {
    super(freeValue, initialCapacity, loadFactor);
  }

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance with the default
   * capacity and load factor.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongObjectHashMap(long freeValue, TLongHashingStrategy strategy) {
    super(freeValue, strategy);
  }

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance whose capacity
   * is the next highest prime above <tt>initialCapacity + 1</tt>
   * unless that value is already prime.
   *
   * @param initialCapacity an <code>int</code> value
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongObjectHashMap(long freeValue, int initialCapacity, TLongHashingStrategy strategy) {
    super(freeValue, initialCapacity, strategy);
  }

  /**
   * Creates a new <code>TStatelessLongObjectHashMap</code> instance with a prime
   * value at or near the specified capacity and load factor.
   *
   * @param initialCapacity used to find a prime capacity for the table.
   * @param loadFactor used to calculate the threshold over which
   * rehashing takes place.
   * @param strategy used to compute hash codes and to compare keys.
   */
  public TStatelessLongObjectHashMap(long freeValue, int initialCapacity, float loadFactor, TLongHashingStrategy strategy) {
    super(freeValue, initialCapacity, loadFactor, strategy);
  }

  /**
   * @return a deep clone of this collection
   */
  @Override 
    public Object clone() {
    TStatelessLongObjectHashMap m = (TStatelessLongObjectHashMap)super.clone();
    m._values = this._values.clone();
    return m;
  }

  /**
   * @return a TStatelessLongObjectIterator with access to this map's keys and values
   */
  public TStatelessLongObjectIterator iterator() {
    return new TStatelessLongObjectIterator(this);
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
    _values = new Object[capacity];
    return capacity;
  }

  /**
   * Inserts a key/value pair into the map.
   *
   * @param key an <code>long</code> value
   * @param value an <code>Object</code> value
   * @return the previous value associated with <tt>key</tt>,
   * or null if none was found.
   */
  public Object put(long key, Object value) {
    long previousKey;
    Object previous = null;
    int index = insertionIndex(key);
    boolean isNewMapping = true;
    if (index < 0) {
      index = -index -1;
      previous = _values[index];
      isNewMapping = false;
    }
    previousKey = _set[index];
    _set[index] = key;
    _values[index] = value;
    if (isNewMapping) {
      postInsertHook(previousKey == this._FREE);
    }

    return previous;
  }

  /**
   * rehashes the map to the new capacity.
   *
   * @param newCapacity an <code>int</code> value
   */
  @Override 
    protected void rehash(int newCapacity) {
    int oldCapacity = _set.length;
    long oldKeys[] = _set;
    Object oldVals[] = _values;

    _set = new long[newCapacity];
    if (this._FREE != 0) {
      Arrays.fill(_set, this._FREE);
    }
    _values = new Object[newCapacity];

    for (int i = oldCapacity; i-- > 0;) {
      if(oldKeys[i] != this._FREE) {
        long o = oldKeys[i];
        int index = insertionIndex(o);
        _set[index] = o;
        _values[index] = oldVals[i];
      }
    }
  }

  /**
   * retrieves the value for <tt>key</tt>
   *
   * @param key an <code>long</code> value
   * @return the value of <tt>key</tt> or null if no such mapping exists.
   */
  public Object get(long key) {
    int index = index(key);
    return index < 0 ? null : _values[index];
  }

  /**
   * Empties the map.
   *
   */
  @Override 
    public void clear() {
    super.clear();
    long[] keys = _set;
    Object[] vals = _values;

    for (int i = keys.length; i-- > 0;) {
      keys[i] = this._FREE;
      vals[i] = null;
    }
  }

//   /**
//    * Deletes a key/value pair from the map.
//    *
//    * @param key an <code>long</code> value
//    * @return an <code>Object</code> value, or null if no mapping for key exists
//    */
//   public Object remove(long key) {
//     Object prev = null;
//     int index = index(key);
//     if (index >= 0) {
//       prev = _values[index];
//       removeAt(index);    // clear key,state; adjust size
//     }
//     return prev;
//   }

  /**
   * Compares this map with another map for equality of their stored
   * entries.
   *
   * @param other an <code>Object</code> value
   * @return a <code>boolean</code> value
   */
  @Override 
    public boolean equals(Object other) {
    if (! (other instanceof TStatelessLongObjectHashMap)) {
      return false;
    }
    TStatelessLongObjectHashMap that = (TStatelessLongObjectHashMap)other;
    if (that.size() != this.size()) {
      return false;
    }
    return forEachEntry(new EqProcedure(that));
  }

  @Override 
    public int hashCode() {
    HashProcedure p = new HashProcedure();
    forEachEntry(p);
    return p.getHashCode();
  }	

  protected final class HashProcedure implements TLongObjectProcedure {
    private int h = 0;
        
    public int getHashCode() {
      return h;
    }
        
    public final boolean execute(long key, Object value) {
      h += (_hashingStrategy.computeHashCode(key) ^ HashFunctions.hash(value));
      return true;
    }
  }

  private static final class EqProcedure implements TLongObjectProcedure {
    private final TStatelessLongObjectHashMap _otherMap;

    EqProcedure(TStatelessLongObjectHashMap otherMap) {
      _otherMap = otherMap;
    }

    public final boolean execute(long key, Object value) {
      int index = _otherMap.index(key);
      if (index >= 0 && eq(value, _otherMap.get(key))) {
        return true;
      }
      return false;
    }

    /**
     * Compare two objects for equality.
     */
    private final boolean eq(Object o1, Object o2) {
      return o1 == o2 || ((o1 != null) && o1.equals(o2));
    }

  }

//   /**
//    * removes the mapping at <tt>index</tt> from the map.
//    *
//    * @param index an <code>int</code> value
//    */
//   @Override 
//     protected void removeAt(int index) {
//     super.removeAt(index);  // clear key, state; adjust size
//     _values[index] = null;
//   }

  /**
   * Returns the values of the map.
   *
   * @return a <code>Collection</code> value
   */
  public Object[] getValues() {
    Object[] vals = new Object[size()];
    Object[] v = _values;
    long[] keys = _set;

    for (int i = v.length, j = 0; i-- > 0;) {
      if (keys[i] != this._FREE) {
        vals[j++] = v[i];
      }
    }
    return vals;
  }

  /**
   * returns the keys of the map.
   *
   * @return a <code>Set</code> value
   */
  public long[] keys() {
    long[] keys = new long[size()];
    long[] k = _set;

    for (int i = k.length, j = 0; i-- > 0;) {
      if (k[i] != this._FREE) {
        keys[j++] = k[i];
      }
    }
    return keys;
  }

  /**
   * checks for the presence of <tt>val</tt> in the values of the map.
   *
   * @param val an <code>Object</code> value
   * @return a <code>boolean</code> value
   */
  public boolean containsValue(Object val) {
    long[] keys = _set;
    Object[] vals = _values;

    // special case null values so that we don't have to
    // perform null checks before every call to equals()
    if (null == val) {
      for (int i = vals.length; i-- > 0;) {
        if (keys[i] != this._FREE &&
            val == vals[i]) {
          return true;
        }
      }
    } else {
      for (int i = vals.length; i-- > 0;) {
        if (keys[i] != this._FREE &&
            (val == vals[i] || val.equals(vals[i]))) {
          return true;
        }
      }
    } // end of else
    return false;
  }


  /**
   * checks for the present of <tt>key</tt> in the keys of the map.
   *
   * @param key an <code>long</code> value
   * @return a <code>boolean</code> value
   */
  public boolean containsKey(long key) {
    return contains(key);
  }

  /**
   * Executes <tt>procedure</tt> for each key in the map.
   *
   * @param procedure a <code>TLongProcedure</code> value
   * @return false if the loop over the keys terminated because
   * the procedure returned false for some key.
   */
  public boolean forEachKey(TLongProcedure procedure) {
    return forEach(procedure);
  }

  /**
   * Executes <tt>procedure</tt> for each value in the map.
   *
   * @param procedure a <code>TObjectProcedure</code> value
   * @return false if the loop over the values terminated because
   * the procedure returned false for some value.
   */
  public boolean forEachValue(TObjectProcedure procedure) {
    long[] keys = _set;
    Object[] values = _values;
    for (int i = values.length; i-- > 0;) {
      if (keys[i] != this._FREE && ! procedure.execute(values[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Executes <tt>procedure</tt> for each key/value entry in the
   * map.
   *
   * @param procedure a <code>TOLongObjectProcedure</code> value
   * @return false if the loop over the entries terminated because
   * the procedure returned false for some entry.
   */
  public boolean forEachEntry(TLongObjectProcedure procedure) {
    long[] keys = _set;
    Object[] values = _values;
    for (int i = keys.length; i-- > 0;) {
      if (keys[i] != this._FREE && ! procedure.execute(keys[i],values[i])) {
        return false;
      }
    }
    return true;
  }

//   /**
//    * Retains only those entries in the map for which the procedure
//    * returns a true value.
//    *
//    * @param procedure determines which entries to keep
//    * @return true if the map was modified.
//    */
//   public boolean retainEntries(TLongObjectProcedure procedure) {
//     boolean modified = false;
//     long[] keys = _set;
//     Object[] values = _values;
//     for (int i = keys.length; i-- > 0;) {
//       if (keys[i] != this._FREE && ! procedure.execute(keys[i],values[i])) {
//         removeAt(i);
//         modified = true;
//       }
//     }
//     return modified;
//   }

  /**
   * Transform the values in this map using <tt>function</tt>.
   *
   * @param function a <code>TObjectFunction</code> value
   */
  public void transformValues(TObjectFunction function) {
    long[] keys = _set;
    Object[] values = _values;
    for (int i = values.length; i-- > 0;) {
      if (_set[i] != this._FREE) {
        values[i] = function.execute(values[i]);
      }
    }
  }

} // TStatelessLongObjectHashMap
