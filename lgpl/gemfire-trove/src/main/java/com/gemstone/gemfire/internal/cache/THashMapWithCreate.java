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

import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * Extends THashMap adding putIfAbsent method.
 * 
 * @author swale
 * @since 7.0
 */
public final class THashMapWithCreate extends THashMap {

  private static final long serialVersionUID = 2440644010877735505L;

  /**
   * Factory to create value object for
   * {@link THashMapWithCreate#create(Object, ValueCreator, Object)}.
   */
  public static interface ValueCreator {
    Object create(Object key, Object params);
  }

  /**
   * Creates a new <code>THashMapWithCreate</code> instance with the default
   * capacity and load factor.
   */
  public THashMapWithCreate() {
    super();
  }

  /**
   * Creates a new <code>THashMapWithCreate</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt>.
   * 
   * @param initialCapacity
   *          an <code>int</code> value
   */
  public THashMapWithCreate(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Creates a new <code>THashMapWithCreate</code> instance with a prime
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
  public THashMapWithCreate(int initialCapacity, float loadFactor,
      TObjectHashingStrategy strategy) {
    super(initialCapacity, loadFactor, strategy);
  }

  /**
   * Inserts a key/value pair into the map if the key does not already exist.
   * 
   * @param key
   *          an <code>Object</code> value
   * @param value
   *          an <code>Object</code> value
   * @return the previous value associated with <tt>key</tt>, or null if none
   *         was found.
   */
  public Object putIfAbsent(Object key, Object value) {
    if (null != key) {
      final Object oldKey;
      final int index = insertionIndex(key);
      if (index >= 0) {
        oldKey = _set[index];
        _set[index] = key;
        _values[index] = value;
        postInsertHook(oldKey == null);
        return null;
      }
      else {
        return _values[-index - 1];
      }
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Inserts a key/value pair into the map if the key does not already exist
   * creating the value using the provided factory only in that case.
   * 
   * @param key
   *          an <code>Object</code> value
   * @param valueCreator
   *          an {@link ValueCreator} instance
   * @param params
   *          any parameters to be passed to {@link ValueCreator#create} method
   * 
   * @return the previous value associated with <tt>key</tt>, or null
   */
  public Object putIfAbsent(final Object key, final ValueCreator valueCreator,
      final Object params) {
    if (key != null) {
      final Object oldKey, value;
      final int index = insertionIndex(key);
      if (index >= 0) {
        oldKey = _set[index];
        _set[index] = key;
        value = valueCreator.create(key, params);
        _values[index] = value;
        postInsertHook(oldKey == null);
        return null;
      }
      else {
        return _values[-index - 1];
      }
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }

  /**
   * Inserts a key/value pair into the map if the key does not already exist
   * creating the value using the provided factory only in that case.
   * 
   * @param key
   *          an <code>Object</code> value
   * @param valueCreator
   *          an {@link ValueCreator} instance
   * @param params
   *          any parameters to be passed to {@link ValueCreator#create} method
   * 
   * @return the previous value associated with <tt>key</tt>, or the newly
   *         created one
   */
  public Object create(Object key, ValueCreator valueCreator, Object params) {
    if (null != key) {
      final Object oldKey, value;
      final int index = insertionIndex(key);
      if (index >= 0) {
        oldKey = _set[index];
        _set[index] = key;
        value = valueCreator.create(key, params);
        _values[index] = value;
        postInsertHook(oldKey == null);
        return value;
      }
      else {
        return _values[-index - 1];
      }
    }
    else {
      throw new NullPointerException("null keys not supported");
    }
  }
}
