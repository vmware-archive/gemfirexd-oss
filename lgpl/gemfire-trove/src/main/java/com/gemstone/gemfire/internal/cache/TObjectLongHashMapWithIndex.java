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

import com.gemstone.gnu.trove.TObjectLongHashMap;

/**
 * A simple extension to {@link TObjectLongHashMap} that allows to get the
 * index of value, and value from index directly. This avoids having to do a
 * contains() + get() to determine a valid value in the map.
 * 
 * @author swale
 * @since 7.0
 */
public final class TObjectLongHashMapWithIndex extends
    TObjectLongHashMap {

  private static final long serialVersionUID = -2831843784355749476L;

  public TObjectLongHashMapWithIndex() {
  }

  public TObjectLongHashMapWithIndex(final int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Get the index of given key or -1 if it does not exist.
   */
  public final int getIndex(final Object val) {
    return super.index(val);
  }

  /**
   * Get the long value for given index obtained using
   * {@link #getIndex(Object)}.
   */
  public final long getValueForIndex(final int index) {
    return _values[index];
  }

  /**
   * @see #insertionIndex(Object)
   */
  public final int getInsertionIndex(final Object key) {
    return super.insertionIndex(key);
  }

  /**
   * Put at an index returned by {@link #getInsertionIndex(Object)}.
   */
  public final Object putAtIndex(final Object key, long value, int index) {
    long previous = this.defaultValue;
    boolean isNewMapping = true;
    if (index < 0) {
        index = -index -1;
        previous = _values[index];
        isNewMapping = false;
    }
    Object oldKey = _set[index];
    _set[index] = key;
    _values[index] = value;

    if (isNewMapping) {
        postInsertHook(oldKey == null);
    }
    return previous;
  }
}
