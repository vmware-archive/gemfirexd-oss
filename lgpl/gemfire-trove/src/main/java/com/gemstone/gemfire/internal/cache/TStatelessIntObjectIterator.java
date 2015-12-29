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
import java.util.ConcurrentModificationException;

/**
 * Iterator for maps of type int and Object.
 *
 * @author darrel
 */

public class TStatelessIntObjectIterator extends TIterator {
    /** the collection being iterated over */
    private final TStatelessIntObjectHashMap _map;

    /**
     * Creates an iterator over the specified map
     */
    public TStatelessIntObjectIterator(TStatelessIntObjectHashMap map) {
	super(map);
	this._map = map;
    }

    /**
     * Moves the iterator forward to the next entry in the underlying map.
     *
     * @exception java.util.NoSuchElementException if the iterator is already exhausted
     */
    public void advance() {
	moveToNextIndex();
    }

    /**
     * Provides access to the key of the mapping at the iterator's position.
     * Note that you must <tt>advance()</tt> the iterator at least once
     * before invoking this method.
     *
     * @return the key of the entry at the iterator's current position.
     */
    public int key() {
	return _map._set[_index];
    }

    /**
     * Provides access to the value of the mapping at the iterator's position.
     * Note that you must <tt>advance()</tt> the iterator at least once
     * before invoking this method.
     *
     * @return the value of the entry at the iterator's current position.
     */
    public Object value() {
	return _map._values[_index];
    }

    /**
     * Replace the value of the mapping at the iterator's position with the
     * specified value. Note that you must <tt>advance()</tt> the iterator at
     * least once before invoking this method.
     *
     * @param val the value to set in the current entry
     * @return the old value of the entry.
     */
    public Object setValue(Object val) {
	Object old = value();
	_map._values[_index] = val;
	return old;
    }
  /**
   * Returns the index of the next value in the data structure
   * or a negative value if the iterator is exhausted.
   *
   * @return an <code>int</code> value
   * @exception ConcurrentModificationException if the underlying collection's
   * size has been modified since the iterator was created.
   */
  @Override 
    protected final int nextIndex() {
    if (_expectedSize != _hash.size()) {
      throw new ConcurrentModificationException();
    }

    int[] keys = _map._set;
    int FREE = _map.getFreeValue();
    int i = _index;
    while (i-- > 0 && (keys[i] == FREE)) ;
    return i;
  }
}// TStatelessIntObjectIterator
