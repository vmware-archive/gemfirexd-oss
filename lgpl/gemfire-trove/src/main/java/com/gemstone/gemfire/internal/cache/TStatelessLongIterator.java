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
 * Iterator for long collections.
 *
 * @author darrel
 */
public class TStatelessLongIterator extends TIterator {
  /** the collection on which the iterator operates */
  private final TStatelessLongHash _hash;

  /**
   * Creates a TStatelessLongIterator for the elements in the specified collection.
   */
  public TStatelessLongIterator(TStatelessLongHash hash) {
    super(hash);
    this._hash = hash;
  }

  /**
   * Advances the iterator to the next element in the underlying collection
   * and returns it.
   *
   * @return the next long in the collection
   * @exception java.util.NoSuchElementException if the iterator is already exhausted
   */
  public long next() {
    moveToNextIndex();
    return _hash._set[_index];
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

    long[] set = _hash._set;
    long FREE = _hash.getFreeValue();
    int i = _index;
    while (i-- > 0 && (set[i] == FREE)) ;
    return i;
  }
}// TStatelessLongIterator
