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
 * Extends {@link THashMap} to allow passing in a long context value
 * along-with the object. Avoids having to create a wrapper object just for
 * this, or calculating the long context for every hashCode and equals call.
 */
public final class THashMapWithLongContext extends THashMap {

  private static final long serialVersionUID = 3104657416158603157L;

  private final TObjectHashingStrategyWithLongContext strategyWithContext;

  public THashMapWithLongContext(final TObjectHashingStrategy strategy,
      final TObjectHashingStrategyWithLongContext strategyWithContext) {
    super(strategy);
    this.strategyWithContext = strategyWithContext;
  }

  public THashMapWithLongContext(int initialCapacity, float loadFactor,
      final TObjectHashingStrategy strategy,
      final TObjectHashingStrategyWithLongContext strategyWithContext) {
    super(initialCapacity, loadFactor, strategy);
    this.strategyWithContext = strategyWithContext;
  }

  public final Object get(final Object key, final long context) {
    final int index = index(key, context);
    return index >= 0 ? this._values[index] : null;
  }

  /**
   * Locates the index of <tt>obj</tt>.
   *
   * @param obj an <code>Object</code> value
   * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
   */
  protected final int index(final Object obj, final long context) {
    final int hash, probe, length;
    final Object[] set;
    Object cur;
    int index;

    set = this._set;
    length = set.length;
    hash = this.strategyWithContext.computeHashCode(obj, context) & 0x7fffffff;
    index = hash % length;
    cur = set[index];

    if (cur != null
        && (cur == REMOVED || !this.strategyWithContext.equals(cur, obj,
        context))) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      do {
        index -= probe;
        if (index < 0) {
          index += length;
        }
        cur = set[index];
      } while (cur != null
          && (cur == REMOVED || !this.strategyWithContext.equals(cur, obj,
          context)));
    }
    return cur != null ? index : -1;
  }
}
