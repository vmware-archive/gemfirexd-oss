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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gnu.trove.*;

/**
 * The base class for hashtables of primitive values.
 * Each subclass is responsible to reserve a primitive value for
 * indicating that a slot is FREE.
 * This saves memory compared to TPrimitiveHash that has a byte[].
 * We also lose the distinction of FREE vs REMOVED so remove is not allowed
 * on stateless collections.
 * If needed remove support can be added by adding another reserved primitive
 * value to indicate a REMOVE.
 *
 * @author darrel
 */

abstract public class TStatelessPrimitiveHash extends THash  {

  /**
   * Creates a new <code>THash</code> instance with the default
   * capacity and load factor.
   */
  public TStatelessPrimitiveHash() {
    super();
  }

  /**
   * Creates a new <code>TStatelessPrimitiveHash</code> instance with a prime
   * capacity at or near the specified capacity and with the default
   * load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public TStatelessPrimitiveHash(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  @Override 
  protected void removeAt(int index) {
    throw new IllegalStateException("remove not allowed on stateless classes");
  }

  /**
   * Creates a new <code>TStatelessPrimitiveHash</code> instance with a prime
   * capacity at or near the minimum needed to hold
   * <tt>initialCapacity<tt> elements with load factor
   * <tt>loadFactor</tt> without triggering a rehash.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public TStatelessPrimitiveHash(int initialCapacity, float loadFactor) {
    super();
    _loadFactor = loadFactor;
    setUp((int)Math.ceil(initialCapacity / loadFactor));
  }

  @Override 
    public Object clone() {
    TStatelessPrimitiveHash h = (TStatelessPrimitiveHash)super.clone();
    return h;
  }
} // TStatelessPrimitiveHash
