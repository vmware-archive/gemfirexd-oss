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

/**
 * Interface to support pluggable hashing strategies in
 * {@link THashMapWithLongContext} to allow passing in a long context value
 * along-with the object. Avoids having to create a wrapper object just for
 * this, or calculating the long context for every hashCode and equals call.
 */
public interface TObjectHashingStrategyWithLongContext {

  /**
   * Computes a hash code for the specified object. Implementors can use the
   * object's own <tt>hashCode</tt> method, the Java runtime's
   * <tt>identityHashCode</tt>, or a custom scheme.
   *
   * @param o       object for which the hashcode is to be computed
   * @param context long context passed to
   *                {@link THashMapWithLongContext#get(Object, long)}
   * @return the hashCode
   */
  int computeHashCode(Object o, long context);

  /**
   * Compares o1 and o2 for equality. Strategy implementors may use the
   * objects' own equals() methods, compare object references, or implement
   * some custom scheme.
   *
   * @param o1      an <code>Object</code> value
   * @param o2      an <code>Object</code> value
   * @param context long context passed to
   *                {@link THashMapWithLongContext#get(Object, long)}
   * @return true if the objects are equal according to this strategy.
   */
  boolean equals(Object o1, Object o2, long context);
}
