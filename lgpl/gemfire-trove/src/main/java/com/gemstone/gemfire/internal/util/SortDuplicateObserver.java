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
package com.gemstone.gemfire.internal.util;

/**
 * Check for duplicates in sorting and whether they should be eliminated.
 *
 * @author swale
 * @since gfxd 2.0
 */
public interface SortDuplicateObserver {

  /**
   * returns true if this observer can possibly return true for
   * {@link #eliminateDuplicate(Object, Object)}
   */
  boolean canSkipDuplicate();

  /**
   * Return true if a duplicate object should be eliminated or not in the result
   * of a sort.
   *
   * @param insertObject
   *          the object being inserted or compared against
   *          <code>existingObject</code>
   * @param existingObject
   *          the existing object in the sorted result
   *
   * @return true if <code>newObject</code> should be eliminated from sorted
   *         result and false otherwise
   */
  boolean eliminateDuplicate(Object insertObject, Object existingObject);
}
