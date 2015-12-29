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
package com.gemstone.gemfire.internal.concurrent;

/**
 * Any additional result state needed to be passed to {@link MapCallback} which
 * returns values by reference.
 *
 * @author swale
 * @since Helios
 */
public interface MapResult {

  /**
   * Set whether the result of {@link MapCallback#newValue} created a new value
   * or not. If not, then the result value of newValue is not inserted into the
   * map though it is still returned by the create methods. Default for
   * MapResult is assumed to be true if this method was not invoked by
   * {@link MapCallback} explicitly.
   */
  void setNewValueCreated(boolean created);

  /**
   * Result set by {@link #setNewValueCreated(boolean)}. Default is required to
   * be true.
   */
  boolean isNewValueCreated();
}
