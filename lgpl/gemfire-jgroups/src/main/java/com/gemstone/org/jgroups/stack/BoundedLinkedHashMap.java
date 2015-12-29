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
package com.gemstone.org.jgroups.stack;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class <code>BoundedLinkedHashMap</code> is a bounded
 * <code>LinkedHashMap</code>. The bound is the maximum
 * number of entries the <code>BoundedLinkedHashMap</code>
 * can contain.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 * @deprecated as of 5.7 create your own class that extends {@link LinkedHashMap}
 * and implement {@link LinkedHashMap#removeEldestEntry}
 * to enforce a maximum number of entries.
 */
@Deprecated
public class BoundedLinkedHashMap extends LinkedHashMap
{
  private static final long serialVersionUID = -3419897166186852692L;

  /**
   * The maximum number of entries allowed in this
   * <code>BoundedLinkedHashMap</code>
   */
  protected int _maximumNumberOfEntries;

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param loadFactor The load factor
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, float loadFactor, int maximumNumberOfEntries) {
    super(initialCapacity, loadFactor);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, int maximumNumberOfEntries) {
    super(initialCapacity);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int maximumNumberOfEntries) {
    super();
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Returns the maximum number of entries.
   * @return the maximum number of entries
   */
  public int getMaximumNumberOfEntries(){
    return this._maximumNumberOfEntries;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry entry) {
    return size() > this._maximumNumberOfEntries;
  }
}
