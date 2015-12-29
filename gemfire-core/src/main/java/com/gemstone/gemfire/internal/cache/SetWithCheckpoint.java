/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.gemstone.gemfire.internal.cache;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.locks.NonReentrantLock;
import com.gemstone.gnu.trove.THashSet;

/**
 * A {@link Set} implementation that allows putting up checkpoints and iterating
 * over elements added since the {@link Checkpoint} or removing all new
 * elements. The set disallows removing elements from itself directly (though
 * {@link #iterator()} can still be used to remove elements, it will cause
 * unexpected behaviour for any {@link Checkpoint}s captured prior to that).
 * 
 * It tries to optimize memory overhead by using a list for small sizes and
 * switching to a linked HashSet for larger sizes.
 * 
 * @author swale
 * @since 7.0
 */
public final class SetWithCheckpoint extends AbstractSet<Object> implements
    Set<Object> {

  private final ArrayList<Object> list;

  private final boolean isIdentity;

  /**
   * Acts like an index over the list of objects present in {@link #list}.
   */
  private THashSet set;

  /**
   * The size of {@link #list} before a {@link #set} will be created for more
   * efficient lookup.
   */
  private static final int MIN_SET_SIZE = 8;

  private static final int DEFAULT_CAPACITY = 4;

  /**
   * Allows capturing state of an append-only {@link SetWithCheckpoint} and
   * iterating over any changed elements ({@link #elementAt(int)}) or removing
   * them ({@link #removeAt(int)}).
   * 
   * @author swale
   * @since 7.0
   */
  final class AppendOnlyCheckpoint implements Checkpoint {

    int cpPos;

    final NonReentrantLock sync;

    AppendOnlyCheckpoint(final NonReentrantLock sync) {
      this.cpPos = list.size();
      this.sync = sync;
    }

    /**
     * @see Checkpoint#attemptLock(long)
     */
    public boolean attemptLock(long msecs) {
      return this.sync.attemptLock(msecs);
    }

    /**
     * @see Checkpoint#releaseLock()
     */
    public void releaseLock() {
      this.sync.releaseLock();
    }

    /**
     * @see Checkpoint#elementAt(int)
     */
    public Object elementAt(int index) {
      return index >= 0 ? list.get(this.cpPos + index) : null;
    }

    /**
     * @see Checkpoint#elementState(int)
     */
    public ObjectState elementState(int index) {
      return ObjectState.ADDED;
    }

    /**
     * @see Checkpoint#numChanged()
     */
    public int numChanged() {
      return list.size() - this.cpPos;
    }

    /**
     * @see Checkpoint#removeAt(int)
     */
    public Object removeAt(int index) {
      return SetWithCheckpoint.this.removeAt(this.cpPos + index);
    }

    /**
     * Remove all new elements added to {@link SetWithCheckpoint} after this
     * checkpoint was captured.
     */
    public void removeAllChanged() {
      final int origSize = list.size();
      if (set != null) {
        // check if newSize will go below required minimum set size
        if (this.cpPos < MIN_SET_SIZE) {
          // SubList clear is more efficient since it avoids range check etc.
          // in each call to remove as above
          list.subList(this.cpPos, origSize).clear();
          set = null;
        }
        else {
          for (int index = origSize - 1; index >= this.cpPos; index--) {
            final Object removed = list.remove(index);
            set.remove(removed);
          }
        }
      }
      else {
        // SubList clear is more efficient since it avoids range check etc.
        // in each call to remove as above
        list.subList(this.cpPos, origSize).clear();
      }
    }

    /**
     * @see Checkpoint#updateToEnd()
     */
    public void updateToEnd() {
      this.cpPos = list.size();
    }
  }

  /**
   * Default constructor with initial capacity of {@value #DEFAULT_CAPACITY}.
   */
  public SetWithCheckpoint() {
    this(false);
  }

  /**
   * Constructor with initial capacity of {@value #DEFAULT_CAPACITY} and having
   * boolean argument to specify whether the set uses identity comparison rather
   * than {@link Object#equals(Object)}.
   */
  public SetWithCheckpoint(final boolean isIdentitySet) {
    this.list = new ArrayList<Object>(DEFAULT_CAPACITY);
    this.isIdentity = isIdentitySet;
  }

  /**
   * Constructs a set given an initial capacity.
   */
  public SetWithCheckpoint(final int initialCapacity,
      final boolean isIdentitySet) {
    this.list = new ArrayList<Object>(initialCapacity);
    this.isIdentity = isIdentitySet;
    if (initialCapacity > MIN_SET_SIZE) {
      if (isIdentitySet) {
        this.set = new THashSet(initialCapacity,
            ObjectEqualsHashingStrategy.getInstance());
      }
      else {
        this.set = new THashSet(initialCapacity);
      }
    }
  }

  public Checkpoint checkpoint(final NonReentrantLock sync) {
    return new AppendOnlyCheckpoint(sync);
  }

  // ----------------------- Methods of the Set interface

  /**
   * Returns an iterator over the elements in this set. The elements are
   * returned in the insertion order.
   * 
   * @return an Iterator over the elements in this set
   * 
   * @see ConcurrentModificationException
   * @see Set#iterator()
   */
  @Override
  public final Iterator<Object> iterator() {
    return this.list.iterator();
  }

  /**
   * Returns the number of elements in this set.
   * 
   * @return the number of elements in this set
   * 
   * @see Set#size()
   */
  @Override
  public final int size() {
    return this.list.size();
  }

  /**
   * Returns <tt>true</tt> if this set contains the specified element.
   * 
   * @param o
   *          element whose presence in this set is to be tested
   * 
   * @return <tt>true</tt> if this set contains the specified element
   * 
   * @see Set#contains(Object)
   */
  @Override
  public final boolean contains(final Object o) {
    if (this.set != null) {
      return this.set.contains(o);
    }
    return listContains(o);
  }

  private final boolean listContains(final Object o) {
    if (this.isIdentity) {
      for (int index = 0; index < this.list.size(); index++) {
        if (this.list.get(index) == o) {
          return true;
        }
      }
    }
    else {
      for (int index = 0; index < this.list.size(); index++) {
        if (this.list.get(index).equals(o)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Adds the specified element to this set if it is not already present.
   * 
   * @param o
   *          element to be added to this set
   * 
   * @return <tt>true</tt> if this set did not already contain the specified
   *         element
   * 
   * @see Set#add(Object)
   */
  @Override
  public final boolean add(final Object o) {
    if (this.set != null) {
      if (this.set.add(o)) {
        this.list.add(o);
        return true;
      }
      return false;
    }
    if (!listContains(o)) {
      this.list.add(o);
      final int size;
      if ((size = this.list.size()) > MIN_SET_SIZE) {
        if (this.isIdentity) {
          this.set = new THashSet(size,
              ObjectEqualsHashingStrategy.getInstance());
        }
        else {
          this.set = new THashSet(size);
        }
        for (int index = 0; index < size; index++) {
          this.set.add(this.list.get(index));
        }
      }
      return true;
    }
    return false;
  }

  public Object elementAt(final int index) {
    return this.list.get(index);
  }

  /**
   * Remove is not supported.
   */
  @Override
  public final boolean remove(final Object o) {
    throw new UnsupportedOperationException("remove not support for "
        + this.getClass().getName());
  }

  public final Object removeAt(final int index) {
    final Object removed = this.list.remove(index);
    if (removed != null && this.set != null) {
      if (this.list.size() < MIN_SET_SIZE) {
        this.set = null;
      }
      else {
        this.set.remove(removed);
      }
    }
    return removed;
  }

  /**
   * Remove is not supported.
   */
  @Override
  public final boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException("removeAll not support for "
        + this.getClass().getName());
  }

  /**
   * Removes all of the elements from this set.
   */
  @Override
  public void clear() {
    this.list.clear();
    if (this.set != null) {
      this.set.clear();
    }
  }

  // ----------------------- End methods of the Set interface
}
