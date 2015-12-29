///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
/*
 * Contains changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.gemstone.gnu.trove;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
//import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * An implementation of the <tt>Set</tt> interface that uses an
 * open-addressed hash table to store its contents.
 *
 * Created: Sat Nov  3 10:38:17 2001
 *
 * @author Eric D. Friedman
 * @version $Id: THashSet.java,v 1.11 2003/03/23 04:06:59 ericdf Exp $
 */

public class THashSet extends TObjectHash implements Set, Serializable {
private static final long serialVersionUID = 2705415913937015662L;

    /**
     * Creates a new <code>THashSet</code> instance with the default
     * capacity and load factor.
     */
    public THashSet() {
        super();
    }

    /**
     * Creates a new <code>THashSet</code> instance with the default
     * capacity and load factor.
     * 
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashSet(TObjectHashingStrategy strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public THashSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashSet(int initialCapacity, TObjectHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor a <code>float</code> value
     */
    public THashSet(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor a <code>float</code> value
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashSet(int initialCapacity, float loadFactor, TObjectHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance containing the
     * elements of <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     */
    public THashSet(Collection collection) {
        this(collection.size());
        addAll(collection);
    }

    /**
     * Creates a new <code>THashSet</code> instance containing the
     * elements of <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashSet(Collection collection, TObjectHashingStrategy strategy) {
        this(collection.size(), strategy);
        addAll(collection);
    }

    /**
     * Inserts a value into the set.
     *
     * @param obj an <code>Object</code> value
     * @return true if the set was modified by the add operation
     */
    public boolean add(Object obj) {
        int index = insertionIndex(obj);

        if (index < 0) {
            return false;       // already present in set, nothing to add
        }

        Object old = _set[index];
        _set[index] = obj;

        postInsertHook(old == null);
        return true;            // yes, we added something
    }

    // GemStoneAddition
    /**
     * Inserts a value into the set if not present else returns existing value.
     */
    public final Object putIfAbsent(Object obj) {
        int index = insertionIndex(obj);

        if (index < 0) {
            return _set[-index - 1];  // already present in set, nothing to add
        }

        Object old = _set[index];
        _set[index] = obj;

        postInsertHook(old == null);
        return null;            // yes, we added something
    }

    @Override // GemStoneAddition
    public boolean equals(Object other) {
        if (! (other instanceof Set)) {
            return false;
        }
        Set that = (Set)other;
        if (that.size() != this.size()) {
            return false;
        }
        return containsAll(that);
    }

    @Override // GemStoneAddition
    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEach(p);
        return p.getHashCode();
    }

    protected/*GemStoneAddition*/ final class HashProcedure implements TObjectProcedure {
        private int h = 0;
        
        public int getHashCode() {
            return h;
        }
        
        public final boolean execute(Object key) {
            h += _hashingStrategy.computeHashCode(key);
            return true;
        }
    }

    /**
     * Expands the set to accomodate new values.
     *
     * @param newCapacity an <code>int</code> value
     */
    @Override // GemStoneAddition
    protected void rehash(int newCapacity) {
        final int oldCapacity = _set.length;
        final Object[] oldSet = _set;

        final Object[] newSet = _set = new Object[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if(oldSet[i] != null && oldSet[i] != REMOVED) {
                final Object o = oldSet[i];
                final int index = insertionIndex(o);
                if (index < 0) { // everyone pays for this because some people can't RTFM
                    throwObjectContractViolation(newSet[(-index -1)], o);
                }
                newSet[index] = o;
            }
        }
    }

    /**
     * Returns a new array containing the objects in the set.
     *
     * @return an <code>Object[]</code> value
     */
    public final Object[] toArray() {
        Object[] result = new Object[size()];
        forEach(new ToObjectArrayProcedure(result));
        return result;
    }

    /**
     * Returns a typed array of the objects in the set.
     *
     * @param a an <code>Object[]</code> value
     * @return an <code>Object[]</code> value
     */
    public final Object[] toArray(Object[] a) {
        int size = size();
        if (a.length < size)
            a = (Object[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

        forEach(new ToObjectArrayProcedure(a));

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    /**
     * Empties the set.
     */
    @Override // GemStoneAddition
    public final void clear() {
        super.clear();
        Object[] set = _set;

        for (int i = set.length; i-- > 0;) {
            set[i] = null;
        }
    }

    /**
     * Removes <tt>obj</tt> from the set.
     *
     * @param obj an <code>Object</code> value
     * @return true if the set was modified by the remove operation.
     */
    public boolean remove(Object obj) {
        int index = index(obj);
        if (index >= 0) {
            removeAt(index);
            return true;
        }
        return false;
    }

    /**
     * Creates an iterator over the values of the set.  The iterator
     * supports element deletion.
     *
     * @return an <code>Iterator</code> value
     */
    public Iterator iterator() {
        return new TObjectHashIterator(this);
    }

    /**
     * Tests the set to determine if all of the elements in
     * <tt>collection</tt> are present.
     *
     * @param collection a <code>Collection</code> value
     * @return true if all elements were present in the set.
     */
    public final boolean containsAll(Collection collection) {
        for (Iterator i = collection.iterator(); i.hasNext();) {
            if (! contains(i.next())) {
                return false;
            }
        }
        return true;
    }

    // GemStoneAddition
    /**
     * Searches the set for <tt>obj</tt>
     *
     * @param obj an <code>Object</code> value
     * @return the value in the map
     */
    public Object get(Object obj) {
      final int index = index(obj);
      if (index >= 0) {
        return _set[index];
      }
      else {
        return null;
      }
    }

    /**
     * Adds all of the elements in <tt>collection</tt> to the set.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the add all operation.
     */
    public boolean addAll(Collection collection) {
        boolean changed = false;
// GemStone changes BEGIN
        ensureCapacity(collection.size());
        for (Object o : collection) {
          if (add(o)) {
            changed = true;
          }
        }
        /* (original code)
        int size = collection.size();
        Iterator it;

        ensureCapacity(size);
        it = collection.iterator();
        while (size-- > 0) {
            if (add(it.next())) {
                changed = true;
            }
        }
        */
// GemStone changes END
        return changed;
    }

    /**
     * Removes all of the elements in <tt>collection</tt> from the set.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the remove all operation.
     */
    public boolean removeAll(Collection collection) {
        boolean changed = false;
// GemStone changes BEGIN
        for (Object o : collection) {
          if (remove(o)) {
            changed = true;
          }
        }
        /* (original code)
        int size = collection.size();
        Iterator it;

        it = collection.iterator();
        while (size-- > 0) {
            if (remove(it.next())) {
                changed = true;
            }
        }
        */
// GemStone changes END
        return changed;
    }

    /**
     * Removes any values in the set which are not contained in
     * <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the retain all operation
     */
    public boolean retainAll(Collection collection) {
        boolean changed = false;
// GemStone changes BEGIN
        final Object[] set = _set;
        Object o;
        for (int i = set.length; i-- > 0;) {
          o = set[i];
          if (o != null && o != REMOVED
              && !collection.contains(o)) {
            removeAt(i);
            changed = true;
          }
        }
        /* (original code)
        int size = size();
        Iterator it;

        it = iterator();
        while (size-- > 0) {
            if (! collection.contains(it.next())) {
                it.remove();
                changed = true;
            }
        }
        */
// GemStone changes END
        return changed;
    }

    private void writeObject(ObjectOutputStream stream)
        throws IOException {
        stream.defaultWriteObject();

        // number of entries
        stream.writeInt(_size);

        SerializationProcedure writeProcedure = new SerializationProcedure(stream);
        if (! forEach(writeProcedure)) {
            throw writeProcedure.exception;
        }
    }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        int size = stream.readInt();
        setUp(size);
        while (size-- > 0) {
            Object val = stream.readObject();
            add(val);
        }
    }

    // GemStoneAddition
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append('[');
      forEach(new TObjectProcedure() {
        private boolean notFirst;
        public final boolean execute(Object key) {
          if (this.notFirst) {
            sb.append(", ");
          }
          else {
            this.notFirst = true;
          }
          sb.append(key);
          return true;
        }
      });
      return sb.append(']').toString();
    }
} // THashSet
