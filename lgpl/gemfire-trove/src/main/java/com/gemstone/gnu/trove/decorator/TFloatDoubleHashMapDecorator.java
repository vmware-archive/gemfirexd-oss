///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2002, Eric D. Friedman All Rights Reserved.
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

package com.gemstone.gnu.trove.decorator;

import com.gemstone.gnu.trove.TFloatDoubleHashMap;
import com.gemstone.gnu.trove.TFloatDoubleIterator;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
//import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper class to make a TFloatDoubleHashMap conform to the <tt>java.util.Map</tt> API.
 * This class simply decorates an underlying TFloatDoubleHashMap and translates the Object-based
 * APIs into their Trove primitive analogs.
 *
 * <p>
 * Note that wrapping and unwrapping primitive values is extremely inefficient.  If
 * possible, users of this class should override the appropriate methods in this class
 * and use a table of canonical values.
 * </p>
 *
 * Created: Mon Sep 23 22:07:40 PDT 2002 
 *
 * @author Eric D. Friedman
 * @version $Id: TFloatDoubleHashMapDecorator.java,v 1.4 2004/03/18 15:30:34 ericdf Exp $
 * @since trove 0.1.8
 */
public class TFloatDoubleHashMapDecorator extends AbstractMap implements Map, Cloneable {
    /** the wrapped primitive map */
    protected TFloatDoubleHashMap _map;

    /**
     * Creates a wrapper that decorates the specified primitive map.
     */
    public TFloatDoubleHashMapDecorator(TFloatDoubleHashMap map) {
	super();
	this._map = map;
    }

    /** 
     * Clones the underlying trove collection and returns the clone wrapped in a new
     * decorator instance.  This is a shallow clone except where primitives are 
     * concerned.
     *
     * @return a copy of the receiver
     */
    @Override // GemStoneAddition
    public Object clone() {
        try {
            TFloatDoubleHashMapDecorator copy = (TFloatDoubleHashMapDecorator) super.clone();
            copy._map = (TFloatDoubleHashMap)_map.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            // assert(false);
            throw new InternalError(); // we are cloneable, so this does not happen
        }
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key an <code>Object</code> value
     * @param value an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>,
     * or Integer(0) if none was found.
     */
    @Override // GemStoneAddition
    public Object put(Object key, Object value) {
	return wrapValue(_map.put(unwrapKey(key), unwrapValue(value)));
    }

    /**
     * Compares this map with another map for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return true if the maps are identical
     */
    @Override // GemStoneAddition
    public boolean equals(Object other) {
	if (_map.equals(other)) {
	    return true;	// comparing two trove maps
	} else if (other instanceof Map) {
	    Map that = (Map)other;
	    if (that.size() != _map.size()) {
		return false;	// different sizes, no need to compare
	    } else {		// now we have to do it the hard way
		Iterator it = that.entrySet().iterator();
		for (int i = that.size(); i-- > 0;) {
		    Map.Entry e = (Map.Entry)it.next();
		    Object key = e.getKey();
		    Object val = e.getValue();
		    if (key instanceof Float && val instanceof Double) {
			float k = unwrapKey(key);
			double v = unwrapValue(val);
			if (_map.containsKey(k) && v == _map.get(k)) {
			    // match, ok to continue
			} else {
			    return false; // no match: we're done
			}
		    } else {
			return false; // different type in other map
		    }
		}
		return true;	// all entries match
	    }
	} else {
	    return false;
	}
    }

    /**
     * Retrieves the value for <tt>key</tt>
     *
     * @param key an <code>Object</code> value
     * @return the value of <tt>key</tt> or null if no such mapping exists.
     */
    @Override // GemStoneAddition
    public Object get(Object key) {
	float k = unwrapKey(key);
	double v = _map.get(k);
	// 0 may be a false positive since primitive maps
	// cannot return null, so we have to do an extra
	// check here.
	if (v == 0) {
	    return _map.containsKey(k) ? wrapValue(v) : null;
	} else {
	    return wrapValue(v);
	}
    }


    /**
     * Empties the map.
     */
    @Override // GemStoneAddition
    public void clear() {
	this._map.clear();
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>Object</code> value
     * @return the removed value, or Integer(0) if it was not found in the map
     */
    @Override // GemStoneAddition
    public Object remove(Object key) {
	return wrapValue(_map.remove(unwrapKey(key)));
    }

    /**
     * Returns a Set view on the entries of the map.
     *
     * @return a <code>Set</code> value
     */
    @Override // GemStoneAddition
    public Set entrySet() {
        return new AbstractSet() {
          @Override // GemStoneAddition
		public int size() {
		    return _map.size();
		}
          @Override // GemStoneAddition
		public boolean isEmpty() {
		    return TFloatDoubleHashMapDecorator.this.isEmpty();
		}
          @Override // GemStoneAddition
		public boolean contains(Object o) {
		    if (o instanceof Map.Entry) {
			Object k = ((Map.Entry)o).getKey();
			Object v = ((Map.Entry)o).getValue();
			return (TFloatDoubleHashMapDecorator.this.containsKey(k) &&
				TFloatDoubleHashMapDecorator.this.get(k).equals(v));
		    } else {
			return false;
		    }
		}
          @Override // GemStoneAddition
		public Iterator iterator() {
		    return new Iterator() {
			    private final TFloatDoubleIterator it = _map.iterator();
			    
			    public Object next() {
				it.advance();
				final Object key = wrapKey(it.key());
				final Object v = wrapValue(it.value());
				return new Map.Entry() {
					private Object val = v;
					    @Override // GemStoneAddition
					public boolean equals(Object o) {
					    return ((o instanceof Map.Entry) &&
						    ((Map.Entry)o).getKey().equals(key) &&
						    ((Map.Entry)o).getValue().equals(val));
					}
					public Object getKey() {
					    return key;
					}
					public Object getValue() {
					    return val;
					}
					    @Override // GemStoneAddition
					public int hashCode() {
					    return key.hashCode() + val.hashCode();
					}
					public Object setValue(Object value) {
					    val = value;
					    return put(key, value);
					}
				    };
			    }

			    public boolean hasNext() {
				return it.hasNext();
			    }

			    public void remove() {
				it.remove();
			    }
			};
		}
		    @Override // GemStoneAddition
		public boolean add(Object o) {
		    throw new UnsupportedOperationException();
		}
		    @Override // GemStoneAddition
		public boolean remove(Object o) {
		    throw new UnsupportedOperationException();
		}
		    @Override // GemStoneAddition
		public boolean addAll(Collection c) {
		    throw new UnsupportedOperationException();		    
		}
		    @Override // GemStoneAddition
		public boolean retainAll(Collection c) {
		    throw new UnsupportedOperationException();
		}
		    @Override // GemStoneAddition
		public boolean removeAll(Collection c) {
		    throw new UnsupportedOperationException();
		}
		    @Override // GemStoneAddition
		public void clear() {
		    TFloatDoubleHashMapDecorator.this.clear();
		}
	    };
    }

    /**
     * Checks for the presence of <tt>val</tt> in the values of the map.
     *
     * @param val an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    @Override // GemStoneAddition
    public boolean containsValue(Object val) {
	return _map.containsValue(unwrapValue(val));
    }

    /**
     * Checks for the present of <tt>key</tt> in the keys of the map.
     *
     * @param key an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    @Override // GemStoneAddition
    public boolean containsKey(Object key) {
	return _map.containsKey(unwrapKey(key));
    }

    /**
     * Returns the number of entries in the map.
     * @return the map's size.
     */
    @Override // GemStoneAddition
    public int size() {
	return this._map.size();
    }

    /**
     * Indicates whether map has any entries.
     * @return true if the map is empty
     */
    @Override // GemStoneAddition
    public boolean isEmpty() {
	return (size() == 0);
    }

    /**
     * Copies the key/value mappings in <tt>map</tt> into this map.
     * Note that this will be a <b>deep</b> copy, as storage is by
     * primitive value.
     *
     * @param map a <code>Map</code> value
     */
    @Override // GemStoneAddition
    public void putAll(Map map) {
	Iterator it = map.entrySet().iterator();
	for (int i = map.size(); i-- > 0;) {
	    Map.Entry e = (Map.Entry)it.next();
	    this.put(e.getKey(), e.getValue());
	}
    }

    /**
     * Wraps a key
     *
     * @param k a key in the underlying map
     * @return an Object representation of the key
     */
    protected Float wrapKey(float k) {
	return new Float(k);
    }

    /**
     * Unwraps a key
     *
     * @param key a wrapped key
     * @return an unwrapped representation of the key
     */
    protected float unwrapKey(Object key) {
	return ((Float)key).floatValue();
    }
    /**
     * Wraps a value
     *
     * @param k a value in the underlying map
     * @return an Object representation of the value
     */
    protected Double wrapValue(double k) {
	return Double.valueOf(k);
    }

    /**
     * Unwraps a value
     *
     * @param value a wrapped value
     * @return an unwrapped representation of the value
     */
    protected double unwrapValue(Object value) {
	return ((Double)value).doubleValue();
    }

} // TFloatDoubleHashMapDecorator
