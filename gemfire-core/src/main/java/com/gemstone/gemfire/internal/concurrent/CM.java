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
package com.gemstone.gemfire.internal.concurrent;

import com.gemstone.gemfire.internal.size.SingleObjectSizer;

/**
 * These methods are the same ones on
 * the JDK 5 version java.util.concurrent.ConcurrentMap.
 * @see CFactory
 * @author darrel
 * @deprecated use ConcurrentMap
 */
public interface CM extends java.util.Map {
    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * This is equivalent to
     * <pre>
     *   if (!map.containsKey(key))
     *       return map.put(key, value);
     *   else
     *       return map.get(key);</pre>
     * except that the action is performed atomically.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with the key,
     *         if the implementation supports null values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null,
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     *
     */
    Object putIfAbsent(Object key, Object value);

    /**
     * Removes the entry for a key only if currently mapped to a given value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key) &amp;&amp; map.get(key).equals(value)) {
     *       map.remove(key);
     *       return true;
     *   } else return false;</pre>
     * except that the action is performed atomically.
     *
     * @param key key with which the specified value is associated
     * @param value value expected to be associated with the specified key
     * @return <tt>true</tt> if the value was removed
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the key or value is of an inappropriate
     *         type for this map (optional)
     * @throws NullPointerException if the specified key or value is null,
     *         and this map does not permit null keys or values (optional)
     */
    boolean remove(Object key, Object value);

    /**
     * Replaces the entry for a key only if currently mapped to a given value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key) &amp;&amp; map.get(key).equals(oldValue)) {
     *       map.put(key, newValue);
     *       return true;
     *   } else return false;</pre>
     * except that the action is performed atomically.
     *
     * @param key key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @return <tt>true</tt> if the value was replaced
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of a specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if a specified key or value is null,
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of a specified key
     *         or value prevents it from being stored in this map
     */
    boolean replace(Object key, Object oldValue, Object newValue);

    /**
     * Replaces the entry for a key only if currently mapped to some value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key)) {
     *       return map.put(key, value);
     *   } else return null;</pre>
     * except that the action is performed atomically.
     *
     * @param key key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with the key,
     *         if the implementation supports null values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null,
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    Object replace(Object key, Object value);

    /**
     * This returns memory overhead excluding individual entry sizes. Just additional
     * memory consumed by this data structure. 
     * 
     * @param sizer The sizer object that is to be used for estimating objects.
     * @return memory overhead by this map excluding entry size.
     */
    long estimateMemoryOverhead(SingleObjectSizer sizer);
}
