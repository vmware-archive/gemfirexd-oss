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

import java.util.Map;

/**
 * @author kneeraj
 *
 */
public interface CacheMap {

  // Query Operations

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings in this map
   */
  int size();

  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings
   */
  boolean isEmpty();

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified
   * key.  More formally, returns <tt>true</tt> if and only if
   * this map contains a mapping for a key.
   *
   * @param key key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified
   *         key
   * @throws NullPointerException if the specified key is null and this map
   *         does not permit null keys
   */
  boolean containsKey(Object key);

  /**
   * Returns the value to which the specified key is mapped,
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or
   *         {@code null} if this map contains no mapping for the key
   * @throws NullPointerException if the specified key is null and this map
   *         does not permit null keys
   */
  Object get(Object key);

  // Modification Operations

  /**
   * Associates the specified value with the specified key in this map
   * (optional operation).  If the map previously contained a mapping for
   * the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   * @throws NullPointerException if the specified key or value is null
   *         and this map does not permit null keys or values
   * @throws IllegalArgumentException if some property of the specified key
   *         or value prevents it from being stored in this map
   */
  Object put(Object key, Object value);

  /**
   * Removes the mapping for a key from this map if it is present
   * (optional operation).
   *
   * <p>Returns the value to which this map previously associated the key,
   * or <tt>null</tt> if the map contained no mapping for the key.
   *
   *
   * <p>The map will not contain a mapping for the specified key once the
   * call returns.
   *
   * @param key key whose mapping is to be removed from the map
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   * @throws NullPointerException if the specified key is null and this
   *         map does not permit null keys
   */
  Object remove(Object key);
  
  /**
   * Removes all of the mappings from this map (optional operation).
   * The map will be empty after this call returns.
   *
   * @throws UnsupportedOperationException if the <tt>clear</tt> operation
   *         is not supported by this map
   */
  void clear();
  
  /**
   * Destroys the cache completely and frees resources
   */
  void destroyCache();
}
