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

package hydra.blackboard;

import java.util.Map;

/**
 *  Variant of the {@link java.util.Map} interface, suitable for using as a
 *  facade over RMI and GemFire versions of <code>Map</code>.
 *
 *  All operations will modify the underlying shared map, with the exception
 *  of {@link #getMap}, which returns a copy that is not backed by the
 *  shared map, but is useful for iterating over a snapshot of its contents.
 */
public interface SharedMap {

  /**
   *  Equivalent of {@link java.util.Map#size}.
   */
  public int size();

  /**
   *  Equivalent of {@link java.util.Map#isEmpty}.
   */
  public boolean isEmpty();

  /**
   *  Equivalent of {@link java.util.Map#containsKey(Object)}.
   */
  public boolean containsKey(Object key);

  /**
   *  Equivalent of {@link java.util.Map#containsValue(Object)}.
   */
  public boolean containsValue(Object value);

  /**
   *  Equivalent of {@link java.util.Map#get(Object)}.
   */
  public Object get(Object key);

  /**
   *  Equivalent of {@link java.util.Map#put(Object, Object)}.
   */
  public Object put(Object key, Object value);

  /**
   *  Replaces the current value with the specified one
   *  if the specified one is larger than the current one.
   */
  public long putIfLarger(Object key, long value);

  /**
   *  Replaces the current value with the specified one
   *  if the specified one is smaller than the current one.
   */
  public long putIfSmaller(Object key, long value);

  /**
   *  Equivalent of {@link java.util.Map#remove(Object)}.
   */
  public Object remove(Object key);

  /**
   *  Equivalent of {@link java.util.Map#putAll(Map)}.
   */
  public void putAll(Map t);

  /**
   *  Equivalent of {@link java.util.Map#clear}.
   */
  public void clear();

  /**
   *  Returns a copy of the underlying map.  Useful for getting
   *  all keys, values, or entries and iterating over them
   *  without getting {@link java.util.ConcurrentModificationException}.
   *
   *  Note, however, that an iterator on this map is not backed by the
   *  shared map.  For example, the <code>add</code> and <code>remove</code>
   *  operations on iterators will modify only the copy, not the shared map.
   */
  public Map getMap();

  /**
   * Returns a {@linkplain hydra.ConfigHashtable#getRandGen random}
   * key from this map.  If the map is empty, <code>null</code> is
   * returned.
   *
   * author David Whitlock
   * @since 2.0.3
   */
  public Object getRandomKey();

  /**
   * Performs an atomic "test and replace" on this map.  The
   * <code>newValue</code> will only be placed into this map only if the
   * current value of the mapping matches (is <code>equals</code> to)
   * <code>expectedValue</code>.  This method is similar to the
   * <code>replace</code> method on the JSR-166
   * <code>ConcurrentHashMap</code>.
   *
   * @param key
   *        The key of the mapping
   * @param expectedValue
   *        The value that we expect the mapping to have
   * @param newValue
   *        The new value put into the mapping if the current value
   *        equals <code>expectedValue</code>.
   *
   * @return <code>true</code> if <code>newValue</code> was placed
   *         into the map.
   *
   * author David Whitlock
   * @since 2.0.3
   */
  public boolean replace(Object key, Object expectedValue,
                         Object newValue);

}
