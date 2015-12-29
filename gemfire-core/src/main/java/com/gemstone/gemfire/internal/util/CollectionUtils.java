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
package com.gemstone.gemfire.internal.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.lang.Filter;

/**
 * The CollectionUtils class is a utility class for working with the Java Collections framework of classes, data
 * structures and algorithms.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.Filter
 * @see java.util.Arrays
 * @see java.util.Collection
 * @see java.util.Collections
 * @see java.util.Iterator
 * @see java.util.List
 * @see java.util.Map
 * @see java.util.Set
 * @since 7.0
 */
@SuppressWarnings("unused")
public abstract class CollectionUtils {

  /**
   * Returns the specified array as a List of elements.
   * <p/>
   * @param <T> the class type of the elements in the array.
   * @param array the object array of elements to convert to a List.
   * @return a List of elements contained in the specified array.
   * @see java.util.Arrays#asList(Object[])
   */
  public static <T> List<T> asList(final T... array) {
    return new ArrayList<T>(Arrays.asList(array));
  }

  /**
   * Returns the specified array as a Set of elements.
   * <p/>
   * @param <T> the class type of the elements in the array.
   * @param array the object array of elements to convert to a Set.
   * @return a Set of elements contained in the specified array.
   * @see java.util.Arrays#asList(Object[])
   */
  public static <T> Set<T> asSet(final T... array) {
    return new HashSet<T>(Arrays.asList(array));
  }

  /**
   * Null-safe implementation for method invocations that return a List Collection.  If the returned List is null,
   * then this method will return an empty List in it's place.
   * <p/>
   * @param <T> the class type of the List's elements.
   * @param list the target List to verify as not null.
   * @return the specified List if not null otherwise return an empty List.
   */
  public static <T> List<T> emptyList(final List<T> list) {
    return (list != null ? list : Collections.<T>emptyList());
  }

  /**
   * Null-safe implementation for method invocations that return a Set Collection.  If the returned Set is null,
   * then this method will return an empty Set in it's place.
   * <p/>
   * @param <T> the class type of the Set's elements.
   * @param set the target Set to verify as not null.
   * @return the specified Set if not null otherwise return an empty Set.
   */
  public static <T> Set<T> emptySet(final Set<T> set) {
    return (set != null ? set : Collections.<T>emptySet());
  }

  /**
   * Iterates the Collection and finds all object elements that match the Filter criteria.
   * <p/>
   * @param <T> the class type of the Collection elements.
   * @param collection the Collection of elements to iterate and filter.
   * @param filter the Filter applied to the Collection of elements in search of all matching elements.
   * @return a List of elements from the Collection matching the criteria of the Filter in the order in which they were
   * found.  If no elements match the Filter criteria, then an empty List is returned.
   */
  public static <T> List<T> findAll(final Collection<T> collection, final Filter<T> filter) {
    final List<T> matches = new ArrayList<T>(collection.size());

    for (final T element : collection) {
      if (filter.accept(element)) {
        matches.add(element);
      }
    }

    return matches;
  }

  /**
   * Iterates the Collection and finds all object elements that match the Filter criteria.
   * <p/>
   * @param <T> the class type of the Collection elements.
   * @param collection the Collection of elements to iterate and filter.
   * @param filter the Filter applied to the Collection of elements in search of the matching element.
   * @return a single element from the Collection that match the criteria of the Filter.  If multiple elements match
   * the Filter criteria, then this method will return the first one.  If no element of the Collection matches
   * the criteria of the Filter, then this method returns null.
   */
  public static <T> T findBy(final Collection<T> collection, final Filter<T> filter) {
    for (T element : collection) {
      if (filter.accept(element)) {
        return element;
      }
    }

    return null;
  }

  /**
   * Removes keys from the Map based on a Filter.
   * <p/>
   * @param <K> the Class type of the key.
   * @param <V> the Class type of the value.
   * @param map the Map from which to remove key-value pairs based on a Filter.
   * @param filter the Filter to apply to the Map entries to ascertain their "value".
   * @return the Map with entries filtered by the specified Filter.
   * @see java.util.Map
   * @see java.util.Map.Entry
   * @see com.gemstone.gemfire.internal.lang.Filter
   */
  public static <K, V> Map<K, V> removeKeys(final Map<K, V> map, final Filter<Map.Entry<K, V>> filter) {
    for (final Iterator<Map.Entry<K, V>> mapEntries = map.entrySet().iterator(); mapEntries.hasNext(); ) {
      if (!filter.accept(mapEntries.next())) {
        mapEntries.remove();
      }
    }

    return map;
  }

  /**
   * Removes keys with null values in the Map.
   * <p/>
   * @param map the Map from which to remove null key-value pairs.
   * @return the Map without any null keys or values.
   * @see #removeKeys
   * @see java.util.Map
   */
  public static <K, V> Map<K, V> removeKeysWithNullValues(final Map<K, V> map) {
    return removeKeys(map, new Filter<Map.Entry<K, V>>() {
      @Override public boolean accept(final Map.Entry<K, V> entry) {
        return (entry.getValue() != null);
      }
    });
  }

}
