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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.lang.Filter;
import com.gemstone.gemfire.internal.lang.StringUtils;

import org.junit.Test;

/**
 * The CollectionUtilsTest class is a test suite of test cases testing the contract and functionality of the
 * CollectionUtils class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.util.CollectionUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@SuppressWarnings("null")
public class CollectionUtilsTest {

  @Test
  public void testAsList() {
    final Integer[] numbers = { 0, 1, 2, 1, 0 };

    final List<Integer> numberList = CollectionUtils.asList(numbers);

    assertNotNull(numberList);
    assertFalse(numberList.isEmpty());
    assertEquals(numbers.length, numberList.size());
    assertTrue(numberList.containsAll(Arrays.asList(numbers)));
    assertEquals(new Integer(0), numberList.remove(0));
    assertEquals(numbers.length - 1, numberList.size());
  }

  @Test
  public void testAsSet() {
    final Integer[] numbers = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    final Set<Integer> numberSet = CollectionUtils.asSet(numbers);

    assertNotNull(numberSet);
    assertFalse(numberSet.isEmpty());
    assertEquals(numbers.length, numberSet.size());
    assertTrue(numberSet.containsAll(Arrays.asList(numbers)));
    assertTrue(numberSet.remove(1));
    assertEquals(numbers.length - 1, numberSet.size());
  }

  @Test
  public void testAsSetWithNonUniqueElements() {
    final Integer[] numbers = { 0, 1, 2, 1, 0 };

    final Set<Integer> numberSet = CollectionUtils.asSet(numbers);

    assertNotNull(numberSet);
    assertFalse(numberSet.isEmpty());
    assertEquals(3, numberSet.size());
    assertTrue(numberSet.containsAll(Arrays.asList(numbers)));
  }

  @Test
  public void testEmptyListWithNullList() {
    final List<Object> actualList = CollectionUtils.emptyList(null);

    assertNotNull(actualList);
    assertTrue(actualList.isEmpty());
  }

  @Test
  public void testEmptyListWithEmptyList() {
    final List<Object> expectedList = new ArrayList<Object>(0);

    assertNotNull(expectedList);
    assertTrue(expectedList.isEmpty());

    final List<Object> actualList = CollectionUtils.emptyList(expectedList);

    assertSame(expectedList, actualList);
  }

  @Test
  public void testEmptyListWithList() {
    final List<String> expectedList = Arrays.asList("aardvark", "baboon", "cat", "dog", "eel", "ferret");

    assertNotNull(expectedList);
    assertFalse(expectedList.isEmpty());

    final List<String> actualList = CollectionUtils.emptyList(expectedList);

    assertSame(expectedList, actualList);
  }

  @Test
  public void testEmptySetWithNullSet() {
    final Set<Object> actualSet = CollectionUtils.emptySet(null);

    assertNotNull(actualSet);
    assertTrue(actualSet.isEmpty());
  }

  @Test
  public void testEmptySetWithEmptySet() {
    final Set<Object> expectedSet = new HashSet<Object>(0);

    assertNotNull(expectedSet);
    assertTrue(expectedSet.isEmpty());

    final Set<Object> actualSet = CollectionUtils.emptySet(expectedSet);

    assertSame(expectedSet, actualSet);
  }

  @Test
  public void testEmptySetWithSet() {
    final Set<String> expectedSet = new HashSet<String>(Arrays.asList("aardvark", "baboon", "cat", "dog", "ferret"));

    assertNotNull(expectedSet);
    assertFalse(expectedSet.isEmpty());

    final Set<String> actualSet = CollectionUtils.emptySet(expectedSet);

    assertSame(expectedSet, actualSet);
  }

  @Test
  public void testFindAll() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    final List<Integer> matches = CollectionUtils.findAll(numbers, new Filter<Integer>() {
      // accept all even numbers
      public boolean accept(final Integer number) {
        return (number % 2 == 0);
      }
    });

    assertNotNull(matches);
    assertFalse(matches.isEmpty());
    assertTrue(matches.containsAll(Arrays.asList(0, 2, 4, 6, 8)));
  }

  @Test
  public void testFindAllWhenMultipleElementsMatch() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 1, 4, 1, 6, 1, 7, 1, 9);

    final List<Integer> matches = CollectionUtils.findAll(numbers, new Filter<Integer>() {
      // accept 1
      public boolean accept(final Integer number) {
        return (number == 1);
      }
    });

    assertNotNull(matches);
    assertEquals(5, matches.size());
    assertEquals(matches, Arrays.asList(1, 1, 1, 1, 1));
  }

  @Test
  public void testFindAllWhenNoElementsMatch() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    final List<Integer> matches = CollectionUtils.findAll(numbers, new Filter<Integer>() {
      // accept negative numbers
      public boolean accept(final Integer number) {
        return (number < 0);
      }
    });

    assertNotNull(matches);
    assertTrue(matches.isEmpty());
  }

  @Test
  public void testFindBy() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    final Integer match = CollectionUtils.findBy(numbers, new Filter<Integer>() {
      // accept 2
      public boolean accept(final Integer number) {
        return (number == 2);
      }
    });

    assertNotNull(match);
    assertEquals(2, match.intValue());
  }

  @Test
  public void testFindByWhenMultipleElementsMatch() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 1, 4, 1, 6, 1, 7, 1, 9);

    final Integer match = CollectionUtils.findBy(numbers, new Filter<Integer>() {
      // accept 1
      public boolean accept(final Integer number) {
        return (number == 1);
      }
    });

    assertNotNull(match);
    assertEquals(1, match.intValue());
  }

  @Test
  public void testFindByWhenNoElementsMatch() {
    final List<Integer> numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    final Integer match = CollectionUtils.findBy(numbers, new Filter<Integer>() {
      // accept 10
      public boolean accept(final Integer number) {
        return (number == 10);
      }
    });

    assertNull(match);
  }

  @Test
  public void testRemoveKeys() {
    final Map<Object, String> expectedMap = new HashMap<Object, String>(6);

    expectedMap.put("key1", "value");
    expectedMap.put("key2", "null");
    expectedMap.put("key3", "nil");
    expectedMap.put("key4", null);
    expectedMap.put("key5", "");
    expectedMap.put("key6", "  ");

    assertFalse(expectedMap.isEmpty());
    assertEquals(6, expectedMap.size());

    final Map<Object, String> actualMap = CollectionUtils.removeKeys(expectedMap, new Filter<Map.Entry<Object, String>>() {
      @Override public boolean accept(final Map.Entry<Object, String> entry) {
        return !StringUtils.isBlank(entry.getValue());
      }
    });

    assertSame(expectedMap, actualMap);
    assertFalse(actualMap.isEmpty());
    assertEquals(3, actualMap.size());
    assertTrue(actualMap.keySet().containsAll(Arrays.asList("key1", "key2", "key3")));
  }

  @Test
  public void testRemoveKeysWithNullValues() {
    final Map<Object, Object> expectedMap = new HashMap<Object, Object>(3);

    expectedMap.put("one", "test");
    expectedMap.put("two", null);
    expectedMap.put(null, "null");
    expectedMap.put("null", "nil");

    assertFalse(expectedMap.isEmpty());
    assertEquals(4, expectedMap.size());

    final Map<Object, Object> actualMap = CollectionUtils.removeKeysWithNullValues(expectedMap);

    assertSame(expectedMap, actualMap);
    assertEquals(3, expectedMap.size());
    assertEquals("null", expectedMap.get(null));
  }

  @Test
  public void testRemoveKeysWithNullValuesFromEmptyMap() {
    final Map<?, ?> expectedMap = Collections.emptyMap();

    assertNotNull(expectedMap);
    assertTrue(expectedMap.isEmpty());

    final Map<?, ?> actualMap = CollectionUtils.removeKeysWithNullValues(expectedMap);

    assertSame(expectedMap, actualMap);
    assertTrue(actualMap.isEmpty());
  }

  @Test
  public void testRemoveKeysWithNullValuesFromMapWithNoNullValues() {
    final Map<String, Object> map = new HashMap<String, Object>(5);

    map.put("one", "test");
    map.put("null", "null");
    map.put("two", "testing");
    map.put(null, "nil");
    map.put("three", "tested");

    assertEquals(5, map.size());

    CollectionUtils.removeKeysWithNullValues(map);

    assertEquals(5, map.size());
  }

}
