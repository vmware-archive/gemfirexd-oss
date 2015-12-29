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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Iterator;

import com.gemstone.gemfire.cache.query.internal.index.IndexElemArray;

import junit.framework.TestCase;

public class IndexElemArrayTest extends TestCase {
  IndexElemArray list;

  public void testFunctionality() throws Exception {
    System.setProperty("index_elemarray_size", "7");
    list = new IndexElemArray();
    boundaryCondition();
    add();
    clearAndAdd();
    removeFirst();
    clearAndAdd();
    removeLast();
    clearAndAdd();
    remove();
    clearAndAdd();
    iterate();
    clearAndAdd();
  }

  private void add() {
    Object objBefore = list.getElementData();
    insert(7);
    Object objAfter = list.getElementData();
    assertSame(objBefore, objAfter);

    assertEquals(7, list.size());
    for (int i = 0; i < 7; i++) {
      assertEquals(i + 1, list.get(i));
    }
    list.add(8);
    objAfter = list.getElementData();
    assertNotSame(objBefore, objAfter);
    
    assertEquals(8, list.size());
    for (int i = 0; i < 8; i++) {
      assertEquals(i + 1, list.get(i));
    }
  }

  private void insert(int num) {
    for (int i = 1; i <= num; i++) {
      list.add(i);
    }
  }

  private void removeFirst() {
    list.remove(1);
    assertEquals(6, list.size());
    for (int i = 0; i < 6; i++) {
      assertEquals(i + 2, list.get(i));
    }
  }

  private void removeLast() {
    list.remove(7);
    assertEquals(6, list.size());
    for (int i = 0; i < 6; i++) {
      assertEquals(i + 1, list.get(i));
    }
  }

  private void remove() {
    list.remove(4);
    assertEquals(6, list.size());
    int temp[] = { 1, 2, 3, 5, 6, 7 };
    for (int i = 0; i < 6; i++) {
      assertEquals(temp[i], list.get(i));
    }
  }

  private void clearAndAdd() {
    list.clear();
    insert(7);
  }

  private void iterate() {
    Iterator itr = list.iterator();
    int i = 1;
    while (itr.hasNext()) {
      assertEquals(i++, itr.next());
    }
  }
  
  private void boundaryCondition() {
      try {
        Object o = list.get(2);
        fail("get() Should have thrown IndexOutOfBoundsException");
      } catch (IndexOutOfBoundsException e) {
      }
  }
}
