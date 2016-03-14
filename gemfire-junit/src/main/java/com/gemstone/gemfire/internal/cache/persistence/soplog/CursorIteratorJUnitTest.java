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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.cache.persistence.soplog.CursorIterator.WrappedIterator;

public class CursorIteratorJUnitTest extends TestCase {
  public void testWrappedIterator() {
    List<Integer> test = new ArrayList<Integer>();
    test.add(0);
    test.add(1);
    test.add(2);
    test.add(3);
    
    CursorIterator<Integer> iter = new WrappedIterator<Integer>(test.iterator());
    
    assertTrue(iter.hasNext());
    assertEquals(Integer.valueOf(0), iter.next());
    assertTrue(iter.hasNext());
    assertEquals(Integer.valueOf(0), iter.current());
    assertEquals(Integer.valueOf(1), iter.next());
  }
}
