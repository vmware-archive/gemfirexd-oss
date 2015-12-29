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
package com.gemstone.gemfire.internal;

import junit.framework.TestCase;
import com.gemstone.gemfire.internal.UniqueIdGenerator;

/**
 * Tests UniqueIdGenerator.
 * @author Darrel
 * @since 5.0.2 (cbb5x_PerfScale)
 */
public class UniqueIdGeneratorJUnitTest extends TestCase {
  
  public UniqueIdGeneratorJUnitTest() {
  }
  
  public void setup() {
  }
  
  public void tearDown() {
  }
  
  public void testBasics() throws Exception {
    UniqueIdGenerator uig = new UniqueIdGenerator(1);
    assertEquals(0, uig.obtain());
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    uig.release(0);
    assertEquals(0, uig.obtain());

    uig = new UniqueIdGenerator(32768);
    for (int i=0; i < 32768; i++) {
      assertEquals(i, uig.obtain());
    }
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    for (int i=32767; i >= 0; i--) {
      uig.release(i);
      assertEquals(i, uig.obtain());
    }
  }
}
