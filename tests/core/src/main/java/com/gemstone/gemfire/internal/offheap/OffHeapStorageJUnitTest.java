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
package com.gemstone.gemfire.internal.offheap;

import junit.framework.TestCase;

public class OffHeapStorageJUnitTest extends TestCase {

  private final static long MEGABYTE = 1024 * 1024;
  private final static long GIGABYTE = 1024 * 1024 * 1024;

  public OffHeapStorageJUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
  }

  public void tearDown() throws Exception {
  }

  public void testParseOffHeapMemorySizeNegative() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize("-1"));
  }
  public void testParseOffHeapMemorySizeNull() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(null));
  }
  public void testParseOffHeapMemorySizeEmpty() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(""));
  }
  public void testParseOffHeapMemorySizeBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE));
  }
  public void testParseOffHeapMemorySizeKiloBytes() {
    try {
      OffHeapStorage.parseOffHeapMemorySize("1k");
      fail("Did not receive expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }
  public void testParseOffHeapMemorySizeMegaBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1m"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "m"));
  }
  public void testParseOffHeapMemorySizeGigaBytes() {
    assertEquals(GIGABYTE, OffHeapStorage.parseOffHeapMemorySize("1g"));
    assertEquals(Integer.MAX_VALUE * GIGABYTE, OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "g"));
  }
}
