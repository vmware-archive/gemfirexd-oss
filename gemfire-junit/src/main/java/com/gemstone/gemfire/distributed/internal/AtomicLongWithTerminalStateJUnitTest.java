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
package com.gemstone.gemfire.distributed.internal;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
public class AtomicLongWithTerminalStateJUnitTest extends TestCase {
  
  public void test() {
    AtomicLongWithTerminalState al = new AtomicLongWithTerminalState();
    assertEquals(23, al.compareAddAndGet(-1, 23));
    assertEquals(23, al.getAndSet(-1));
    // test for terminal state
    assertEquals(-1, al.compareAddAndGet(-1, 12));
    // test for normal delta
    assertEquals(11, al.compareAddAndGet(0, 12));
  }
}
