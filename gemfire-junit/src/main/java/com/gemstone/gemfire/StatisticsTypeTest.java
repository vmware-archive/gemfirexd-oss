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
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

//import junit.framework.*;

/**
 * Tests the functionality of the {@link StatisticsType} class.
 *
 * @author David Whitlock
 *
 */
public class StatisticsTypeTest extends GemFireTestCase {

  public StatisticsTypeTest(String name) {
    super(name);
  }

  ////////  Test methods

  private StatisticsFactory factory() {
    return InternalDistributedSystem.getAnyInstance();
  }
  
  /**
   * Get the offset of an unknown statistic
   */
  public void testNameToIdUnknownStatistic() {
    StatisticDescriptor[] stats = {
      factory().createIntGauge("test", "TEST", "ms")
    };

    StatisticsType type = factory().createType("testNameToIdUnknownStatistic", "TEST", stats);
    assertEquals(0, type.nameToId("test"));
    try {
      type.nameToId("Fred");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }
  public void testNameToDescriptorUnknownStatistic() {
    StatisticDescriptor[] stats = {
      factory().createIntGauge("test", "TEST", "ms")
    };

    StatisticsType type = factory().createType("testNameToDescriptorUnknownStatistic", "TEST", stats);
    assertEquals("test", type.nameToDescriptor("test").getName());
    try {
      type.nameToDescriptor("Fred");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

}
