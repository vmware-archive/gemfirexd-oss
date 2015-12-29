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

package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.distributed.*;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

import junit.framework.TestCase;

import java.lang.management.*;
import java.util.*;

/**
 * Unit test for basic DataPolicy.EMPTY feature.
 * NOTE: these tests using a loner DistributedSystem and local scope regions
 * @author Darrel Schneider
 * @since 5.0
 */
public class Bug42039JUnitTest extends TestCase
{
  /**
   * Keep calling DistributedSystem.connect over and over again
   * with a locator configured. Since the locator is not running
   * expect the connect to fail.
   * See if threads leak because of the repeated calls
   */
  public void testBug42039() throws Exception {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    p.setProperty("locators", "localhost[6666]");
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    for (int i=0; i < 2; i++) {
      try {
        DistributedSystem.connect(p);
        fail("expected connect to fail");
      } catch (Exception expected) {
      }
    }
    int initialThreadCount = threadBean.getThreadCount();
    for (int i=0; i < 5; i++) {
      try {
        DistributedSystem.connect(p);
        fail("expected connect to fail");
      } catch (Exception expected) {
      }
    }
    Thread.sleep(1000); // give chance for thread to exit
    int endThreadCount = threadBean.getThreadCount();
    if (endThreadCount > initialThreadCount) {
      assertEquals(initialThreadCount, endThreadCount);
    }
  }
}
