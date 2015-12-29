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
package com.gemstone.gemfire.management.bean.stats;

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.management.internal.beans.AsyncEventQueueMBeanBridge;

/**
 * 
 * @author rishim
 *
 */
public class AsyncEventQueueStatsJUnitTest extends MBeanStatsTestCase {

  private AsyncEventQueueMBeanBridge bridge;

  private AsyncEventQueueStats asyncEventQueueStats;

  public AsyncEventQueueStatsJUnitTest(String name) {
    super(name);
  }

  public void init() {
    asyncEventQueueStats = new AsyncEventQueueStats(system, "test");

    bridge = new AsyncEventQueueMBeanBridge();
    bridge.addAsyncEventQueueStats(asyncEventQueueStats);
  }

  public void testSenderStats() throws InterruptedException {
    asyncEventQueueStats.setQueueSize(10);

    sample();
    assertEquals(10, getEventQueueSize());
    
    asyncEventQueueStats.setQueueSize(0);
    sample();
    
    assertEquals(0, getEventQueueSize());
    
    
  }

  private int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

}
