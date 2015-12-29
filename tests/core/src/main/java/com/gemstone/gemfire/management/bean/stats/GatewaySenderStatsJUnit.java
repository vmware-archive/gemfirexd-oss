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

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.management.internal.beans.GatewaySenderMBeanBridge;

/**
 * @author rishim
 */
public class GatewaySenderStatsJUnit extends MBeanStatsTestCase {
  
  private GatewaySenderMBeanBridge bridge;

  private GatewaySenderStats senderStats;

  private static long testStartTime = NanoTimer.getTime();

  public GatewaySenderStatsJUnit(String name) {
    super(name);
  }

  public void init() {
    senderStats = new GatewaySenderStats(system, "test");

    bridge = new GatewaySenderMBeanBridge();
    bridge.addGatewaySenderStats(senderStats);
  }
  
  public void testSenderStats() throws InterruptedException{
    senderStats.incBatchesRedistributed();
    senderStats.incEventsReceived();
    senderStats.setQueueSize(10);
    senderStats.endPut(testStartTime);
    senderStats.endBatch(testStartTime, 100);
    senderStats.incEventsNotQueuedConflated();
    
    sample();
    
    assertEquals(1, getTotalBatchesRedistributed());
    assertEquals(1, getTotalEventsConflated());
    assertEquals(10, getEventQueueSize());
    assertTrue(getEventsQueuedRate() >0);
    assertTrue(getEventsReceivedRate() >0);
    assertTrue(getBatchesDispatchedRate() >0);
    assertTrue(getAverageDistributionTimePerBatch() >0);
  }

  private int getTotalBatchesRedistributed() {
    return bridge.getTotalBatchesRedistributed();
  }

  private int getTotalEventsConflated() {
    return bridge.getTotalEventsConflated();
  }  

  private int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

  private float getEventsQueuedRate() {
    return bridge.getEventsQueuedRate();
  }

  private float getEventsReceivedRate() {
    return bridge.getEventsReceivedRate();
  }
   
  private float getBatchesDispatchedRate() {
    return bridge.getBatchesDispatchedRate();
  }
   
  private long getAverageDistributionTimePerBatch() {
    return bridge.getAverageDistributionTimePerBatch();
  }
}
