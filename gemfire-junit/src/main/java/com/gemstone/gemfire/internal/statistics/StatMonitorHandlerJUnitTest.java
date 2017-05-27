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
package com.gemstone.gemfire.internal.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.statistics.StatMonitorHandler.StatMonitorNotifier;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

/**
 * Unit test for the StatMonitorHandler and its inner classes.
 *   
 * @author Kirk Lund
 * @since 7.0
 */
public class StatMonitorHandlerJUnitTest extends TestCase {

  private LogWriterI18n log = null;

  public StatMonitorHandlerJUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    this.log = new PureLogWriter(LogWriterImpl.levelNameToCode("config"));
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testAddNewMonitor() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());
  }
  
  public void testAddExistingMonitorReturnsFalse() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.addMonitor(monitor));
  }
  
  public void testRemoveExistingMonitor() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.removeMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    assertEquals(0, monitor.getNotificationCount());
  }
  
  public void testRemoveMissingMonitorReturnsFalse() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.removeMonitor(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
  }
  
  public void testNotificationSampleFrequencyDefault() {
    final int sampleFrequency = 1;
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    final int sampleCount = 100;
    for (int i = 0; i < sampleCount; i++) {
      handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
      waitForNotificationCount(monitor, 1+i, 2*1000, 10, false);
    }
    assertEquals(sampleCount/sampleFrequency, monitor.getNotificationCount());
  }
  
  public void testNotificationSampleTimeMillis() {
    final long currentTime = System.currentTimeMillis();
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    long nanoTimeStamp = NanoTimer.getTime();
    handler.sampled(nanoTimeStamp, Collections.<ResourceInstance>emptyList());
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertTrue(monitor.getTimeStamp() != nanoTimeStamp);
    assertTrue(monitor.getTimeStamp() >= currentTime);
  }
  
  public void testNotificationResourceInstances() {
    final int resourceInstanceCount = 100;
    final List<ResourceInstance> resourceInstances = new ArrayList<ResourceInstance>();
    for (int i = 0; i < resourceInstanceCount; i++) {
      resourceInstances.add(new ResourceInstance(i, null, null));
    }
    
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    handler.sampled(NanoTimer.getTime(), Collections.unmodifiableList(resourceInstances));

    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    
    final List<ResourceInstance> notificationResourceInstances = 
        monitor.getResourceInstances();
    assertNotNull(notificationResourceInstances);
    assertEquals(resourceInstances, notificationResourceInstances);
    assertEquals(resourceInstanceCount, notificationResourceInstances.size());
    
    int i = 0;
    for (ResourceInstance resourceInstance : notificationResourceInstances) {
      assertEquals(i, resourceInstance.getId());
      i++;
    }
  }
  
  public void testStatMonitorNotifierAliveButWaiting() throws Exception {
    if (!StatMonitorHandler.enableMonitorThread) {
      return;
    }
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    
    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return notifier.isWaiting();
      }
      public String description() {
        return "notifier.isWaiting()";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 2000, 10, true);
    
    for (int i = 0; i < 20; i++) {
      assert(notifier.isWaiting());
      Thread.sleep(10);
    }
  }
  
  public void testStatMonitorNotifierWakesUpForWork() throws Exception {
    if (!StatMonitorHandler.enableMonitorThread) {
      return;
    }
    StatMonitorHandler handler = new StatMonitorHandler(this.log);
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    
    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return notifier.isWaiting();
      }
      public String description() {
        return "notifier.isWaiting()";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 2000, 10, true);
    
    // if notification occurs then notifier woke up...
    assertEquals(0, monitor.getNotificationCount());
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());

    // and goes back to waiting...
    wc = new WaitCriterion() {
      public boolean done() {
        return notifier.isWaiting();
      }
      public String description() {
        return "notifier.isWaiting()";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 2000, 10, true);
  }
  
  private static void waitForNotificationCount(final TestStatisticsMonitor monitor, final int expected, long ms, long interval, boolean throwOnTimeout) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return monitor.getNotificationCount() >= expected;
      }
      public String description() {
        return "waiting for notification count to be " + expected;
      }
    };
    DistributedTestBase.waitForCriterion(wc, ms, interval, throwOnTimeout);
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  static class TestStatisticsMonitor extends StatisticsMonitor {
    
    private volatile long timeStamp;
    
    private volatile List<ResourceInstance> resourceInstances;
    
    private volatile int notificationCount;
    
    public TestStatisticsMonitor() {
      super();
    }
    
    @Override
    protected void monitor(long timeStamp, List<ResourceInstance> resourceInstances) {
      this.timeStamp = timeStamp;
      this.resourceInstances = resourceInstances;
      this.notificationCount++;
    }
    
    long getTimeStamp() {
      return this.timeStamp;
    }
    
    List<ResourceInstance> getResourceInstances() {
      return this.resourceInstances;
    }
    
    int getNotificationCount() {
      return this.notificationCount;
    }
  }
}
