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

import java.io.File;
import java.util.List;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.PureLogWriter;

import junit.framework.TestCase;

/**
 * Unit and integration tests for the StatisticsMonitor.
 *   
 * @author Kirk Lund
 * @since 7.0
 */
public class StatisticsMonitorJUnitTest extends TestCase {
  
  private Mockery mockContext;
  private LogWriterI18n log;
  private TestStatisticsManager manager; 
  private SampleCollector sampleCollector;

  public StatisticsMonitorJUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    this.mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
    
    this.log = new PureLogWriter(LogWriterImpl.levelNameToCode("config"));
    
    final long startTime = System.currentTimeMillis();
    this.manager = new TestStatisticsManager(
        1, 
        "StatisticsMonitorJUnitTest", 
        startTime, 
        log);
    
    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig = this.mockContext.mock(StatArchiveHandlerConfig.class, "StatisticsMonitorJUnitTest$StatArchiveHandlerConfig");
    this.mockContext.checking(new Expectations() {{
      allowing(mockStatArchiveHandlerConfig).getArchiveFileName();
      will(returnValue(new File("")));
      allowing(mockStatArchiveHandlerConfig).getArchiveFileSizeLimit();
      will(returnValue(0));
      allowing(mockStatArchiveHandlerConfig).getArchiveDiskSpaceLimit();
      will(returnValue(0));
      allowing(mockStatArchiveHandlerConfig).getSystemId();
      will(returnValue(1));
      allowing(mockStatArchiveHandlerConfig).getSystemStartTime();
      will(returnValue(startTime));
      allowing(mockStatArchiveHandlerConfig).getSystemDirectoryPath();
      will(returnValue(""));
      allowing(mockStatArchiveHandlerConfig).getProductDescription();
      will(returnValue("StatisticsMonitorJUnitTest"));
    }});

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());
  }

  public void tearDown() throws Exception {
    super.tearDown();
    if (this.sampleCollector != null) {
      this.sampleCollector.close();
      this.sampleCollector = null;
    }
    this.mockContext.assertIsSatisfied();
    this.mockContext = null;
    this.manager = null;
  }
  
  public void testAddListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    assertNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
        
    monitor.addListener(listener);

    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }
  
  public void testAddExistingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());
  }
  
  public void testRemoveListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    assertNull(this.sampleCollector.getStatMonitorHandlerSnapshot());

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());
    
    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
    
    monitor.removeListener(listener);
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));

    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertTrue(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }
  
  public void testRemoveMissingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));

    monitor.removeListener(listener);

    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));
  }
  
  // TODO: test addStatistic
  // TODO: test removeStatistic
  // TODO: test monitor and/or monitorStatisticIds
  // TODO: test notifyListeners
  
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
