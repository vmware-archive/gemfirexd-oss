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

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.statistics.TestSampleHandler.Info;
import com.gemstone.gemfire.internal.statistics.TestSampleHandler.ResourceInstanceInfo;
import com.gemstone.gemfire.internal.statistics.TestSampleHandler.ResourceTypeInfo;
import com.gemstone.gemfire.internal.statistics.TestSampleHandler.SampledInfo;

import junit.framework.TestCase;

/**
 * Unit tests for the SampleCollector class.
 *   
 * @author Kirk Lund
 * @since 7.0
 */
public class SampleCollectorJUnitTest extends TestCase {

  private static final String dir = "SampleCollectorJUnitTest";

  private Mockery mockContext;
  private TestStatisticsManager manager; 
  private SampleCollector sampleCollector;
  
  public SampleCollectorJUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(dir).mkdir();
    this.mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
    LogWriterI18n log = new PureLogWriter(LogWriterImpl.levelNameToCode("config"));
    final long startTime = System.currentTimeMillis();
    this.manager = new TestStatisticsManager(
        1, 
        "SampleCollectorJUnitTest", 
        startTime, 
        log);
    
    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig = this.mockContext.mock(StatArchiveHandlerConfig.class, "SampleCollectorJUnitTest$StatArchiveHandlerConfig");
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
      will(returnValue("SampleCollectorJUnitTest"));
    }});

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());
  }

  @Override
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
  
  public void testAddHandlerBeforeSample() {
    TestSampleHandler handler = new TestSampleHandler();
    this.sampleCollector.addSampleHandler(handler);

    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createIntCounter("ST1_1_name", "ST1_1_desc", "ST1_1_units")
    };
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    
    this.sampleCollector.sample(NanoTimer.getTime());
    
    assertEquals(3, handler.getNotificationCount());
    List<Info> notifications = handler.getNotifications(); 
    
    // validate the allocatedResourceType notification
    assertTrue(notifications.get(0) instanceof ResourceTypeInfo);
    ResourceTypeInfo allocatedResourceTypeInfo = 
        (ResourceTypeInfo) notifications.get(0);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    ResourceType resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(0, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    StatisticsType statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST1);
    assertEquals("ST1_name", statisticsType.getName());
    assertEquals("ST1_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);
    
    // validate the allocatedResourceInstance notification
    assertTrue(notifications.get(1) instanceof ResourceInstanceInfo);
    ResourceInstanceInfo allocatedResourceInstanceInfo = 
        (ResourceInstanceInfo) notifications.get(1);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    ResourceInstance resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(0, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    Statistics statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_1);
    assertEquals(1, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_1_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());
    
    // validate the sampled notification
    assertTrue(notifications.get(2) instanceof SampledInfo);
    SampledInfo sampledInfo = (SampledInfo) notifications.get(2);
    assertNotNull(sampledInfo);
    assertEquals("sampled", sampledInfo.getName());
    assertEquals(1, sampledInfo.getResourceCount());
  }
  
  public void testAddHandlerAfterSamples() {
    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createIntCounter("ST1_1_name", "ST1_1_desc", "ST1_1_units")
    };
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2_text", 1);

    StatisticDescriptor[] statsST2 = new StatisticDescriptor[] {
        manager.createIntCounter("ST2_1_name", "ST2_1_desc", "ST2_1_units")
    };
    StatisticsType ST2 = manager.createType("ST2_name", "ST2_desc", statsST2);
    Statistics st2_1 = manager.createAtomicStatistics(ST2, "st2_1_text", 1);
    
    st1_1.incInt("ST1_1_name", 1);
    st1_2.incInt("ST1_1_name", 1);
    st2_1.incInt("ST2_1_name", 1);
    long sampleTime = NanoTimer.getTime();
    this.sampleCollector.sample(sampleTime);

    st1_1.incInt("ST1_1_name", 2);
    st2_1.incInt("ST2_1_name", 1);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    st1_1.incInt("ST1_1_name", 1);
    st1_1.incInt("ST1_1_name", 2);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    TestSampleHandler handler = new TestSampleHandler();
    this.sampleCollector.addSampleHandler(handler);
    
    assertEquals("TestSampleHandler = " + handler, 0, handler.getNotificationCount());

    st1_2.incInt("ST1_1_name", 1);
    st2_1.incInt("ST2_1_name", 1);
    sampleTime += NanoTimer.millisToNanos(1000);
    this.sampleCollector.sample(sampleTime);

    assertEquals(6, handler.getNotificationCount());
    List<Info> notifications = handler.getNotifications();

    // validate the allocatedResourceType notification for ST1
    int notificationIdx = 0;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceTypeInfo);
    ResourceTypeInfo allocatedResourceTypeInfo = 
        (ResourceTypeInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    ResourceType resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(0, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    StatisticsType statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST1);
    assertEquals("ST1_name", statisticsType.getName());
    assertEquals("ST1_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);

    // validate the allocatedResourceInstance notification for st1_1
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    ResourceInstanceInfo allocatedResourceInstanceInfo = 
        (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    ResourceInstance resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(0, resourceInstance.getId());
    assertEquals(0, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    Statistics statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_1);
    assertEquals(1, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_1_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());
    
    // validate the allocatedResourceInstance notification for st1_2
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    allocatedResourceInstanceInfo = 
        (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(1, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st1_2);
    assertEquals(2, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st1_2_text", statistics.getTextId());
    assertEquals("ST1_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());
    
    // validate the allocatedResourceType notification for ST2
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceTypeInfo);
    allocatedResourceTypeInfo = (ResourceTypeInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceTypeInfo);
    assertEquals("allocatedResourceType", allocatedResourceTypeInfo.getName());
    resourceType = allocatedResourceTypeInfo.getResourceType();
    assertNotNull(resourceType);
    assertEquals(1, resourceType.getId());
    assertEquals(1, resourceType.getStatisticDescriptors().length);
    statisticsType = resourceType.getStatisticsType();
    assertNotNull(statisticsType);
    assertTrue(statisticsType == ST2);
    assertEquals("ST2_name", statisticsType.getName());
    assertEquals("ST2_desc", statisticsType.getDescription());
    assertEquals(1, statisticsType.getStatistics().length);

    // validate the allocatedResourceInstance notification for st2_1
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof ResourceInstanceInfo);
    allocatedResourceInstanceInfo = 
        (ResourceInstanceInfo) notifications.get(notificationIdx);
    assertNotNull(allocatedResourceInstanceInfo);
    assertEquals("allocatedResourceInstance", allocatedResourceInstanceInfo.getName());
    resourceInstance = allocatedResourceInstanceInfo.getResourceInstance();
    assertNotNull(resourceInstance);
    assertEquals(2, resourceInstance.getId());
    assertEquals(1, resourceInstance.getUpdatedStats().length);
    assertEquals(1, resourceInstance.getLatestStatValues().length); // TODO: is this correct?
    statistics = resourceInstance.getStatistics();
    assertNotNull(statistics);
    assertTrue(statistics == st2_1);
    assertEquals(3, statistics.getUniqueId());
    assertEquals(1, statistics.getNumericId());
    assertEquals("st2_1_text", statistics.getTextId());
    assertEquals("ST2_name", statistics.getType().getName());
    assertTrue(resourceType == resourceInstance.getResourceType());
    
    // validate the sampled notification
    notificationIdx++;
    assertTrue(notifications.get(notificationIdx) instanceof SampledInfo);
    SampledInfo sampledInfo = (SampledInfo) notifications.get(notificationIdx);
    assertNotNull(sampledInfo);
    assertEquals("sampled", sampledInfo.getName());
    assertEquals(3, sampledInfo.getResourceCount());
  }
  
  public void testGetStatMonitorHandler() {
    StatMonitorHandler handler = SampleCollector.getStatMonitorHandler();
    assertNotNull(handler);
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    assertNull(handler.getStatMonitorNotifier());
  }

  public void testGetStatMonitorHandlerAfterClose() {
    this.sampleCollector.close();
    try {
      /*StatMonitorHandler handler =*/ SampleCollector.getStatMonitorHandler();
      fail("getStatMonitorHandler should throw IllegalStateException when SampleCollector is closed");
    } catch (IllegalStateException expected) {
      // passed
    }
  }

  public void testGetStatMonitorHandlerBeforeAndAfterClose() {
    StatMonitorHandler handler = SampleCollector.getStatMonitorHandler();
    assertNotNull(handler);
    this.sampleCollector.close();
    try {
      handler = SampleCollector.getStatMonitorHandler();
      fail("getStatMonitorHandler should throw IllegalStateException when SampleCollector is closed");
    } catch (IllegalStateException expected) {
      // passed
    }
  }
  
  public void testGetStatArchiveHandler() {
    StatArchiveHandler handler = this.sampleCollector.getStatArchiveHandler();
    assertNotNull(handler);
  }
}
