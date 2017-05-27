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

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.StatArchiveReader.StatValue;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class StatSamplerJUnitTest extends TestCase {

  private Map<String,String> statisticTypes;
  private Map<String,Map<String,Number>> allStatistics;
  
  public StatSamplerJUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    this.statisticTypes = new HashMap<String,String>();
    this.allStatistics = new HashMap<String,Map<String,Number>>();
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("stats.log-level");
    System.clearProperty("stats.disable");
    System.clearProperty("stats.name");
    System.clearProperty("stats.archive-file");
    System.clearProperty("stats.file-size-limit");
    System.clearProperty("stats.disk-space-limit");
    System.clearProperty("stats.sample-rate");
    this.statisticTypes = null;
    this.allStatistics = null;
    StatisticsTypeFactoryImpl.clear();
    StatArchiveWriter.clearTraceFilter();
  }
  
  public void testStatSampler() throws Exception {
    final String testName = "testStatSampler";
    StatArchiveWriter.setTraceFilter("st1_1", "ST1");
    
    System.setProperty("stats.log-level", "config");
    System.setProperty("stats.disable", "false");
    System.setProperty("stats.name", "StatSamplerJUnitTest_" + testName);
    System.setProperty("stats.archive-file", "StatSamplerJUnitTest_" + testName + ".gfs");
    System.setProperty("stats.file-size-limit", "0");
    System.setProperty("stats.disk-space-limit", "0");
    System.setProperty("stats.sample-rate", "100");
    
    final CancelCriterion stopper = new CancelCriterion() {
      public String cancelInProgress() {
        return null;
      }
      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    };
    final LocalStatisticsFactory factory = new LocalStatisticsFactory(stopper);
    final StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        factory.createDoubleCounter("double_counter_1", "d1",  "u1"),
        factory.createDoubleCounter("double_counter_2", "d2",  "u2",  true),
        factory.createDoubleGauge(  "double_gauge_3",   "d3",  "u3"),
        factory.createDoubleGauge(  "double_gauge_4",   "d4",  "u4",  false),
        factory.createIntCounter(   "int_counter_5",    "d5",  "u5"),
        factory.createIntCounter(   "int_counter_6",    "d6",  "u6",  true),
        factory.createIntGauge(     "int_gauge_7",      "d7",  "u7"),
        factory.createIntGauge(     "int_gauge_8",      "d8",  "u8",  false),
        factory.createLongCounter(  "long_counter_9",   "d9",  "u9"),
        factory.createLongCounter(  "long_counter_10",  "d10", "u10", true),
        factory.createLongGauge(    "long_gauge_11",    "d11", "u11"),
        factory.createLongGauge(    "long_gauge_12",    "d12", "u12", false)
    };
    final StatisticsType ST1 = factory.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = factory.createAtomicStatistics(ST1, "st1_1", 1);
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
        return samplerStatsInstances != null && samplerStatsInstances.length > 0;
      }
      public String description() {
        return "Waiting for statSampler stats";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 4000, 10, true);
    
    Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
    assertNotNull(samplerStatsInstances);
    assertEquals(1, samplerStatsInstances.length);
    final Statistics samplerStats = samplerStatsInstances[0];
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3",   3);
    incInt(st1_1,    "int_counter_5",    5);
    incInt(st1_1,    "int_gauge_7",      7);
    incLong(st1_1,   "long_counter_9",   9);
    incLong(st1_1,   "long_gauge_11",    11);
    
    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   -1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      -1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    -1);
    incLong(st1_1,   "long_gauge_12",    1);

    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    waitForStatSample(samplerStats);
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3",   3);
    incInt(st1_1,    "int_counter_5",    5);
    incInt(st1_1,    "int_gauge_7",      7);
    incLong(st1_1,   "long_counter_9",   9);
    incLong(st1_1,   "long_gauge_11",    11);
    
    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   -1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      -1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    -1);
    incLong(st1_1,   "long_gauge_12",    1);

    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    factory.close();
    
    final File archiveFile = new File(System.getProperty("stats.archive-file"));
    assertTrue(archiveFile.exists());
    final StatArchiveReader reader = new StatArchiveReader(
        new File[]{archiveFile}, null, false);

    @SuppressWarnings("rawtypes")
    List resources = reader.getResourceInstList();
    for (@SuppressWarnings("rawtypes")
    Iterator iter = resources.iterator(); iter.hasNext();) {
      StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
      String resourceName = ri.getName();
      assertNotNull(resourceName);
      
      if (!resourceName.equals("st1_1")) {
        //factory.getLogWriterI18n().convertToLogWriter()
        //    .fine
        System.out.println("testStatSampler skipping " + resourceName);
        continue;
      }
      
      String expectedStatsType = this.statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());
      
      Map<String,Number> expectedStatValues = this.allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);
      
      StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));
        
        assertEquals(statName, statValues[i].getDescriptor().getName());
        
        statValues[i].setFilter(StatValue.FILTER_NONE);
        //double[] rawSnapshots = statValues[i].getRawSnapshots();
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), 
            statValues[i].getSnapshotsMostRecent());
      }
    }
  }
  
  private void waitForStatSample(final Statistics samplerStats) {
    final int startSampleCount = samplerStats.getInt("sampleCount");
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return samplerStats.getInt("sampleCount") > startSampleCount;
      }
      public String description() {
        return "Waiting for statSampler sampleCount to increment";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 3000, 10, true);
  }
  
  private void incDouble(Statistics statistics, String stat, double value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incDouble(stat, value);
    statValues.put(stat, statistics.getDouble(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
  
  private void incInt(Statistics statistics, String stat, int value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incInt(stat, value);
    statValues.put(stat, statistics.getInt(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private void incLong(Statistics statistics, String stat, long value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incLong(stat, value);
    statValues.put(stat, statistics.getLong(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
}
