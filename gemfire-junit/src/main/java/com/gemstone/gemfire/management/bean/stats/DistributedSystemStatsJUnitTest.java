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

import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.SystemManagementService;

/**
 * 
 * @author rishim
 * @since  1.4
 */
public class DistributedSystemStatsJUnitTest extends TestCase {

  protected static final long SLEEP = 100;
  protected static final long TIMEOUT = 4 * 1000;

  protected InternalDistributedSystem system;

  protected Cache cache;

  public DistributedSystemStatsJUnitTest(String name) {
    super(name);
  }

  @SuppressWarnings("deprecation")
  public void setUp() throws Exception {

    final Properties props = new Properties();
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-sampling-enabled", "false");
    props.setProperty("statistic-sample-rate", "60000");
    props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    props.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    // set JMX_MANAGER_UPDATE_RATE to practically an infinite value, so that
    // LocalManager wont try to run ManagementTask
    props.setProperty(DistributionConfig.JMX_MANAGER_UPDATE_RATE_NAME, "60000");
    props.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");

    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);
    assertNotNull(this.system.getStatSampler());
    assertNotNull(this.system.getStatSampler().waitForSampleCollector(TIMEOUT));

    this.cache = new CacheFactory().create();

  }

  public void testIssue51048() throws InterruptedException {
    SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();

    CachePerfStats cachePerfStats = ((GemFireCacheImpl) cache).getCachePerfStats();

    for (int i = 1; i <= 10; i++) {
      cachePerfStats.incCreates();
    }

    sample();

    service.getLocalManager().runManagementTaskAdhoc();
    assertTrue(dsmbean.getAverageWrites() == 10);

    sample();

    service.getLocalManager().runManagementTaskAdhoc();

    assertTrue(dsmbean.getAverageWrites() == 0);

  }

  public void tearDown() throws Exception {
    super.tearDown();
    // System.clearProperty("gemfire.stats.debug.debugSampleCollector");
    this.system.disconnect();
    this.system = null;
  }

  protected void waitForNotification() throws InterruptedException {
    this.system.getStatSampler().waitForSample(TIMEOUT);
    Thread.sleep(SLEEP);
  }

  protected void sample() throws InterruptedException {
    this.system.getStatSampler().getSampleCollector().sample(NanoTimer.getTime());
    Thread.sleep(SLEEP);
  }

}
