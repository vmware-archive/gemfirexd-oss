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

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.NanoTimer;

import junit.framework.TestCase;

/**
 * Base test case for the management.bean.stats tests.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public abstract class MBeanStatsTestCase extends TestCase {

  protected static final long SLEEP = 100;
  protected static final long TIMEOUT = 4*1000;
  
  protected InternalDistributedSystem system;

  public MBeanStatsTestCase(String name) {
    super(name);
  }
  
  @SuppressWarnings("deprecation")
  public void setUp() throws Exception {
    super.setUp();
    //System.setProperty("gemfire.stats.debug.debugSampleCollector", "true");

    final Properties props = new Properties();
    //props.setProperty("log-level", "finest");
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-sampling-enabled", "false");
    props.setProperty("statistic-sample-rate", "60000");
    
    this.system = (InternalDistributedSystem) DistributedSystem.connect(props);
    assertNotNull(this.system.getStatSampler());
    assertNotNull(this.system.getStatSampler().waitForSampleCollector(TIMEOUT));
    
    new CacheFactory().create();
    
    init();
    
    sample();
  }

  public void tearDown() throws Exception {
    super.tearDown();
    //System.clearProperty("gemfire.stats.debug.debugSampleCollector");
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
  
  protected abstract void init();
}
