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
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

import dunit.AsyncInvocation;

public class AsyncEventListenerDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public AsyncEventListenerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test to verify that AsyncEventQueue can not be created when null listener
   * is passed.
   */
  public void testCreateAsyncEventQueueWithNullListener() {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    AsyncEventQueueFactory asyncQueueFactory = cache
        .createAsyncEventQueueFactory();
    try {
      asyncQueueFactory.create("testId", null);
      fail("AsyncQueueFactory should not allow to create AsyncEventQueue with null listener");
    }
    catch (IllegalArgumentException e) {
      // expected
    }

  }

  public void testSerialAsyncEventQueueAttributes() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, true, "testDS", true });

    vm4.invoke(WANTestBase.class, "validateAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true });
  }
  
  public void testSerialAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause at least for the batchTimeInterval

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    int vm4size = (Integer)vm4.invoke(WANTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(WANTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */

  public void testReplicatedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Region persistence enabled: false 
   * Async queue persistence enabled: false
   * 
   * Error is thrown from AsyncEventListener implementation while processing the batch.
   * Added to test the fix done for defect #45152.
   */

  public void testReplicatedSerialAsyncEventQueue_ExceptionScenario() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    
    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(2000);// pause at least for the batchTimeInterval

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        100 });
    
    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 100 });// primary sender
    vm5.invoke(WANTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  public void testReplicatedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause at least for the batchTimeInterval

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_RR", keyValues });

    pause(1000);
    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_RR", updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_RR", updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Number of WAN sites: 2 Region persistence
   * enabled: false Async channel persistence enabled: false
   */
  public void testReplicatedSerialAsyncEventQueueWith2WANSites() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    // ------------ START - CREATE CACHE, REGION ON LOCAL SITE ------------//
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "lnAsync", false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "lnAsync", false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "lnAsync", false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "lnAsync", false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ln", "lnAsync", isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ln", "lnAsync", isOffHeap() });
    vm6.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ln", "lnAsync", isOffHeap() });
    vm7.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ln", "lnAsync", isOffHeap() });
    // ------------- END - CREATE CACHE, REGION ON LOCAL SITE -------------//

    // ------------- START - CREATE CACHE ON REMOTE SITE ---------------//
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
        false, 100, 10, false, false, null, true });
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "nyAsync", false, 100, 100, false, false, null, false });
    vm3.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] {
        "nyAsync", false, 100, 100, false, false, null, false });

    vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });

    vm2.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ny", "nyAsync", isOffHeap() });
    vm3.invoke(WANTestBase.class,
        "createReplicatedRegionWithSenderAndAsyncEventQueue", new Object[] {
            testName + "_RR", "ny", "nyAsync", isOffHeap() });

    // ------------- END - CREATE CACHE, REGION ON REMOTE SITE -------------//

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    // validate AsyncEventListener on local site
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "lnAsync", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "lnAsync", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "lnAsync", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "lnAsync", 0 });// secondary

    // validate region size on remote site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

    // validate AsyncEventListener on remote site
    vm2.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "nyAsync", 1000 });// primary sender
    vm3.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "nyAsync", 0 });// secondary

  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * event queue persistence enabled: false
   * 
   * Note: The test doesn't create a locator but uses MCAST port instead.
   */

  public void DISABLED_testReplicatedSerialAsyncEventQueueWithoutLocator() {
    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(WANTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm5.invoke(WANTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm6.invoke(WANTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm7.invoke(WANTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */

  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */

  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(WANTestBase.class,
        "createAsyncEventQueueWithDiskStore", new Object[] { "ln", false, 100,
            100, true, null });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    // pause async channel and then do the puts
    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    // ------------------ KILL VM4 AND REBUILD
    // ------------------------------------------
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, firstDStore });
    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    // -----------------------------------------------------------------------------------

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There are 3 VMs in the site and the VM with primary sender is shut down.
   */
  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart2() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm4.invoke(WANTestBase.class, "addCacheListenerAndCloseCache",
        new Object[] { testName + "_RR" });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm5.invoke(WANTestBase.class, "doPuts",
        new Object[] { testName + "_RR", 2000 });

    // -----------------------------------------------------------------------------------
    vm5.invoke(WANTestBase.class, "waitForSenderToBecomePrimary",
        new Object[] { AsyncEventQueueImpl
            .getSenderIdFromAsyncEventQueueId("ln") });
    
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    int vm4size = (Integer)vm4.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });

    getLogWriter().info("vm4 size is: " + vm4size);
    getLogWriter().info("vm5 size is: " + vm5size);
    // verify that there is no event loss
    assertTrue(
        "Total number of entries in events map on vm4 and vm5 should be at least 2000",
        (vm4size + vm5size) >= 2000);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */
  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreads() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */
  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreads_2() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm6.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm7.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        500 });
    vm4.invokeAsync(WANTestBase.class, "doNextPuts", new Object[] { testName + "_RR",
      500, 1000 });
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
      1000, 1500 });
    
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */
  public void testPartitionedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        500 });
    vm5.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
        testName + "_PR", 500, 1000 });
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  public void testPartitionedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    
    pause(2000);

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", keyValues });

    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", updateKeyValues });

    vm5.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
      testName + "_PR", updateKeyValues });

    vm5.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
      "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        500 });
    vm5.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
        testName + "_PR", 500, 1000 });
    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(WANTestBase.class,
        "createAsyncEventQueueWithDiskStore", new Object[] { "ln", false, 100,
            100, true, null });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    // pause async channel and then do the puts
    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueueAndWaitForDispatcherToPause",
            new Object[] { "ln" });
  
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });

    // ------------------ KILL VM4 AND REBUILD
    // ------------------------------------------
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, firstDStore });
    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    // -----------------------------------------------------------------------------------

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
  }

  public void testParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        256 });
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);
  }
  
  public void testParallelAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause at least for the batchTimeInterval

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });

    int vm4size = (Integer)vm4.invoke(WANTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(WANTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }

  public void testParallelAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });

    pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", keyValues });

    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", updateKeyValues });

 
    vm4.invoke(WANTestBase.class, "waitForAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, keyValues.size());
  }

  /**
   * Added to reproduce defect #47213
   */
  public void testParallelAsyncEventQueueWithConflationEnabled_bug47213() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });

    vm4.invoke(WANTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });

    pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", keyValues });

    pause(2000);
    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName + "_PR", updateKeyValues });

    // pause to ensure that events have been conflated.
    pause(2000);
    vm4.invoke(WANTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, keyValues.size());
    
  }

  public void testParallelAsyncEventQueueWithOneAccessor() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm3.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm3.invoke(WANTestBase.class,
        "createPartitionedRegionAccessorWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln" });
    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        256 });
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    vm3.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);

  }

  public void testParallelAsyncEventQueueWithPersistence() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        256 });
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);
  }
  
  public void testReplicatedParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    int vm4size = (Integer)vm4.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm6size = (Integer)vm6.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm7size = (Integer)vm7.invoke(WANTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });

    assertEquals(vm4size + vm5size + vm6size + vm7size, 1000);
  }
  
/**
 * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm5 is
 * killed so the buckets hosted by it are shifted to vm4.
 */
  public void testParallelAsyncEventQueueHA_Scenario1() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
      "createFirstLocatorWithDSId", new Object[] { 1 });
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    getLogWriter().info("Created the cache");

    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });

    getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 80 });

    getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsVm5 = (Set<Integer>)vm5.invoke(
        WANTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { testName + "_PR" });

    getLogWriter().info("Primary buckets on vm5: " + primaryBucketsVm5);
    // ---------------------------- Kill vm5 --------------------------
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});

    pause(1000);// give some time for rebalancing to happen
    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm5, 5 });
  }

  /**
   * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm5 is
   * killed and subsequently vm6 is brought up. Buckets are now rebalanced
   * between vm4 & vm6.
   */
  public void testParallelAsyncEventQueueHA_Scenario2() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    getLogWriter().info("Created the cache");

    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });

    getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 80 });

    getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsVm5 = (Set<Integer>)vm5.invoke(
        WANTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { testName + "_PR" });

    getLogWriter().info("Primary buckets on vm5: " + primaryBucketsVm5);
    // ---------------------------- Kill vm5 --------------------------
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
    // ----------------------------------------------------------------

    // ---------------------------- start vm6 --------------------------
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm6.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });

    // ------------------------------------------------------------------

    pause(1000);// give some time for rebalancing to happen
    Set<Integer> primaryBucketsVm6 = (Set<Integer>)vm6.invoke(
        WANTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { testName + "_PR" });

    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm6.invoke(WANTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm6, 5 });
  }

  /**
   * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm6 is
   * brought up and rebalancing is triggered so the buckets get balanced among
   * vm4, vm5 & vm6.
   */
  public void testParallelAsyncEventQueueHA_Scenario3() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    getLogWriter().info("Created the cache");

    vm4.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });

    getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(WANTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 80 });

    getLogWriter().info("Done puts");

    // ---------------------------- start vm6 --------------------------
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm6.invoke(WANTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            testName + "_PR", "ln", isOffHeap() });

    // ------------------------------------------------------------------
    vm4.invoke(WANTestBase.class, "doRebalance", new Object[] {});
    pause(2000);// give some time for rebalancing to happen

    Set<Integer> primaryBucketsVm6 = (Set<Integer>)vm6.invoke(
        WANTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { testName + "_PR" });
    getLogWriter().info("Primary buckets on vm6: " + primaryBucketsVm6);
    vm4.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm6.invoke(WANTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm6, 5 });
  }

  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have that same AEQ
   */
  public void testParallelAsyncEventQueueAttachedToChildRegionButNotToParentRegion() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
      "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
      "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create cache on node
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //create AsyncEventQueue on node
    vm3.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
      true, 100, 10, false, false, null, true });

    //create leader (parent) PR on node
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "PARENT_PR", null, 0, 100, isOffHeap() });
    String parentRegionFullPath =
      (String) vm3.invoke(WANTestBase.class, "getRegionFullPath", new Object[] { testName + "PARENT_PR"});

    //create colocated (child) PR on node
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegionWithAsyncEventQueue", new Object[] {
        testName + "CHILD_PR", "ln", 100, parentRegionFullPath, isOffHeap() });

    //do puts in colocated (child) PR on node
    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "CHILD_PR", 1000 });

    //wait for AsyncEventQueue to get empty on node
    vm3.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
      new Object[] { "ln" });

    //verify the events in listener
    int vm3size = (Integer)vm3.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
            new Object[] { "ln"});
    assertEquals(vm3size, 1000);
  }
}
