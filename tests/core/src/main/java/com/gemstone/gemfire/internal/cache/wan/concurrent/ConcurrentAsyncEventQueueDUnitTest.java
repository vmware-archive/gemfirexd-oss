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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

import dunit.AsyncInvocation;

/**
 * @author skumar
 *
 */
public class ConcurrentAsyncEventQueueDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ConcurrentAsyncEventQueueDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testConcurrentSerialAsyncEventQueueAttributes() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, true, "testDS", true, 5, OrderPolicy.THREAD });

    vm4.invoke(WANTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.THREAD });
  }
  
 
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyKey() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 150, true, true, "testDS", true, 5, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.KEY });
  }

  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyPartition() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 150, true, true, "testDS", true, 5, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.PARTITION });
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */

  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
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
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
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
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: Thread ordering
   */

  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyThread() {
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

    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        500 });
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "doNextPuts", new Object[] { testName + "_RR",
      500, 1000 });
    AsyncInvocation inv3 = vm4.invokeAsync(WANTestBase.class, "doNextPuts", new Object[] { testName + "_RR",
      1000, 1500 });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
    } catch (InterruptedException ie) {
      fail(
          "Cought interrupted exception while waiting for the task tgo complete.",
          ie);
    }
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1500 });// primary sender
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
   * Region: PartitionedRegion 
   * WAN: Parallel
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */

  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
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
  
    assertEquals(vm4size + vm5size + vm6size + vm7size, 1000);
  
  }
  
  
  /**
   * Test configuration::
   * 
   * Region: PartitionedRegion 
   * WAN: Parallel
   * Dispatcher threads: more than 1
   * Order policy: PARTITION based ordering
   */

  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyPartition() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 100, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 100, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 100, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 100, true, false, null, false, 3,
            OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
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

    assertEquals(1000, vm4size + vm5size + vm6size + vm7size);
  }
}
