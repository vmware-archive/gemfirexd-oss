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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase.MyGatewayEventFilter;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;

import dunit.AsyncInvocation;

public class ParallelWANPropogationDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANPropogationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void test_ParallelGatewaySenderMetaRegionNotExposedToUser_Bug44216() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createCache(lnPort);
    createSender("ln", 2, true, 100, 300, false, false,
        null, true);
    createPartitionedRegion(testName + "_PR", "ln", 1, 100, isOffHeap());

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals("ln")) {
        sender = s;
        break;
      }
    }
    try {
      sender.start();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed with IOException");
    }

    GemFireCacheImpl gemCache = (GemFireCacheImpl)cache;
    Set regionSet = gemCache.rootRegions();

    for (Object r : regionSet) {
      if (((Region)r).getName().equals(
          ((ParallelGatewaySenderImpl)sender).getQueues().toArray(new RegionQueue[1])[0].getRegion().getName())) {
        fail("The shadowPR is exposed to the user");
      }
    }
  }
  
  public void testParallelPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //keep a larger batch to minimize number of exception occurrences in the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false});
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    
    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });

    
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    // Just making sure that though the remote site is started later,
    // remote site is still able to get the data. Since the receivers are
    // started before creating partition region it is quite possible that the
    // region may loose some of the events. This needs to be handled by the code
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  /**
   * Normal happy scenario test case.
   * @throws Exception
   */
  public void testParallelPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  public void testParallelPropagation_ManualStart() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  /**
   * Normal happy scenario test case2.
   * @throws Exception
   */
  public void testParallelPropagationPutBeforeSenderStart() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });
    
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation inv4 = vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });

    try{
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    }
    catch(InterruptedException ie) {
      fail("Caught interrupted exception");
    }
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  /**
   * Local and remote sites are up and running.
   * Local site cache is closed and the site is built again.
   * Puts are done to local site.
   * Expected: Remote site should receive all the events put after the local
   * site was built back.
   * 
   * @throws Exception
   */
  public void testParallelPropagationWithLocalCacheClosedAndRebuilt() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
              testName + "_PR", 1000 });
    //-------------------Close and rebuild local site ---------------------------------

    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
    
    Integer regionSize = 
      (Integer) vm2.invoke(WANTestBase.class, "getRegionSize", new Object[] {testName + "_PR" });
    getLogWriter().info("Region size on remote is: " + regionSize);
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    //------------------------------------------------------------------------------------
    
    addExpectedException(EntryExistsException.class.getName());
    addExpectedException(BatchException70.class.getName());
    addExpectedException(ServerOperationException.class.getName());
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 1000 });
  }
  
  public void testParallelColocatedPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 1000 });
  }
  /**
   * Create colocated partitioned regions.
   * Parent region has PGS attached and child region doesn't.
   * 
   * Validate that events for parent region reaches remote site.
   * 
   * @throws Exception
   */

  public void testParallelColocatedPropagation2() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegions2",
        new Object[] { testName, null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName+"_child1", 1000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName+"_child2", 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName+"_child1", 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName+"_child2", 0 });
  }

  
  public void testParallelPropagationWihtOverflow() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    //let all the senders start before doing any puts to ensure that none of the events is lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] { testName, 150 });

    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 150 });
  }

  public void testSerialReplicatedAndParallePartitionedPropagation()
      throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial",
        2, false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial",
        2, false, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel",
        2, true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel",
        2, true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel",
        2, true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel",
        2, true, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial", isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  public void testPartitionedParallelPropagationToTwoWanSites()
      throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel1",
        2, true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel1",
        2, true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel1",
        2, true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel1",
        2, true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel2",
        3, true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel2",
        3, true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel2",
        3, true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "lnParallel2",
        3, true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel1,lnParallel2", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel1,lnParallel2", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel1,lnParallel2", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "lnParallel1,lnParallel2", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel1" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel2" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "lnParallel2" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing puts, make sure that the senders are started.
    //this will ensure that not a single events is lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel1" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel1" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel1" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel1" });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel2" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel2" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel2" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "lnParallel2" });
    
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel1"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel1"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel1"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel1"});
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel2"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel2"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel2"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"lnParallel2"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  public void testPartitionedParallelPropagationHA() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    AsyncInvocation inv1 = vm7.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 5000 });
    pause(500);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 10000 });
    pause(1500);
    AsyncInvocation inv4 = vm5.invokeAsync(WANTestBase.class, "killSender");
    inv1.join();
    inv2.join();
    inv3.join();
    inv4.join();
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
    
    //verify all buckets drained on the sender nodes that up and running.
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
  }

  public void testParallelPropagationWithFilter() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    //wait for senders to be running before doing any puts. This will ensure that
    //not a single events is lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
  }
  
  
  public void testParallelPropagationWithPutAll() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPutAll", new Object[] { testName + "_PR",
        100 , 50 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 5000 });
  
  }
  
  /**
   * There was a bug that all destroy events were being put into different buckets of sender queue
   * against the key 0. Bug# 44304
   *  
   * @throws Exception
   */
  public void testParallelPropagationWithDestroy() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 100, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 100, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 100, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 100, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    pause(2000);
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    vm4.invoke(WANTestBase.class, "doDestroys", new Object[] { testName + "_PR", 500 });
    
    
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueBucketSize", new Object[] { "ln", 15 });
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueBucketSize", new Object[] { "ln", 15 });
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueBucketSize", new Object[] { "ln", 15 });
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueBucketSize", new Object[] { "ln", 15 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    //give some time for the queue to drain
    pause(5000);
    
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 500 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 500 });
  
  }
  
  /**
   * Test method to test the mixed WAN configuration. 
   * i.e. out of 3 sites, 2 sites will have GatewaySender while 
   * 1 site will have GatewayHub configuration.
   * Events will be put on all the 3 sites and eventually, each site
   * should have same number of region entries. 
   */
  public void testParallelPropagationForMixedWANConfiguration() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    getLogWriter().info("Created the locators");
    
    Integer vm3ReceiverPort = (Integer) vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    Integer vm4ReceiverPort = (Integer) vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    Integer vm5ReceiverPort = (Integer) vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
  
    getLogWriter().info("Created the cache and receivers");
    
    //site1
    Map<String, Integer> gatewayInfo = new HashMap<String, Integer>();
    gatewayInfo.put("ny", vm4ReceiverPort);
    gatewayInfo.put("tk", vm5ReceiverPort);
    vm3.invoke(WANTestBase.class, "createGatewayHub", new Object[] {"GatewayHubln", gatewayInfo});
    
    getLogWriter().info("Created GatewayHub on site1");
    
    //site2
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });

    getLogWriter().info("Created senders on site2");
    
    //site3
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
      true, 100, 10, false, false, null, true });

    getLogWriter().info("Created senders on site3");
    
    //create PR on site1
    vm3.invoke(WANTestBase.class, "createPartitionedRegion_WithGatewayEnabled", new Object[] {
      testName + "_PR", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion_WithGatewayEnabled", new Object[] {
      testName + "_PR", 0, 1, isOffHeap() });
    
    getLogWriter().info("Created PR on site1");
    
    //create PR on site2
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    
    getLogWriter().info("Created PR on site2");
    
    //create PR on site3
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk1,tk2", 0, 1, isOffHeap() });

    getLogWriter().info("Created PR on site3");
    
    //start senders on site2 and site3
    vm3.invoke(WANTestBase.class, "startGatewayHub", new Object[] {"GatewayHubln"});
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });

    getLogWriter().info("Started senders on site2 and site3");
    
    pause(2000);

    //do some puts on site1 (the site with GatewayHub)
    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 300 });
    //do next puts on site2 (the site with GatewaySender)
    vm4.invoke(WANTestBase.class, "doNextPuts", new Object[] { testName + "_PR", 300, 600 });
    //do next puts on site3
    vm5.invoke(WANTestBase.class, "doNextPuts", new Object[] { testName + "_PR", 600, 1000 });
    
    getLogWriter().info("Done puts on site1, site2 and site3");
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 1000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 1000 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
     testName + "_PR", 1000 });
  }
  
  /**
   * Normal happy scenario test case. But with Tx operations
   * @throws Exception
   */
  public void testParallelPropagationTxOperations() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    //vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    //vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    //vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
    //    true, 100, 10, false, false, null, true });
    //vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
    //    true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
//    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
//        testName + "_PR", "ln", true, 1, 100, isOffHeap() });
//    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
//        testName + "_PR", "ln", true, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
//    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
//    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
//    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
//    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "doTxPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
//    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
//    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 3 });
  }

  public void testParallelGatewaySenderQueueLocalSize() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    pause(1000);
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        10 });
    
    vm4.invoke(WANTestBase.class, "checkPRQLocalSize", new Object[] { "ln", 5 });
    vm5.invoke(WANTestBase.class, "checkPRQLocalSize", new Object[] { "ln", 5 });
    
  }
  
  public void testParallelGatewaySenderQueueLocalSizeWithHA() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    pause(1000);
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        10 });
    
    vm4.invoke(WANTestBase.class, "checkPRQLocalSize", new Object[] { "ln", 5 });
    vm5.invoke(WANTestBase.class, "checkPRQLocalSize", new Object[] { "ln", 5 });
    
    vm5.invoke(WANTestBase.class, "killSender", new Object[] { });
    
    pause(1000);
    vm4.invoke(WANTestBase.class, "checkPRQLocalSize", new Object[] { "ln", 10 });
    
  }

  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have that same AEQ
   */
  public void testParallelSenderAttachedToChildRegionButNotToParentRegion() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
      "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
      "createFirstRemoteLocator", new Object[] { 2, lnPort });
   
    //create cache and receiver on site2
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //create cache on site1
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
   
    //create sender on site1
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
  
    //start sender on site1
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
  
    //create leader (parent) PR on site1
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "PARENT_PR", null, 0, 100, isOffHeap() });
    String parentRegionFullPath =
      (String) vm3.invoke(WANTestBase.class, "getRegionFullPath", new Object[] { testName + "PARENT_PR"});
  
    //create colocated (child) PR on site1
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegion", new Object[] {
        testName + "CHILD_PR", "ln", 0, 100, parentRegionFullPath, isOffHeap() });

    //create leader and colocated PR on site2
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "PARENT_PR", null, 0, 100, isOffHeap() });
    vm2.invoke(WANTestBase.class, "createColocatedPartitionedRegion", new Object[] {
        testName + "CHILD_PR", null, 0, 100, parentRegionFullPath, isOffHeap() });

    //do puts in colocated (child) PR on site1
    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "CHILD_PR", 1000 });

    //verify the puts reach site2
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "CHILD_PR", 1000 });
  }
}
