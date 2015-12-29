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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

public class ParallelWANPropagationLoopBackDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ParallelWANPropagationLoopBackDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Test loop back issue between 2 WAN sites (LN & NY). LN -> NY -> LN.
   * Site1 (LN): vm2, vm4, vm5
   * Site2 (NY): vm3, vm6, vm7
   */
  public void testParallelPropagationLoopBack() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 1, isOffHeap() });
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 1, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 1, isOffHeap() });
    
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 1, isOffHeap() });
    
    vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    //do one put on each site so the single bucket gets created
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm6.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    //now, the bucket has been created. Attach listener to it.
    vm2.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln", 1 });
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln", 1 });
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln", 1 });
    
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny", 1 });
    
    //do one more put on each site again
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=3; i< 4; i++) {
      keyValues.put(i, i);
    }
    vm6.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    //validate region sizes on the two sites
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 4 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 4 });
  
    pause(5000);
    vm2.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 0 });
    vm6.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 0 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 0 });
    
    Map vm2QueueMap = (HashMap)vm2.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 1});
    Map vm4QueueMap = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 1});
    Map vm5QueueMap = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 1});
    Map vm3QueueMap = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny", 1});
    Map vm6QueueMap = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny", 1});
    Map vm7QueueMap = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny", 1});
    
    List createList2 = (List)vm2QueueMap.get("Create0");
    List createList4 = (List)vm4QueueMap.get("Create0");
    List createList5 = (List)vm5QueueMap.get("Create0");
    List createList3 = (List)vm3QueueMap.get("Create0");
    List createList6 = (List)vm6QueueMap.get("Create0");
    List createList7 = (List)vm7QueueMap.get("Create0");
    
    List updateList2 = (List)vm2QueueMap.get("Update0");
    List updateList4 = (List)vm4QueueMap.get("Update0");
    List updateList5 = (List)vm5QueueMap.get("Update0");
    List updateList3 = (List)vm3QueueMap.get("Update0");
    List updateList6 = (List)vm6QueueMap.get("Update0");
    List updateList7 = (List)vm7QueueMap.get("Update0");
    
    assertEquals("createList for queue listener on site1 is incorrect", 1, ((createList2 != null) ? createList2.size() : 0) + 
        ((createList4 != null) ? createList4.size() : 0) + 
        ((createList5 != null) ? createList5.size() : 0));
    assertEquals("updateList for queue listener on site1 is incorrect", 0, ((updateList2 != null) ? updateList2.size() : 0) + 
        ((updateList4 != null) ? updateList4.size() : 0) + 
        ((updateList5 != null) ? updateList5.size() : 0));
    
    assertEquals("createList for queue listener on site2 is incorrect", 1, ((createList3 != null) ? createList3.size() : 0) + 
        ((createList6 != null) ? createList6.size() : 0) + 
        ((createList7 != null) ? createList7.size() : 0));
    assertEquals("updateList for queue listener on site2 is incorrect", 0, ((updateList3 != null) ? updateList3.size() : 0) + 
        ((updateList6 != null) ? updateList6.size() : 0) + 
        ((updateList7 != null) ? updateList7.size() : 0));

  }
  
  /**
   * Test loop back issue among 3 WAN sites with Ring topology i.e. LN -> NY -> TK -> LN
   * Site1 (LN): vm3, vm6
   * Site2 (NY): vm4, vm7
   * Site3 (TK): vm5
   */
  public void testParallelPropagationLoopBack3Sites() {
    //Create locators
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    
    //create cache and receivers
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    //create senders
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 3,
      true, 100, 10, false, false, null, true });
    
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk", 1,
      true, 100, 10, false, false, null, true });
    
    //create PR
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 1, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 1, isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk", 0, 1, isOffHeap() });
    
    //start senders
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk" });
    
    //do one put on each site to create a single bucket
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    //validate region size
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    
    //now, the bucket has been created. Add a listener to it
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln", 1 });
    
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny", 1 });
    
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk", 1 });
    
    //do one more put on each site again
    keyValues.clear();
    for(int i=3; i< 4; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=4; i< 5; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=5; i< 6; i++) {
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    //validate region size
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    
    pause(5000);
    vm6.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk", 0 });
    
    Map queueMap_3 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 1});
    Map queueMap_6 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 1});
    
    Map queueMap_4 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny", 1});
    Map queueMap_7 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny", 1});
    
    Map queueMap_5 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk", 1});

    
    List createList3 = (List)queueMap_3.get("Create0");
    List createList6 = (List)queueMap_6.get("Create0");
    
    List updateList3 = (List)queueMap_3.get("Update0");
    List updateList6 = (List)queueMap_6.get("Update0");
    
    List createList4 = (List)queueMap_4.get("Create0");
    List createList7 = (List)queueMap_7.get("Create0");
    
    List updateList4 = (List)queueMap_4.get("Update0");
    List updateList7 = (List)queueMap_7.get("Update0");
    
    List createList5 = (List)queueMap_5.get("Create0");
    List updateList5 = (List)queueMap_5.get("Update0");

    
    assertEquals(0, ((updateList3 != null) ? updateList3.size() : 0) + ((updateList6 != null) ? updateList6.size() : 0));
    assertEquals(0, ((updateList4 != null) ? updateList4.size() : 0) + ((updateList7 != null) ? updateList7.size() : 0));
    assertEquals(0, (updateList4 != null) ? updateList4.size() : 0);
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6)", 2, 
        ((createList3 != null) ? createList3.size() : 0) + ((createList6 != null) ? createList6.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7)", 2, 
        ((createList4 != null) ? createList4.size() : 0) + ((createList7 != null) ? createList7.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5)", 2, (createList5 != null) ? createList5.size() : 0);
  }
  
  /**
   * Test loop back issue among 3 WAN sites with N to N topology.
   * Puts are done to all the DSes.
   * LN site: vm3, vm6
   * NY site: vm4, vm7
   * TK site: vm5
   */
  public void testParallelPropagationLoopBack3SitesNtoNTopologyPutInAllDS() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    //site1
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });

    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    
    //site2
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });

    //site3
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
      true, 100, 10, false, false, null, true });

    //create PR
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk1,tk2", 0, 1, isOffHeap() });

    //start all the senders
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });

    pause(2000);
    
    //do single put in each site, so, the buckets will be created
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    
    //now, the buckets are created. Attach listener to each bucket on each site
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln1", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln1", 1 });
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln2", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln2", 1 });
    
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk1", 1 });
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk2", 1 });
    
    //bucket listeners are now attached. Do one more put on each site 
    keyValues.clear();
    for(int i=3; i< 4; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=4; i< 5; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=5; i< 6; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });

    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln1", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny1", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk1", 0 });
    
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln2", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny2", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk2", 0 });
    
    Map queueMap_3_1 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln1", 1});
    Map queueMap_6_1 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln1", 1});
    Map queueMap_3_2 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln2", 1});
    Map queueMap_6_2 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln2", 1});
    
    Map queueMap_4_1 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_7_1 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_4_2 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    Map queueMap_7_2 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    
    Map queueMap_5_1 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk1", 1});
    Map queueMap_5_2 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk2", 1});
    
    List createList3_1 = (List)queueMap_3_1.get("Create0");
    List createList6_1 = (List)queueMap_6_1.get("Create0");
    List createList3_2 = (List)queueMap_3_2.get("Create0");
    List createList6_2 = (List)queueMap_6_2.get("Create0");

    List updateList3_1 = (List)queueMap_3_1.get("Update0");
    List updateList6_1 = (List)queueMap_6_1.get("Update0");
    List updateList3_2 = (List)queueMap_3_2.get("Update0");
    List updateList6_2 = (List)queueMap_6_2.get("Update0");
    
    List createList4_1 = (List)queueMap_4_1.get("Create0");
    List createList7_1 = (List)queueMap_7_1.get("Create0");
    List createList4_2 = (List)queueMap_4_2.get("Create0");
    List createList7_2 = (List)queueMap_7_2.get("Create0");
    
    List updateList4_1 = (List)queueMap_4_1.get("Update0");
    List updateList7_1 = (List)queueMap_7_1.get("Update0");
    List updateList4_2 = (List)queueMap_4_2.get("Update0");
    List updateList7_2 = (List)queueMap_7_2.get("Update0");
    
    List createList5_1 = (List)queueMap_5_1.get("Create0");
    List createList5_2 = (List)queueMap_5_2.get("Create0");
    List updateList5_1 = (List)queueMap_5_1.get("Update0");
    List updateList5_2 = (List)queueMap_5_2.get("Update0");
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) sender1", 1, 
        ((createList3_1 != null) ? createList3_1.size() : 0) + ((createList6_1 != null) ? createList6_1.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender1", 1, 
        ((createList4_1 != null) ? createList4_1.size() : 0) + ((createList7_1 != null) ? createList7_1.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender1", 1, 
        ((createList5_1 != null) ? createList5_1.size() : 0));
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) sender2", 1, 
        ((createList3_2 != null) ? createList3_2.size() : 0) + ((createList6_2 != null) ? createList6_2.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender2", 1, 
        ((createList4_2 != null) ? createList4_2.size() : 0) + ((createList7_2 != null) ? createList7_2.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender2", 1, 
        ((createList5_2 != null) ? createList5_2.size() : 0));
    
    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) sender1", 0, 
        ((updateList3_1 != null) ? updateList3_1.size() : 0) + ((updateList6_1 != null) ? updateList6_1.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender1", 0, 
        ((updateList4_1 != null) ? updateList4_1.size() : 0) + ((updateList7_1 != null) ? updateList7_1.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender1", 0, 
        ((updateList5_1 != null) ? updateList5_1.size() : 0));

    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) sender2", 0, 
        ((updateList3_2 != null) ? updateList3_2.size() : 0) + ((updateList6_2 != null) ? updateList6_2.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender2", 0, 
        ((updateList4_2 != null) ? updateList4_2.size() : 0) + ((updateList7_2 != null) ? updateList7_2.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender2", 0, 
        ((updateList5_2 != null) ? updateList5_2.size() : 0));
  }
  
  /**
   * Test loop back issue among 3 WAN sites with N to N topology.
   * Puts are done to only one DS.
   * LN site: vm3, vm6
   * NY site: vm4, vm7
   * TK site: vm5
   */

  public void testParallelPropagationLoopBack3SitesNtoNTopologyPutFromOneDS() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    //site1
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });

    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    
    //site2
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });

    //site3
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
      true, 100, 10, false, false, null, true });

    //create PR
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk1,tk2", 0, 1, isOffHeap() });

    //start all the senders
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });

    //do one round of puts in order to create bucket
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });

    //now, the buckets are created. Attach listener to each bucket on each site
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln1", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln1", 1 });
    vm3.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln2", 1 });
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln2", 1 });
    
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk1", 1 });
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk2", 1 });
    
    //bucket listeners are now attached. Do more puts 
    keyValues.clear();
    for(int i=3; i< 4; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=4; i< 5; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=5; i< 6; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });

    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln1", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny1", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk1", 0 });
    
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln2", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny2", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk2", 0 });
    
    Map queueMap_3_1 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln1", 1});
    Map queueMap_6_1 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln1", 1});
    Map queueMap_3_2 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln2", 1});
    Map queueMap_6_2 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln2", 1});
    
    Map queueMap_4_1 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_7_1 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_4_2 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    Map queueMap_7_2 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    
    Map queueMap_5_1 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk1", 1});
    Map queueMap_5_2 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk2", 1});
    
    List createList3_1 = (List)queueMap_3_1.get("Create0");
    List createList6_1 = (List)queueMap_6_1.get("Create0");
    List createList3_2 = (List)queueMap_3_2.get("Create0");
    List createList6_2 = (List)queueMap_6_2.get("Create0");

    List updateList3_1 = (List)queueMap_3_1.get("Update0");
    List updateList6_1 = (List)queueMap_6_1.get("Update0");
    List updateList3_2 = (List)queueMap_3_2.get("Update0");
    List updateList6_2 = (List)queueMap_6_2.get("Update0");
    
    List createList4_1 = (List)queueMap_4_1.get("Create0");
    List createList7_1 = (List)queueMap_7_1.get("Create0");
    List createList4_2 = (List)queueMap_4_2.get("Create0");
    List createList7_2 = (List)queueMap_7_2.get("Create0");
    
    List updateList4_1 = (List)queueMap_4_1.get("Update0");
    List updateList7_1 = (List)queueMap_7_1.get("Update0");
    List updateList4_2 = (List)queueMap_4_2.get("Update0");
    List updateList7_2 = (List)queueMap_7_2.get("Update0");
    
    List createList5_1 = (List)queueMap_5_1.get("Create0");
    List createList5_2 = (List)queueMap_5_2.get("Create0");
    List updateList5_1 = (List)queueMap_5_1.get("Update0");
    List updateList5_2 = (List)queueMap_5_2.get("Update0");
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) sender1", 3, 
        ((createList3_1 != null) ? createList3_1.size() : 0) + ((createList6_1 != null) ? createList6_1.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender1", 0, 
        ((createList4_1 != null) ? createList4_1.size() : 0) + ((createList7_1 != null) ? createList7_1.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender1", 0, 
        ((createList5_1 != null) ? createList5_1.size() : 0));
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) sender2", 3, 
        ((createList3_2 != null) ? createList3_2.size() : 0) + ((createList6_2 != null) ? createList6_2.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender2", 0, 
        ((createList4_2 != null) ? createList4_2.size() : 0) + ((createList7_2 != null) ? createList7_2.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender2", 0, 
        ((createList5_2 != null) ? createList5_2.size() : 0));
    
    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) sender1", 0, 
        ((updateList3_1 != null) ? updateList3_1.size() : 0) + ((updateList6_1 != null) ? updateList6_1.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender1", 0, 
        ((updateList4_1 != null) ? updateList4_1.size() : 0) + ((updateList7_1 != null) ? updateList7_1.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender1", 0, 
        ((updateList5_1 != null) ? updateList5_1.size() : 0));

    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) sender2", 0, 
        ((updateList3_2 != null) ? updateList3_2.size() : 0) + ((updateList6_2 != null) ? updateList6_2.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender2", 0, 
        ((updateList4_2 != null) ? updateList4_2.size() : 0) + ((updateList7_2 != null) ? updateList7_2.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender2", 0, 
        ((updateList5_2 != null) ? updateList5_2.size() : 0));
  }
  
  /**
   * Test loop back issue among 3 WAN sites with N to N topology.
   * Puts are done to all the DSes.
   * One of the sites uses old GatewayHub API and other two use new GatewaySender.
   * LN site: vm3, vm6 (GatewayHub)
   * NY site: vm4, vm7 (GatewaySender)
   * TK site: vm5 (GatewaySender)
   * 
   * NOTE: This configuration has a loop back issue and test case fails. Raised defect # 44483.
   */
  public void Defect44398_testParallelPropagationLoopBack3SitesWithGatewayHub() {
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
    
    //do single put in each site, so, the buckets will be created
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    getLogWriter().info("Done one round of puts on all the sites");
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3 });
    
    getLogWriter().info("Validated region size on all the sites");
    
    //now, the buckets are created. Attach listener to each bucket on each site
    //for site1, there are no shadow buckets since it uses GatewayHub. Attach listener to GatewayHub
    vm3.invoke(WANTestBase.class, "addListenerOnGateway", new Object[] {"GatewayHubln"});
    
    
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny1", 1 });
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ny2", 1 });
    
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk1", 1 });
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "tk2", 1 });
    
    //listeners are now attached. Do one more put on each site 
    keyValues.clear();
    for(int i=3; i< 4; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=4; i< 5; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    keyValues.clear();
    for(int i=5; i< 6; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_PR",
      keyValues });
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 6 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
     testName + "_PR", 6 });

    pause(5000);
    
    Map gatewayMap_3 = (HashMap)vm3.invoke(WANTestBase.class, "checkGateways", new Object[] {"GatewayHubln"});
    
    Map queueMap_4_1 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_7_1 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny1", 1});
    Map queueMap_4_2 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    Map queueMap_7_2 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ny2", 1});
    
    Map queueMap_5_1 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk1", 1});
    Map queueMap_5_2 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"tk2", 1});
    
    List createList3_1 = (ArrayList)gatewayMap_3.get("Create0");
    List createList3_2 = (ArrayList)gatewayMap_3.get("Create1");

    List updateList3_1 = (ArrayList)gatewayMap_3.get("Update0");
    List updateList3_2 = (ArrayList)gatewayMap_3.get("Update1");
    
    List createList4_1 = (ArrayList)queueMap_4_1.get("Create0");
    List createList7_1 = (ArrayList)queueMap_7_1.get("Create0");
    List createList4_2 = (ArrayList)queueMap_4_2.get("Create0");
    List createList7_2 = (ArrayList)queueMap_7_2.get("Create0");
    
    List updateList4_1 = (ArrayList)queueMap_4_1.get("Update0");
    List updateList7_1 = (ArrayList)queueMap_7_1.get("Update0");
    List updateList4_2 = (ArrayList)queueMap_4_2.get("Update0");
    List updateList7_2 = (ArrayList)queueMap_7_2.get("Update0");
    
    List createList5_1 = (ArrayList)queueMap_5_1.get("Create0");
    List createList5_2 = (ArrayList)queueMap_5_2.get("Create0");
    List updateList5_1 = (ArrayList)queueMap_5_1.get("Update0");
    List updateList5_2 = (ArrayList)queueMap_5_2.get("Update0");
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) gateway1", 1, 
        ((createList3_1 != null) ? createList3_1.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender1", 1, 
        ((createList4_1 != null) ? createList4_1.size() : 0) + ((createList7_1 != null) ? createList7_1.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender1", 1, 
        ((createList5_1 != null) ? createList5_1.size() : 0));
    
    assertEquals("createList size is incorrect for site1 (vm3 & vm6) gateway2", 1, 
        ((createList3_2 != null) ? createList3_2.size() : 0));
    assertEquals("createList size is incorrect for site2 (vm4 & vm7) sender2", 1, 
        ((createList4_2 != null) ? createList4_2.size() : 0) + ((createList7_2 != null) ? createList7_2.size() : 0));
    assertEquals("createList size is incorrect for site3 (vm5) sender2", 1, 
        ((createList5_2 != null) ? createList5_2.size() : 0));
    
    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) gateway1", 0, 
        ((updateList3_1 != null) ? updateList3_1.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender1", 0, 
        ((updateList4_1 != null) ? updateList4_1.size() : 0) + ((updateList7_1 != null) ? updateList7_1.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender1", 0, 
        ((updateList5_1 != null) ? updateList5_1.size() : 0));

    assertEquals("updateList size is incorrect for site1 (vm3 & vm6) gateway2", 0, 
        ((updateList3_2 != null) ? updateList3_2.size() : 0));
    assertEquals("updateList size is incorrect for site2 (vm4 & vm7) sender2", 0, 
        ((updateList4_2 != null) ? updateList4_2.size() : 0) + ((updateList7_2 != null) ? updateList7_2.size() : 0));
    assertEquals("updateList size is incorrect for site3 (vm5) sender2", 0, 
        ((updateList5_2 != null) ? updateList5_2.size() : 0));
  }


}
