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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class SingleHopGetAllPutAllDUnitTest extends PRClientServerTestBase{


  private static final long serialVersionUID = 3873751456134028508L;
  
  public SingleHopGetAllPutAllDUnitTest(String name) {
    super(name);
    
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
 
  /*
   * Do a getAll from client and see if all the values are returned.
   * Will also have to see if the function was routed from client to all the servers
   * hosting the data. 
   */
  public void testServerGetAllFunction(){
    createScenario();
    client.invoke(SingleHopGetAllPutAllDUnitTest.class,
        "getAll");
  }

  private void createScenario() {
    ArrayList commonAttributes =  createCommonServerAttributes("TestPartitionedRegion", null, 1, 13, null);
    createClientServerScenarioSingleHop(commonAttributes,20, 20, 20);
  }

  public static void getAll() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final List testKeysList = new ArrayList();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testKeysList.iterator(); i.hasNext();) {
        Integer val = new Integer(j++);
        Object key = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }

      // check if the client meta-data is in synch
      verifyMetadata();
      
      // check if the function was routed to pruned nodes
      Map resultMap = region.getAll(testKeysList);
      assertTrue(resultMap.equals(origVals));
      pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertTrue(secondResultMap.equals(origVals));
    }
    catch (Exception e) {
      fail("Test failed after the getAll operation", e);
    }
  }
  
  private static void verifyMetadata() {
    Region region = cache.getRegion(PartitionedRegionName);
    ClientMetadataService cms = ((GemFireCacheImpl)cache).getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData = cms
        .getClientPRMetadata_TEST_ONLY();
    assertEquals(1, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    assertEquals(13, prMetaData.getBucketServerLocationsMap_TEST_ONLY().size());
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY()
        .entrySet()) {
      assertEquals(2, ((List)entry.getValue()).size());
    }
  }
  /*
   * Do a getAll from client and see if all the values are returned.
   * Will also have to see if the function was routed from client to all the servers
   * hosting the data. 
   */
  public void testServerPutAllFunction(){
    createScenario();
    client.invoke(SingleHopGetAllPutAllDUnitTest.class,
        "putAll");
  }
  
  public static void putAll() {
    Region region = cache.getRegion(PartitionedRegionName);
    assertNotNull(region);
    final Map keysValuesMap = new HashMap();
    final List testKeysList = new ArrayList();
    for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
      testKeysList.add("execKey-" + i);
      keysValuesMap.put("execKey-" + i, "values-" + i);
    }
    DistributedSystem.setThreadsSocketPolicy(false);
    try {
      // check if the client meta-data is in synch

      // check if the function was routed to pruned nodes
      region.putAll(keysValuesMap);
      // check the listener
      // check how the function was executed
      pause(2000);
      region.putAll(keysValuesMap);
      
      // check if the client meta-data is in synch
      verifyMetadata();
      
      // check if the function was routed to pruned nodes
      Map resultMap = region.getAll(testKeysList);
      assertTrue(resultMap.equals(keysValuesMap));
      pause(2000);
      Map secondResultMap = region.getAll(testKeysList);
      assertTrue(secondResultMap.equals(keysValuesMap));
    }
    catch (Exception e) {
      fail("Test failed after the putAll operation", e);
    }
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}
