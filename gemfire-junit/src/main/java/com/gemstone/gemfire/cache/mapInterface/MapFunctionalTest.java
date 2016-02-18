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
package com.gemstone.gemfire.cache.mapInterface;

import java.util.HashMap;
import java.util.Properties;
//import java.util.TreeMap;
import junit.framework.TestCase;
import com.gemstone.gemfire.JUnitTestSetup;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
//import com.gemstone.gemfire.cache.CacheWriterException;
//import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
//import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
//import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class MapFunctionalTest extends TestCase {

  static DistributedSystem distributedSystem = null;
  static Region testRegion = null;
  static Object returnObject = null;
  static boolean done = false;

  public static junit.framework.Test suite() {
    return JUnitTestSetup.createJUnitTestSetup(MapFunctionalTest.class);
  }

  public static void caseSetUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    properties.setProperty("locators", "");
    distributedSystem = DistributedSystem.connect(properties);
    Cache cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion", regionAttributes);
  }
  
  public static void caseTearDown() {
    distributedSystem.disconnect();
  }
  
  public MapFunctionalTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
    testRegion.clear();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  
  public void testContainsValuePositive() {
    testRegion.put("Test", "test");
    if(!testRegion.containsValue("test")) {
      fail("contains value failed, value is present but contains value returned false");
    }
    
  }
  
  public void testContainsValueNegative() {
    if(testRegion.containsValue("test123")) {
      fail("Value is not present but contains value returned true");
    }
  }
  
  public void testIsEmptyPositive() {
    testRegion.clear();
    if(!testRegion.isEmpty()) {
      fail("region is empty but isEmpty returns false");
    }
  }
  
  public void testIsEmptyNegative() {
    testRegion.put("test","test");
    if(testRegion.isEmpty()) {
      fail("region is not empty but isEmpty returns true");
    }
  }
  
  public void testPut() {
    testRegion.put("test", "test");
    if(!testRegion.get("test").equals("test")){
      fail("put not successfull");
    }
  }
  
  public void testPutAll() {
    HashMap map = new HashMap();
    for(int i=0 ; i < 5 ; i++) {
      map.put(new Integer(i), new Integer(i));
    }
    testRegion.putAll(map);
    if(!testRegion.containsKey(new Integer(4))|| !testRegion.containsValue(new Integer(4))) {
      fail("Put all did not put in all the keys");
    }
  }
  
  public void testRemove() {
    testRegion.put("Test","test");
    testRegion.remove("Test");
    if(testRegion.containsKey("Test")) {
      fail("remove did not remove the key");
    }
  }
  
  public void testRemoveReturnKey() {
    testRegion.put("Test","test");
    if(!testRegion.remove("Test").equals("test")) {    
      fail("remove did not return the correct value");
    }
  }
  
  public void testSize() {
    testRegion.put("1", "1");
    testRegion.put("2", "2");
    testRegion.put("3", "3");
    if(testRegion.size()!=3) {
      fail("size is not returning the correct size of the region");
    }
  }
  
  public void testPutReturnsObject() {
    testRegion.put("Test", "test");
    if(!testRegion.put("Test","test123").equals("test")) {
      fail("put does not return the correct object");
    }
  }
  
  public void testReturningOldValuePositive() {
    testRegion.put("test", "test123");
    if(!testRegion.put("test", "test567").equals("test123")) {
      fail("old value was not returned inspite of property being set to return null on put");
    }
  }
}
