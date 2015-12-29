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
import com.gemstone.gemfire.JUnitTestSetup;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import junit.framework.TestCase;

public class ExceptionHandlingTest extends TestCase {

  static DistributedSystem distributedSystem = null;
  static Region testRegion = null;
  static Object returnObject = null;
  static boolean done = false;

  public static junit.framework.Test suite() {
    return JUnitTestSetup.createJUnitTestSetup(ExceptionHandlingTest.class);
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

  public ExceptionHandlingTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
    testRegion.clear();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testNullPointerWithContainsValue() {
    boolean caught = false;
    try {
      testRegion.containsValue(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  public void _testNullPointerWithGet() {
    boolean caught = false;
    try {
      testRegion.get(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  public void testNullPointerWithRemove() {
    boolean caught = false;
    try {
      testRegion.remove(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  public void _testNullPointerWithPut() {
    boolean caught = false;
    try {
      testRegion.put(null,null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  public void testNullPointerWithPutAll() {
    boolean caught = false;
    try {
      testRegion.putAll(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  public void testPutAllNullValue() {
    boolean caught = false;
    try {
      HashMap map = new HashMap();
      map.put("key1", "key1value");
      map.put("key2", null);
      testRegion.putAll(map);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }
  
  public void testNullPointerWithContainsKey() {
    boolean caught = false;
    try {
      testRegion.containsKey(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }
}
