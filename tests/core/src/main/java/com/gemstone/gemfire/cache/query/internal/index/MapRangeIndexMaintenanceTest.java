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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.HashMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexMaintenanceException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;

/**
 * @author shobhit
 *
 */
public class MapRangeIndexMaintenanceTest extends TestCase{

  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1"; 

  @Override
  protected void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @Override
  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public void testNullMapKeysInIndexOnLocalRegion() throws Exception{

    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof MapRangeIndex);

    //Let MapRangeIndex remove values for key 1.
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);
    
    //Now mapkeys are null for key 1
    try {
      region.invalidate(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
    
    //Now mapkeys are null for key 1
    try {
      region.destroy(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
  }

  /**
   * Test index object's comapreTo Function implementation correctness for indexes.
   * @throws Exception
   */
  public void testDuplicateKeysInRangeIndexOnLocalRegion() throws Exception{

    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", new TestObject("SUN", 1));
    map1.put("IBM", new TestObject("IBM", 2));
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
    
    assertTrue(keyIndex1 instanceof MapRangeIndex);

    //Put duplicate TestObject with "IBM" name.
    region.put(Integer.toString(1), p);
    Portfolio p2 = new Portfolio(2, 2);
    HashMap map2 = new HashMap();
    map2.put("YHOO", new TestObject("YHOO", 3));
    map2.put("IBM", new TestObject("IBM", 2));
    
    p2.positions = map2;
    region.put(Integer.toString(2), p2);

    //Following destroy fails if fix for 44123 is not there.
    try {
      region.destroy(Integer.toString(1));
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    } catch (Exception ex) {
      if (ex instanceof IndexMaintenanceException) {
        if (! ex.getCause().getMessage()
            .contains("compareTo function is errorneous")) {
          fail("Test Failed! Did not get expected exception IMQException."
              + ex.getMessage());
        }
      } else {
        ex.printStackTrace();
        fail("Test Failed! Did not get expected exception IMQException.");
      }
    }
  }
  public static void main(String[] args) throws Throwable{
    junit.textui.TestRunner.run(suite());
  }
  public static Test suite() {
    TestSuite suite = new TestSuite(MapRangeIndexMaintenanceTest.class);
    return suite;
  }

  /**
   * TestObject with wrong comareTo() implementation implementation.
   * Which throws NullPointer while removing mapping from a MapRangeIndex.
   * @author shobhit
   *
   */
  public class TestObject implements Cloneable, Comparable {
    public TestObject(String name, int id) {
      super();
      this.name = name;
      this.id = id;
    }
    String name;
    int id;
    
    @Override
    public int compareTo(Object o) {
      if (id == ((TestObject)o).id)
        return 0;
      else
        return id > ((TestObject)o).id ? -1 : 1;
    }
  }
}