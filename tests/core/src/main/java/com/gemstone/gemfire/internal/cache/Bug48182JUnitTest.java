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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionShortcut;

/**
 * TestCase that emulates the conditions that produce defect 48182 and ensures that the fix works under those conditions.
 * 48182: Unexpected EntryNotFoundException while shutting down members with off-heap
 * https://svn.gemstone.com/trac/gemfire/ticket/48182 
 * @author rholmes
 */
public class Bug48182JUnitTest extends TestCase {
  /**
   * A region entry key.
   */
  private static final String KEY = "KEY";
  
  /**
   * A region entry value.
   */
  private static final String VALUE = " Vestibulum quis lobortis risus. Cras cursus eget dolor in facilisis. Curabitur purus arcu, dignissim ac lorem non, venenatis condimentum tellus. Praesent at erat dapibus, bibendum nunc sed, congue nulla";
  
  /**
   * A cache.
   */
  private GemFireCacheImpl cache = null;

  /**
   * Create a new Bug48182JUnitTest.
   */
  public Bug48182JUnitTest() {
    super("defect48182JUnitTest");
  }

  /**
   * Create a new Bug48182JUnitTest.
   */
  public Bug48182JUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    // Create our cache
    this.cache = createCache();
  }

  @Override
  public void tearDown() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = null;
    // Cleanup our cache
    closeCache(this.cache);
  }

  /**
   * @return the test's cache.
   */
  protected GemFireCacheImpl getCache() {
    return this.cache;
  }
  
  /**
   * Close a cache.
   * @param gfc the cache to close.
   */
  protected void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }
  
  /**
   * @return the test's off heap memory size.
   */
  protected String getOffHeapMemorySize() {
    return "2m";
  }
  
  /**
   * @return the type of region for the test.
   */
  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }
  
  /**
   * @return the region containing our test data.
   */
  protected String getRegionName() {
    return "region1";
  }
  
  /**
   * Creates and returns the test region with concurrency checks enabled.
   */
  protected Region<Object,Object> createRegion() {
    return createRegion(true);
  }
  
  /**
   * Creates and returns the test region.
   * @param concurrencyChecksEnabled concurrency checks will be enabled if true.
   */
  protected Region<Object,Object> createRegion(boolean concurrencyChecksEnabled) {
    return getCache().createRegionFactory(getRegionShortcut()).setEnableOffHeapMemory(true).setConcurrencyChecksEnabled(concurrencyChecksEnabled).create(getRegionName());    
  }

  /**
   * Creates and returns the test cache.
   */
  protected GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    props.setProperty("off-heap-memory-size", getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  /**
   * Simulates the conditions for 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a cache close during a destroy in an off-heap region.  This test asserts that a CacheClosedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  public void bug50261_test48182WithCacheClose() throws Exception { // disabled due to bug #50261
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().close();
      }      
    };
    
    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
      fail("A CacheClosedException was not triggered");
    } catch(CacheClosedException e) {
      // passed
    }
  }  

  /**
   * Simulates the conditions similar to 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a region destroy during a destroy operation in an off-heap region.  This test asserts that a RegionDestroyedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  public void test48182WithRegionDestroy() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().getRegion(getRegionName()).destroyRegion();
      }      
    };

    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
      fail("A RegionDestroyedException was not triggered");    
    } catch(RegionDestroyedException e) {
      // passed
    }
  }
}
