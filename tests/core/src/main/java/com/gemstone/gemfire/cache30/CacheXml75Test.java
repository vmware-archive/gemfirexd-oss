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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;


/**
 * @author dsmith
 *
 */
public class CacheXml75Test extends CacheXml70Test {
  private static final long serialVersionUID = 225193925777688541L;

  public CacheXml75Test(String name) {
    super(name);
  }

  /**
   * Test the ResourceManager element's critical-off-heap-percentage and 
   * eviction-off-heap-percentage attributes
   * @throws Exception
   */
  public void testResourceManagerThresholds() throws Exception {
    CacheCreation cache = new CacheCreation();
    final float low = 90.0f;
    final float high = 95.0f;

    try {
      System.setProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");

      Cache c;
      ResourceManagerCreation rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(high);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(high, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
      
      rmc = new ResourceManagerCreation();
      // Set them to similar values
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(low + 1);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(low + 1, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(high);
      rmc.setCriticalOffHeapPercentage(low);
      cache.setResourceManagerCreation(rmc);
      try {
        testXml(cache);
        assertTrue(false);
      } catch (IllegalArgumentException expected) {
      } finally {
        closeCache();
      }
  
      // Disable eviction
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(0);
      rmc.setCriticalOffHeapPercentage(low);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(low, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      // Disable refusing ops in "red zone"
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(0);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      // Disable both
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(0);
      rmc.setCriticalOffHeapPercentage(0);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      c = getCache();
      assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage());
      assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage());
    } finally {
      System.clearProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    }
  }
  
  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_7_5;
  }

  @SuppressWarnings("rawtypes")
  public void testCompressor() {
    final String regionName = "testCompressor";
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setCompressor(SnappyCompressor.getDefaultInstance());
    /* Region regionBefore = */ cache.createRegion(regionName, attrs);
    
    testXml(cache);
    
    final Cache c = getCache();
    assertNotNull(c);

    final Region regionAfter = c.getRegion(regionName);
    assertNotNull(regionAfter);
    assertTrue(SnappyCompressor.getDefaultInstance().equals(regionAfter.getAttributes().getCompressor()));
    regionAfter.localDestroyRegion();
  }
  
  @SuppressWarnings("rawtypes")
  public void testEnableOffHeapMemory() {
    try {
      System.setProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
      
      final String regionName = "testEnableOffHeapMemory";
      
      final CacheCreation cache = new CacheCreation();
      final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setEnableOffHeapMemory(true);
      assertEquals(true, attrs.getEnableOffHeapMemory());
      
      final Region regionBefore = cache.createRegion(regionName, attrs);
      assertNotNull(regionBefore);
      assertEquals(true, regionBefore.getAttributes().getEnableOffHeapMemory());
  
      testXml(cache);
      
      final Cache c = getCache();
      assertNotNull(c);
  
      final Region regionAfter = c.getRegion(regionName);
      assertNotNull(regionAfter);
      assertEquals(true, regionAfter.getAttributes().getEnableOffHeapMemory());
      assertEquals(true, ((LocalRegion)regionAfter).getEnableOffHeapMemory());
      regionAfter.localDestroyRegion();
    } finally {
      System.clearProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    }
  }

  @SuppressWarnings("rawtypes")
  public void testEnableOffHeapMemoryRootRegionWithoutOffHeapMemoryThrowsException() {
    final String regionName = getUniqueName();
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setEnableOffHeapMemory(true);
    assertEquals(true, attrs.getEnableOffHeapMemory());
    
    final Region regionBefore = cache.createRegion(regionName, attrs);
    assertNotNull(regionBefore);
    assertEquals(true, regionBefore.getAttributes().getEnableOffHeapMemory());

    try {
      testXml(cache);
    } catch (IllegalStateException e) {
      // expected
      String msg = "The region /" + regionName + " was configured to use off heap memory but no off heap memory was configured.";
      assertEquals(msg, e.getMessage());
    }
  }
  
  @SuppressWarnings({ "rawtypes", "deprecation", "unchecked" })
  public void testEnableOffHeapMemorySubRegionWithoutOffHeapMemoryThrowsException() {
    final String rootRegionName = getUniqueName();
    final String subRegionName = "subRegion";
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation rootRegionAttrs = new RegionAttributesCreation(cache);
    assertEquals(false, rootRegionAttrs.getEnableOffHeapMemory());
    
    final Region rootRegionBefore = cache.createRegion(rootRegionName, rootRegionAttrs);
    assertNotNull(rootRegionBefore);
    assertEquals(false, rootRegionBefore.getAttributes().getEnableOffHeapMemory());
    
    final RegionAttributesCreation subRegionAttrs = new RegionAttributesCreation(cache);
    subRegionAttrs.setEnableOffHeapMemory(true);
    assertEquals(true, subRegionAttrs.getEnableOffHeapMemory());
    
    final Region subRegionBefore = rootRegionBefore.createSubregion(subRegionName, subRegionAttrs);
    assertNotNull(subRegionBefore);
    assertEquals(true, subRegionBefore.getAttributes().getEnableOffHeapMemory());

    try {
      testXml(cache);
    } catch (IllegalStateException e) {
      // expected
      final String msg = "The region /" + rootRegionName + "/" + subRegionName +
          " was configured to use off heap memory but no off heap memory was configured.";
      assertEquals(msg, e.getMessage());
    }
  }
  
  public void testCacheServerDisableTcpNoDelay()
      throws CacheException
  {
    CacheCreation cache = new CacheCreation();

    CacheServer cs = cache.addCacheServer();
    cs.setTcpNoDelay(false);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  
  public void testCacheServerEnableTcpNoDelay()
      throws CacheException
  {
    CacheCreation cache = new CacheCreation();

    CacheServer cs = cache.addCacheServer();
    cs.setTcpNoDelay(true);
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

}
