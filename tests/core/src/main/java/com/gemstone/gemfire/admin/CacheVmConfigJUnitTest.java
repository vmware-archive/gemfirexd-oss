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
package com.gemstone.gemfire.admin;

import java.util.HashSet;
import java.util.Set;

import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * @author Kirk Lund
 * @since 7.0.1
 */
public class CacheVmConfigJUnitTest extends TestCase {

  private Mockery mockContext;
  
  public CacheVmConfigJUnitTest(String name) {
    super(name);
  }

  @Before
  public void setUp() throws Exception {
    this.mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  /** 
   * Tests returning Set of CacheServerConfig as array of CacheServerConfig. 
   * For bug #44704. 
   */
  public void testCacheServerConfigSetAsArray() throws Exception {
    final Set<CacheServerConfig> cacheServerConfigs = new HashSet<CacheServerConfig>();
    final CacheServerConfig mockCacheServerConfig = mockContext.mock(CacheServerConfig.class, "CacheServerConfig");
    cacheServerConfigs.add(mockCacheServerConfig);
    
    CacheServerConfig[] cacheServerConfigArray = (CacheServerConfig[]) 
        cacheServerConfigs.toArray(new CacheServerConfig[cacheServerConfigs.size()]);
    assertNotNull(cacheServerConfigArray);
    assertEquals(1, cacheServerConfigArray.length);
  }
  
  @Test
  /** 
   * Tests returning Set of CacheServerConfig as array of CacheVmConfig. 
   * For bug #44704. 
   */
  public void testCacheServerConfigSetAsCacheVmConfigArray() throws Exception {
    final Set<CacheServerConfig> cacheServerConfigs = new HashSet<CacheServerConfig>();
    final CacheServerConfig mockCacheServerConfig = mockContext.mock(CacheServerConfig.class, "CacheServerConfig");
    cacheServerConfigs.add(mockCacheServerConfig);
    
    CacheVmConfig[] cacheVmConfigArray = (CacheVmConfig[]) 
        cacheServerConfigs.toArray(new CacheVmConfig[cacheServerConfigs.size()]);
    assertNotNull(cacheVmConfigArray);
    assertEquals(1, cacheVmConfigArray.length);
  }

}
