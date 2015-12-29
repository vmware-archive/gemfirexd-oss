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

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.IndexUpdaterThread;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Test create a region (Replicated OR Partitioned) and sets index maintenance
 * Asynchronous so that {@link IndexManager} starts a new thread for index
 * maintenance when region is populated. This test verifies that after cache
 * close {@link IndexUpdaterThread} is shutdown for each region
 * (Replicated/Bucket).
 * 
 * @author shobhit
 * 
 */
public class AsyncIndexUpdaterThreadShutdownTest extends TestCase {

  String name = "PR_with_Async_Index";

  public AsyncIndexUpdaterThreadShutdownTest(String name) {
    super(name);
  }

  public void testAsyncIndexUpdaterThreadShutdownForRR() {
    Cache cache = new CacheFactory().create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);
    
    assertNotNull("Region ref null", localRegion);
    
    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/"+name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }
    
    for (int i=0; i<500; i++) {
      localRegion.put(i, new Portfolio(i));
    }
    
    GemFireCacheImpl internalCache = (GemFireCacheImpl)cache;
    // Don't disconnect distributed system yet to keep system thread groups alive.
    internalCache.close("Normal disconnect", null, false, false);

    // Get Asynchronous index updater thread group from Distributed System.
    ThreadGroup indexUpdaterThreadGroup = LogWriterImpl.getThreadGroup("QueryMonitor Thread Group");
    
    assertEquals(0, indexUpdaterThreadGroup.activeCount());
    
    internalCache.getSystem().disconnect();
    
  }

  public void testAsyncIndexUpdaterThreadShutdownForPR() {
    Cache cache = new CacheFactory().create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);
    
    assertNotNull("Region ref null", localRegion);
    
    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/"+name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }
    
    for (int i=0; i<500; i++) {
      localRegion.put(i, new Portfolio(i));
    }

    GemFireCacheImpl internalCache = (GemFireCacheImpl)cache;
    // Don't disconnect distributed system yet to keep system thread groups alive.
    internalCache.close("Normal disconnect", null, false, false);

    // Get Asynchronous index updater thread group from Distributed System.
    ThreadGroup indexUpdaterThreadGroup = LogWriterImpl.getThreadGroup("QueryMonitor Thread Group");
    
    assertEquals(0, indexUpdaterThreadGroup.activeCount());

    internalCache.getSystem().disconnect();
  }
}
