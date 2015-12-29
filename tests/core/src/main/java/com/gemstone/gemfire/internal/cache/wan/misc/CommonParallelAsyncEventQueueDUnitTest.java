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
package com.gemstone.gemfire.internal.cache.wan.misc;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

/**
 * @author skumar
 *
 */
public class CommonParallelAsyncEventQueueDUnitTest extends WANTestBase {
  
  private static final long serialVersionUID = 1L;

  public CommonParallelAsyncEventQueueDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception  {
    super.setUp();
  }
    
  public void testParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(WANTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR1", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR1", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR1", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR1", "ln", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR2", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR2", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR2", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { testName + "_PR2", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR1",
        256 });
    vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] { testName + "_PR2",
        256,512 });
    
    vm4.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(WANTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256 * 2);
  }
}
