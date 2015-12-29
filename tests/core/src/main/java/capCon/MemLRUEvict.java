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
package capCon;

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

public class MemLRUEvict extends MemLRUTest {

// hydra tasks
// ================================================================================
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new MemLRUEvict();
      ((MemLRUEvict)testInstance).initFields();
      testInstance.initialize();
      Region aRegion = CacheUtil.getRegion(REGION_NAME);
      maximumMegabytes = aRegion.getAttributes().getEvictionAttributes().getMaximum();
      ((MemLRUTest)testInstance).memLRUParams = new MemLRUParameters(KEY_LENGTH, maximumMegabytes, 
           CacheUtil.getRegion(REGION_NAME));
      CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, ((MemLRUTest)testInstance).memLRUParams);
      EvictTest.initEvictTest(testInstance.getWorkRegion());
      Log.getLogWriter().info(testInstance.toString());
   }
}

public static void HydraTask_serialEvictTest() {
   Region workRegion = testInstance.getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_monitorCapacity, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      EvictTest.serialEvictTest(testInstance, workRegion);
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
}

public static void HydraTask_fill() {
   ((MemLRUEvict)testInstance).fill();
}

// implementation of abstract methods
// ================================================================================
public CacheListener getEventListener() {
   return new EvictListener();
}

// protected methods
// ================================================================================
protected void initFields() {
   LRUAllowance = TestConfig.tab().intAt(CapConPrms.LRUAllowance);
   Log.getLogWriter().info("Set LRUAllowance to " + LRUAllowance);
}

protected void fill() {
   int maxEntries = memLRUParams.getMaxEntries();
   Log.getLogWriter().info("Filling with " + maxEntries + " entries...");
   MemLRUEvict aTest = (MemLRUEvict)testInstance;
   for (long i = 1; i <= maxEntries; i++) {
      testInstance.addEntry();
      aTest.verifyMemCapacity(aTest.memLRUParams.getRegionByteLimit());
   }
}

}
