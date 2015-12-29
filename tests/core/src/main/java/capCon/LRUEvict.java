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
import hydra.blackboard.*;
//import java.util.*;
import com.gemstone.gemfire.cache.*;

public class LRUEvict extends LRUTest {

// hydra tasks
// ================================================================================
public synchronized static void HydraTask_initializeWithLogListener() {
   if (testInstance == null) {
      testInstance = new LRUEvict();
      ((LRUEvict)testInstance).initialize(new event.LogListener());
      ((LRUEvict)testInstance).maximumEntries = CacheUtil.getRegion(REGION_NAME).getAttributes().
                 getEvictionAttributes().getMaximum();
      ((LRUEvict)testInstance).initFields();
      Log.getLogWriter().info(testInstance.toString());
   }
}

public synchronized static void HydraTask_initializeWithEvictListener() {
   if (testInstance == null) {
      testInstance = new LRUEvict();
      ((LRUEvict)testInstance).initialize(new EvictListener());
      ((LRUEvict)testInstance).maximumEntries = CacheUtil.getRegion(REGION_NAME).getAttributes().
                 getEvictionAttributes().getMaximum();
      ((LRUEvict)testInstance).initFields();
      EvictTest.initEvictTest(testInstance.getWorkRegion());
      Log.getLogWriter().info(testInstance.toString());
   }
}

public static void HydraTask_serialEvictTest() {
   Region workRegion = testInstance.getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_serialEvictTest, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      EvictTest.serialEvictTest(testInstance, workRegion);
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
}

public static void HydraTask_fill() {
   ((LRUEvict)testInstance).fill();
}

// initialization methods
// ================================================================================
protected synchronized void initialize(CacheListener aListener) {
   useTransactions = TestConfig.tab().booleanAt(CapConPrms.useTransactions, false);
   String clientName = System.getProperty("clientName");
   ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
   numVMs = cd.getVmQuantity();
   numThreads = cd.getVmThreads();
   randomValues = new RandomValues();
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, aListener, new CapConLoader(), null);

}

protected void initFields() {
   LRUAllowance = TestConfig.tab().intAt(CapConPrms.LRUAllowance);
   super.initFields();
}

// implementation of abstract methods
// ================================================================================
public CacheListener getEventListener() {
   return new EvictListener();
}

// end task methods
//========================================================================
public static void HydraTask_endTask() {
   NameBB.getBB().printSharedCounters();
   CapConBB.getBB().printSharedCounters();
   SharedCounters counters = EventCountersBB.getBB().getSharedCounters();
   long numAfterCreateEvents = counters.read(EventCountersBB.numAfterCreateEvents_isNotExp);
   long numAfterDestroyEvents = counters.read(EventCountersBB.numAfterDestroyEvents_isNotExp);
   long numAfterInvalidateEvents = counters.read(EventCountersBB.numAfterInvalidateEvents_isNotExp);
   long numAfterUpdateEvents = counters.read(EventCountersBB.numAfterUpdateEvents_isNotExp);
   long currentNameCounter = NameFactory.getPositiveNameCounter();

   // get expected after create events
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   long expectedAfterCreateEvents = currentNameCounter;
   if (!regDef.getScope().isLocal()) {
      String clientName = System.getProperty("clientName");
      ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
      int numVMs = cd.getVmQuantity();
      expectedAfterCreateEvents = currentNameCounter * numVMs;
      Log.getLogWriter().info("ExpectedAfterCreateEvents is currentNameCounter " + currentNameCounter +
          " * numVMs " + numVMs + ": " + expectedAfterCreateEvents);
   } else {
      Log.getLogWriter().info("ExpectedAfterCreateEvents is currentNameCounter " + currentNameCounter);
   }

   StringBuffer errStr = new StringBuffer();
   if (numAfterCreateEvents != expectedAfterCreateEvents)
      errStr.append("Expected NUM_OBJECT_ADDED_EVENTS " + numAfterCreateEvents + " to be equal to expectedAfterCreateEvents " + expectedAfterCreateEvents + "\n");
   if (numAfterInvalidateEvents != 0)
      errStr.append("Expected NUM_OBJECT_INVALIDATED_EVENTS " + numAfterInvalidateEvents + " to be 0\n");
   if (numAfterUpdateEvents != 0)
      errStr.append("Expected NUM_OBJECT_REPLACED_EVENTS " + numAfterUpdateEvents + " to be 0\n");
   if (numAfterDestroyEvents <= 0)
      errStr.append("Expected NUM_OBJECT_DESTROYED_EVENTS " + numAfterDestroyEvents + " to be > 0");
   if (errStr.length() > 0)
      throw new TestException(errStr.toString());
   double numEvictions = TestHelper.getNumLRUEvictions();
   Log.getLogWriter().info("Number of lru evictions during test: " + numEvictions);
}

// protected methods
// ================================================================================
protected void fill() {
   Log.getLogWriter().info("Filling with " + maximumEntries + " entries...");
   for (int i = 1; i <= maximumEntries; i++) {
      testInstance.addEntry();
      ((LRUEvict)testInstance).verifyNumKeys();
   }
}

}
