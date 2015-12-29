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

public class LRUTest extends CapConTest {

protected int maximumEntries = 0;

// fields to define the allowable deviation from maximumEntries
protected int lowestAllowableEntries = -1;     
protected int highestAllowableEntries = -1;

// initialization methods
// ================================================================================
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new LRUTest();
      testInstance.initialize();
      ((LRUTest)testInstance).maximumEntries = CacheUtil.getRegion(REGION_NAME).getAttributes().
                getEvictionAttributes().getMaximum();
      ((LRUTest)testInstance).initFields();
      Log.getLogWriter().info(testInstance.toString());
   }
}

protected void initFields() {
   Region workRegion = getWorkRegion();
   if (TestConfig.tab().booleanAt(Prms.serialExecution)) {
      // in serial execution, limits are hard limits
      lowestAllowableEntries = maximumEntries;
      highestAllowableEntries = maximumEntries;
      Log.getLogWriter().info("Setting limits: This test is serial execution " +
          TestHelper.regionToString(workRegion, false) + 
          " maximumEntries = " + maximumEntries + 
          ", lowestAllowableEntries = " + lowestAllowableEntries + 
          ", highestAllowableEntries = " + highestAllowableEntries);
   } else {
      // this is a VM region; 
      // LRU can deviate by +/- number of threads in this VM
      // + 1 more for extra allowance
         lowestAllowableEntries = maximumEntries - numThreads - 1;
         highestAllowableEntries = maximumEntries + numThreads + 1;
         Log.getLogWriter().info("Setting limits: This VM is using a VM region " + 
         TestHelper.regionToString(workRegion, false) + ", numThreads = " + numThreads + 
             " maximumEntries = " + maximumEntries + 
             ", lowestAllowableEntries = " + lowestAllowableEntries + 
             ", highestAllowableEntries = " + highestAllowableEntries);
   }
}

// implementation of abstract methods
// ================================================================================
public Object getNewKey() {
   return NameFactory.getNextPositiveObjectName();
}

public Object getNewValue() {
   return randomValues.getRandomObjectGraph();
}

public CacheListener getEventListener() {
   return new LRUListener();
}

public Object getPreviousKey() {
   long RANGE = 20;
   long currentNameCounter = NameFactory.getPositiveNameCounter();
   long lowEnd = currentNameCounter - RANGE;
   if (lowEnd < 0)
      lowEnd = 0;
   long previousNameCounter = TestConfig.tab().getRandGen().nextLong(lowEnd, currentNameCounter);
   return NameFactory.getObjectNameForCounter(previousNameCounter); 
}

// task methods
//========================================================================
public static void HydraTask_addEntries() {
   testInstance.addEntries();
}

public static void HydraTask_serialTest() {
   ((LRUTest)testInstance).doSerialTest(); 
}

public static void HydraTask_monitorCapacity() {
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_monitorCapacity, monitoring capacity for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      ((LRUTest)testInstance).verifyNumKeys();
      checkForEventError();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In HydraTask_monitorCapacity, done running for " + timeToRunSec + " seconds");
}

// end task methods
//========================================================================
public static void HydraTask_endTask() {
   NameBB.getBB().printSharedCounters();
   CapConBB.getBB().printSharedCounters();
   checkForEventError();
   double numEvictions = TestHelper.getNumLRUEvictions();
   Log.getLogWriter().info("Number of lru evictions during test: " + numEvictions);
}

// private methods
//========================================================================
protected void verifyNumKeys() {
   Region workRegion = getWorkRegion();
   int numKeys = workRegion.keys().size();
   Log.getLogWriter().info("In verifyNumKeys, numKeys is " + numKeys);
   CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, numKeys);
   if (numKeys > highestAllowableEntries)
      throw new TestException("Expected num keys to be <= " +
         highestAllowableEntries + ", but num keys for " + TestHelper.regionToString(workRegion, false) + " is " + numKeys + ", " + this.toString());
}

/** This is called when eviction occurs, from a destroy event. 
 *  Verify that eviction is not occurring too early 
 */
protected void verifyEviction(LRUListener listener, EntryEvent event) {
   Region workRegion = getWorkRegion();
   int numKeys = workRegion.keys().size();
   CapConBB.getBB().getSharedCounters().setIfSmaller(CapConBB.MIN_NUM_KEYS_AT_LRU_EVICTION, numKeys);
   int limit = lowestAllowableEntries - 1; // -1 because this is called after eviction (ie a destroy) has occurred
   if (numKeys < limit) { 
      String errStr = "Early eviction detected: Expected num keys to be >= " +
         limit + ", but num keys for " + workRegion.getFullPath() + " is " +
         numKeys + ", " + this.toString() + " " + TestHelper.getStackTrace() +
         listener.toString("", event);
      Log.getLogWriter().info(errStr);
      CapConBB.getBB().getSharedMap().put(TestHelper.EVENT_ERROR_KEY, errStr);
   }
}

/** Return the number of entries that should be in the region for a transaction
 *  boundary (commit or rollback).
 *
 *  @param currentNumEntries The current number of entries in the region
 *
 *  @return The desired number of entries in the region to do a commit or rollback.
 */
private int getNumEntriesForTxBoundary(int currentNumEntries) {
   GsRandom rand = TestConfig.tab().getRandGen();
   if (currentNumEntries < lowestAllowableEntries) {
      // commit periodically while growing the region to its threshold
      return rand.nextInt(currentNumEntries + 1, lowestAllowableEntries);
   } else {
      if (rand.nextBoolean()) { // commit while within capacity
         return rand.nextInt(lowestAllowableEntries, highestAllowableEntries);
      } else { // commit after growing above capacity
         return rand.nextInt(highestAllowableEntries + 1, highestAllowableEntries + 100);
      }
   }
}

private void doSerialTest() {
   Region workRegion = getWorkRegion();
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   Log.getLogWriter().info(TestHelper.regionToString(workRegion, false) + " has " + TestHelper.getTotalNumKeys(workRegion) + " entries");

   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In doSerialTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   boolean inTrans = false;
   int txBoundary = -1;
   boolean done = false;
   int numKeysWhenTxBegan = -1;
   do {
      checkForEventError();
      int beforeNumKeys = workRegion.keys().size();
      if (useTransactions && !inTrans) { // begin a transaction
         TxHelper.begin();
         inTrans = true;
         txBoundary = getNumEntriesForTxBoundary(beforeNumKeys);
         Log.getLogWriter().info("Targeting a transaction boundary when numEntries is " + txBoundary +
                                 ", current num entries is " + beforeNumKeys);
         numKeysWhenTxBegan = beforeNumKeys;
      }
      testInstance.addEntry();
      count++;
      int afterNumKeys = workRegion.keys().size();
      boolean timeToEndTrans = (afterNumKeys >= txBoundary); 
      done = System.currentTimeMillis() - startTime > timeToRunMS;
      boolean rolledBack = false;
      if (inTrans && (timeToEndTrans || done)) {
         Log.getLogWriter().info("Before ending transaction, current num entries is " + afterNumKeys +
             ", num entries when tx began: " + numKeysWhenTxBegan);
         if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
            TxHelper.commitExpectSuccess();
         } else {
            TxHelper.rollback();
            rolledBack = true;
            int numKeysAfterRollback = workRegion.keys().size();
            if (numKeysAfterRollback != numKeysWhenTxBegan)
               throw new TestException("When transaction began num entries was " + numKeysWhenTxBegan + 
                  ", before rollback num entries was " + afterNumKeys + 
                  ", after rollback num entries was " + numKeysAfterRollback);
         }
         inTrans = false;
         afterNumKeys = workRegion.keys().size();
         Log.getLogWriter().info("After ending transaction, current num entries is " + afterNumKeys);
      }
      CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, afterNumKeys);

      if (!rolledBack) { // already did region validation (above) if we rolled back
         if ((inTrans) || (beforeNumKeys < lowestAllowableEntries)) {
            if (beforeNumKeys + 1 != afterNumKeys) {
               throw new TestException("Before adding to " + TestHelper.regionToString(workRegion, false) + ", num keys was " +
                  beforeNumKeys + ", but after adding " + " num keys was " + afterNumKeys + 
                  "; expected num keys to be " + (beforeNumKeys + 1) + ", " + this.toString());
            }
         } else if (afterNumKeys > highestAllowableEntries) {
            throw new TestException("Before adding to " + TestHelper.regionToString(workRegion, false) + ", num keys was " +
               beforeNumKeys + ", but after adding " + " num keys was " + afterNumKeys + 
               "; expected num keys to <= highestAllowableEntries " + highestAllowableEntries + ", " +
               this.toString());
         }
      } 
   } while (!done);
   Log.getLogWriter().info("In doSerialTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + "; " + TestHelper.regionToString(workRegion, false)  + 
       " has " + TestHelper.getTotalNumKeys(workRegion) + " entries");
}

// String methods
//========================================================================
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toString() + "\n");
   aStr.append("   entry evictor settings:\n");
   aStr.append("      maximumEntries (in test instance): " + maximumEntries + "\n");
   aStr.append("      maximumEntries (in evictor attr) : " + 
              (CacheUtil.getRegion(REGION_NAME).getAttributes().getEvictionAttributes().getMaximum()) + "\n");
   aStr.append("   Region limits:\n");
   aStr.append("      lowestAllowableEntries: " + lowestAllowableEntries + "\n");
   aStr.append("      highestAllowableEntries: " + highestAllowableEntries + "\n");
   return aStr.toString();
}

}
