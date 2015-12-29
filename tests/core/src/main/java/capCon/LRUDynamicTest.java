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

public class LRUDynamicTest extends LRUTest {

// The bounds of the size of the region; each point within the bounds is multiplied
// by the BOUND_MULTIPLIER. For example, when the test uses the bounds to get a desired
// region size, the bounds point is multiplied by boundsMultiplier to get the actual
// number of desired entries in the region
private int boundsMultiplier = -1;
private Bounds regionBounds = null;
private boolean randomCapacityChanges = false;
final long NUM_EVICTIONS_BEFORE_CHANGE = 200;

// ================================================================================
// initialization methods 

/** Hydra task to initialize client threads */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new LRUDynamicTest();
      testInstance.initialize();
      ((LRUDynamicTest)testInstance).initFields();
      Log.getLogWriter().info(testInstance.toString());
   }
}

/** Method to initialize this instance of the test class */
protected void initFields() {
   boundsMultiplier = TestConfig.tab().intAt(CapConPrms.boundsMultiplier); 
   int targetLowPoint = TestConfig.tab().intAt(BoundsPrms.targetLowPoint); 
   int targetHighPoint = TestConfig.tab().intAt(BoundsPrms.targetHighPoint); 
   regionBounds = new Bounds(targetLowPoint, targetHighPoint, true, 
                 CapConBB.class, CapConBB.CAPCON_BB_NAME, CapConBB.CAPCON_BB_TYPE);
   CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(1));
   lowestAllowableEntries = boundsMultiplier;
   highestAllowableEntries = boundsMultiplier;
   randomCapacityChanges = ((Boolean)CapConBB.getBB().getSharedMap().get(CapConBB.RANDOM_CAPACITY_CHANGES)).booleanValue();
   Log.getLogWriter().info("Setting TEST_SETTINGS to " + boundsMultiplier);
   CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, new Integer(boundsMultiplier));
   Log.getLogWriter().info("Mutating maximumEntries to " + boundsMultiplier);
   EvictionAttributesMutator mutator = CacheUtil.getRegion(REGION_NAME).getAttributesMutator().getEvictionAttributesMutator();
   mutator.setMaximum(boundsMultiplier);
   Log.getLogWriter().info("After mutating, region is now " + TestHelper.regionToString(CacheUtil.getRegion(REGION_NAME), true));
   CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE,
                                                    NUM_EVICTIONS_BEFORE_CHANGE);
   Log.getLogWriter().info(this.toString());
}

// ================================================================================
// Hydra task methods

/** Hydra task for serial execution of dynamically changing the capacity of an
 *  LRU capacity controller.
 */
public static void HydraTask_serialTest() {
   ((LRUDynamicTest)testInstance).doDynamicTest(true); 
}

/** Hydra task for serial execution of dynamically changing the capacity of an
 *  LRU capacity controller with transactions.
 */
public static void HydraTask_serialTxTest() {
   ((LRUDynamicTest)testInstance).doSerialDynamicTxTest(); 
}

/** Hydra task for concurrent execution of dynamically changing the capacity of an
 *  LRU capacity controller with transactions.
 */
public static void HydraTask_concTxTest() {
   ((LRUDynamicTest)testInstance).doDynamicTest(false);
}

/** Hydra task for concurrent execution of dynamically changing the capacity of an
 *  LRU capacity controller.
 */
public static void HydraTask_concTest() {
   ((LRUDynamicTest)testInstance).doDynamicTest(false); 
}

/** Hydra end task */
public static void HydraTask_endTask() {
   checkForEventError();
   long numCapacityChanges = CapConBB.getBB().getSharedCounters().read(CapConBB.NUM_CAPACITY_CHANGES);
   Log.getLogWriter().info("Number of capacity changes: " + numCapacityChanges);
}

// ================================================================================
// methods to do the work of the hydra tasks

/** Dynamically change the capacity of the LRU capacity controller. Run for
 *  minTaskGranularitySec seconds. Continually add to the region; after so many
 *  evictions, change the capacity of the controller. In serial execution mode,
 *  verify the size of the controller after each addition to the region.
 */
private void doDynamicTest(boolean isSerialExecution) {
   if (isSerialExecution) {
      long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
      Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   }
   final long numEvictionsBeforeChange = 100;
   final long checkForCapacityChangeInterval = 10000; // millis
   Region workRegion = getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In LRUDynamicTest.doDynamicTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   maximumEntries = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS)).intValue();
   super.initFields(); // set highest and lowest allowable
   long lastCapacityChangeCheck = 0;  // will force a check the first time through the loop
   do {
      checkForEventError();
      boolean newCapacity = false;
      boolean newCapacityIsLower = false;
      if (System.currentTimeMillis() - lastCapacityChangeCheck >= checkForCapacityChangeInterval) {
         // see if it is time to change capacity
         lastCapacityChangeCheck = System.currentTimeMillis();
         double currentNumLRUEvictions = TestHelper.getNumLRUEvictions();
         long numLRUEvictionsToTriggerCapacityChange = CapConBB.getBB().getSharedCounters().read(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE);
         Log.getLogWriter().info("Checking for capacity change, currentNumLRUEvictions = " + currentNumLRUEvictions +
                                 ", numLRUEvictionsToTriggerCapacityChange " + numLRUEvictionsToTriggerCapacityChange);
         if (currentNumLRUEvictions >= numLRUEvictionsToTriggerCapacityChange) { // change capacity
            CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE, 
                     (long)currentNumLRUEvictions + numEvictionsBeforeChange);
            int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
            int newMaximumEntries = -1;
            int oldCurrentPoint = currentPoint;
            if (randomCapacityChanges) {
               currentPoint = TestConfig.tab().getRandGen().nextInt(regionBounds.getTargetLowPoint(),
                                                                    regionBounds.getTargetHighPoint());
            } else {
               if (regionBounds.getDirection(currentPoint) == Bounds.UPWARD) {
                  currentPoint++;
               } else {
                  currentPoint--;
               }
            }
            newMaximumEntries = currentPoint * boundsMultiplier;
            Region aRegion =  CacheUtil.getRegion(REGION_NAME);
            Log.getLogWriter().info("Setting new capacity (maximumEntries) " + newMaximumEntries + " on " + 
                CacheUtil.getRegion(REGION_NAME).getFullPath());
            EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
            mutator.setMaximum(newMaximumEntries);
            CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(currentPoint));
            CapConBB.getBB().getSharedCounters().increment(CapConBB.NUM_CAPACITY_CHANGES);
            maximumEntries = newMaximumEntries;
            super.initFields(); // set new lowest and highest allowble entries
            CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, new Integer(maximumEntries));
            newCapacity = true;
            newCapacityIsLower = (currentPoint < oldCurrentPoint);
         }
      }
   
      // add an entry to the region
      long beforeDestroyNotExp = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterDestroyEvents_isNotExp);
      int beforeNumKeys = workRegion.keys().size();
      int afterNumKeys = 0;
      if (useTransactions)
         TxHelper.begin();
      testInstance.addEntry();
      if (TxHelper.exists())
         TxHelper.commitExpectSuccess();
      count++;
      afterNumKeys = workRegion.keys().size();
      CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, afterNumKeys);
      Log.getLogWriter().info("After adding an entry: beforeNumKeys = " + beforeNumKeys +
                              ", afterNumKeys = " + afterNumKeys);

      // if we changed capacity; set the next number of evictions to trigger a capacity change;
      // we want to set this after the addEntry, so if the capacity was lower and we had lots of
      // evictions, we want to do NUM_EVICTIONS_BEFORE_CHANGE more before changing again
      if (newCapacity) {
         double currentNumLRUEvictions = TestHelper.getNumLRUEvictions();
         CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE, 
                  (long)currentNumLRUEvictions + NUM_EVICTIONS_BEFORE_CHANGE);
      }

      if (isSerialExecution) {
         // wait for destroy event if we evicted
         int numEvicted = (beforeNumKeys - afterNumKeys) + 1;
         if (numEvicted < 0)
            numEvicted = 0;
         Log.getLogWriter().info("Adding to the region evicted " + numEvicted + " entries");
         if (numEvicted > 0) { // we evicted; is a local destroy so see an event in only this VM 
            long expectedDestroys = beforeDestroyNotExp + numEvicted;
            TestHelper.waitForCounter(EventCountersBB.getBB(), "EventCountersBB.numAfterDestroyEvents_isNotExp",
                       EventCountersBB.numAfterDestroyEvents_isNotExp, expectedDestroys, true, 60000);
         }
         
         // check capacity after the add
         if ((newCapacity) && (newCapacityIsLower)) {
            if (afterNumKeys > highestAllowableEntries) {
               throw new TestException("After adding to " + TestHelper.regionToString(workRegion, false) + 
                  " num keys is " + afterNumKeys + 
                  "; expected num keys to <= highestAllowableEntries " + highestAllowableEntries + ", " +
                  this.toString());
            }
         } else { // capacity did not change or is higher 
            if (beforeNumKeys < lowestAllowableEntries) {
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
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In doDynamicTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + "; " + TestHelper.regionToString(workRegion, false)  + 
       " has " + workRegion.keys().size() + " entries");
}

/** Method to doing hydra task to test dynamically changing the capacity of
 *  an LRU capacity controller while using transactions. This is for a 
 *  serial execution test.
 */
private void doSerialDynamicTxTest() {
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   Region workRegion = getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In LRUDynamicTest.doDynamicTxTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   maximumEntries = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS)).intValue();
   super.initFields(); // set highest and lowest allowable
   int currentNumEntries = workRegion.keys().size();
   int numEntriesToChangeCapacity =  TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maximumEntries+100);
   int numEntriesToEndTx = TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maximumEntries+100);
   Log.getLogWriter().info("Setting numEntriesToChangeCapacity to " + numEntriesToChangeCapacity +
       " numEntriesToEndTx to " + numEntriesToEndTx +
       " current num entries is " + currentNumEntries);
   TxHelper.begin();
   int numEntriesWhenTxBegan = currentNumEntries;
   boolean done = false;
   do {
      checkForEventError();
      testInstance.addEntry();
      
      currentNumEntries = workRegion.keys().size();
      Log.getLogWriter().info("Current number of entries is " + currentNumEntries +
          ", numEntriesToChangeCapacity is " + numEntriesToChangeCapacity +
          " numEntriesToEndTx is " + numEntriesToEndTx + 
          " current LRU maximum entries is " + maximumEntries);

      if (currentNumEntries == numEntriesToChangeCapacity) {
         int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
         if (randomCapacityChanges) {
            currentPoint = TestConfig.tab().getRandGen().nextInt(regionBounds.getTargetLowPoint(),
                                                                 regionBounds.getTargetHighPoint());
         } else {
            if (regionBounds.getDirection(currentPoint) == Bounds.UPWARD) {
               currentPoint++;
            } else {
               currentPoint--;
            }
         }
         CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(currentPoint));
            Region aRegion =  CacheUtil.getRegion(REGION_NAME);
         maximumEntries = currentPoint * boundsMultiplier;
         super.initFields(); // set new lowest and highest allowble entries
         Log.getLogWriter().info("Setting new capacity (maximumEntries) " + maximumEntries + 
             " on " + aRegion.getFullPath() + ", current num entries is " + currentNumEntries);
         EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
         mutator.setMaximum(maximumEntries);
         CapConBB.getBB().getSharedCounters().increment(CapConBB.NUM_CAPACITY_CHANGES);
         int regSizeAfterChangingCap = workRegion.keys().size();
         CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, new Integer(maximumEntries));
         if (currentNumEntries != regSizeAfterChangingCap) {
            throw new TestException("Expected number of entries before changing capacity " + currentNumEntries 
                  + " to be equal to num entries after changing capacity " + regSizeAfterChangingCap);
         }
      }

      done = System.currentTimeMillis() - startTime > timeToRunMS;
      if (done || (currentNumEntries == numEntriesToEndTx)) { // time to end the transaction
         if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
            int beforeCommitNumEntries = currentNumEntries;
            Log.getLogWriter().info("Before commit, num entries is " + beforeCommitNumEntries);
            TxHelper.commitExpectSuccess();
            int afterCommitNumEntries = workRegion.keys().size();
            if (beforeCommitNumEntries <= maximumEntries) { // should be no change in region size
               if (beforeCommitNumEntries != afterCommitNumEntries)
                  throw new TestException("Before commit, num entries was " + beforeCommitNumEntries + 
                     ", after commit num entries was " + afterCommitNumEntries);
            } else { // number of entries before the commit is > current capacity setting
               if (afterCommitNumEntries > maximumEntries) 
                  throw new TestException("After committing, num entries is " + afterCommitNumEntries +
                        ", but currentCapacity maximumEntries is " + maximumEntries);
            } 
         } else { // do a rollback
            int numEntriesBeforeRollback = currentNumEntries;
            TxHelper.rollback();
            int numEntriesAfterRollback = workRegion.keys().size();
            if (numEntriesAfterRollback != numEntriesWhenTxBegan)
               throw new TestException("When transaction began num entries was " + numEntriesWhenTxBegan + 
                  ", before rollback num entries was " + numEntriesBeforeRollback + 
                  ", after rollback num entries was " + numEntriesAfterRollback);
         }

         // Now set new targets for changing capacity and ending tx.
         // Now that we have ended the tx, the size of the region cannot be more 
         // than the current capacity setting
         currentNumEntries = workRegion.keys().size();
         numEntriesToChangeCapacity =  TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maximumEntries+100);
         numEntriesToEndTx = TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maximumEntries+100);
         Log.getLogWriter().info("Setting numEntriesToChangeCapacity to " + numEntriesToChangeCapacity +
                                 " numEntriesToEndTx to " + numEntriesToEndTx +
                                 " current num entries is " + currentNumEntries);

         if (!done)
            TxHelper.begin();
         numEntriesWhenTxBegan = currentNumEntries; 
      }
   } while (!done);

   Log.getLogWriter().info("In doDynamicTxTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + "; " + 
       TestHelper.regionToString(workRegion, false)  + 
       " has " + workRegion.keys().size() + " entries");
}

// Overridden methods
//========================================================================

public CacheListener getEventListener() {
   if (TestConfig.tab().booleanAt(Prms.serialExecution)) {
      return new LRUListener();  // verifies evictions
   } else {
      return null;  // cannot verify evictions in concurrent test
   }
}

// implementation of abstract methods
// ================================================================================
    
// String methods
//========================================================================
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toString() + "\n");
   aStr.append("   Region bounds:\n");
   aStr.append("      boundsMultiplier: " + boundsMultiplier + "\n");
   aStr.append("      regionBounds: " + regionBounds + "\n");
   aStr.append("      randomCapacityChanges: " + randomCapacityChanges + "\n");
   return aStr.toString();
}

}
